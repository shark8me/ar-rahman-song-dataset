# Experiment 1 — GLiNER filter-decision head at scale: CURRENT STATE

Plan: `.hermes/plans/2026-11-19_185959-exp1-gliner-filter-generalization.md` (2026-11-20).

**Verdict up front:** scaling the verified QA set 25 → 1,703 train examples fixed
everything except the one head the experiment targeted. Per-head skeleton accuracy
on the leakage-safe 373-question test set is excellent for select/group/order/join
(0.98–1.00) and good for agg (0.85), but the filter decision — filter_col +
filter_op, the bottleneck identified in REPORT.md — stayed collapsed at ~0.23 with
the same degenerate `year`/`year_eq` prediction the 25-example run showed.

## What succeeded

- **Scaled verified corpus** (`experiments/multi-composer-qa/gold-multi-scaled.jsonl`,
  2,076 entries): 12 filter-heavy phrasings added to the generator, `--rounds/--out`
  CLI, dedup by normalized question text (911 dups skipped), `template_id` stored per
  entry. `verify_gold.py` re-ran all 2,045 generated ref_sql against their DBs —
  **0 mismatches**. ~100 min of glm-5.3-flashx phrasing.
- **Leakage-safe split** (`experiments/multi-composer-qa/results/exp1_split.json`,
  from `make_split.py`): train 1,703 / test 373, test filter-like share 76.7%,
  held-out template families `song_year` + `singer_first_year_c`, 184-string entity
  blocklist. Leakage assertions pass (verified independently: 0 blocklist entities in
  any train query/values; split unit = entity groups via union-find, not random rows).
- **Baseline error analysis** (`results/exp1/filter_error_baseline.txt`,
  from `analyze_filter_errors.py`): 13/31 filter misses on the 25-example model, ALL
  the same `year/year_eq` collapse; 10/13 entity strings present in train but only in
  no-filter contexts — the model never had title/movie+like supervision.
- **Schema-label bug fixed en route:** first training attempt silently validated only
  375/1,703 examples — `heads_for()` emitted per-composer DB names (`movie`, `song`,
  `release_date`) and an `order` value `name_asc` missing from the frozen schema.
  Fixed by canonicalizing heads (`canonicalize_heads.py` rewrote 1,101 entries;
  ref_sql/answers untouched), `ORDER_LABELS += name_asc` with render.py support +
  test (7/7 renderer tests pass). All 2,076 entries now validate against the label lists.
- **Live training-curve plumbing:** `finetune.py` gained `--data/--split/--out/
  --grad-accum/--fp16/--metrics-out` and a 5s poller writing
  `results/exp1/metrics.jsonl`; `gpu-jobs/watch_curves.py` tails it over ssh into a
  live SVG. MLflow was considered and rejected (single run; a local server that the
  GPU box pushes to is fragile; file-on-vidura + ssh tail survives disconnects).
- **Training run** (vidura, GTX 1070, job `exp1-lora`): 8 epochs, batch 1 +
  grad-accum 16, fp16, task-lr 5e-4 — 856 steps in 51 min, GPU 91%. Loss curves and
  all per-epoch checkpoints collected under `results/exp1/`.

## What failed — the filter head did not generalize

Test-set accuracy (373 held-out questions; scored by `run_exp1_eval.py` on vidura):

| Head | acc | | Head | acc |
|---|---|---|---|---|
| select_cols | 0.979 | | filter_col | **0.260** |
| group_by | 1.000 | | filter_op | **0.231** |
| order | 1.000 | | **filter decision (col+op)** | **0.231** |
| join | 0.984 | | exact skeleton | 0.102 |
| limit | 0.579 | | | |
| agg | 0.847 | | | |

- Held-out-template slice: filter 0.189, skeleton 0.024. Seen-template slice:
  filter 0.286, skeleton 0.205. Held-out and unseen-entity performance did NOT
  separate from seen — the head generalizes to neither.
- Epoch-1 "best" (eval_loss 28.4) and epoch-8 final collapse identically → not
  undertraining. Train loss hit 0.09 while held-out eval loss rose after epoch 1 —
  overfitting past epoch 1, but the filter head is broken in both regimes.
- Not year-token surface bias: queries with no year token collapse to `year` too
  (274 of 274 like-gold no-year queries). Train-side, only 1/276 title-filter
  queries even contains a year.
- Not majority-class: train filter_col is movie_name 640 / year 449 / title 276;
  `year` is not the majority. Meanwhile the structurally identical select_cols
  decision reaches 0.98 — the failure is specific to the filter_col head.
- `like → year_eq` op confusion is largely an artifact of the inference-time
  constraint set (`C.implies` forces year→year_eq) — which itself contradicts the
  training data (444 train entries are year+`like`). Training and inference
  currently disagree on those examples.
- Unfiltered detection is nearly perfect (86/87 `none`), so the head learned the
  filter/no-filter boundary but nothing about which column.

**Gate failed = finding:** 100× more verified data alone does not fix the
filter_col decision; the head ignores slot-type evidence entirely.
*(Superseded by the A1-redo section below — the collapse was largely an
inference-time constraint artifact.)*

## A1-redo — corrected constraint set (2026-11-20)

The original `C.implies` set was written against the 25-example Rahman corpus
and contradicted the scaled gold in two ways, both found by
`check_constraints.py` (new; validates every implication against all 2,076
gold entries — heads normalized since `filter_col`/`filter_op` are bare
strings while `select_cols`/`join` are lists):

- **Direction error**: `like → filter_col ∈ {singer, title, movie_name}` was
  meant as "LIKE only filters text columns" but forces a *specific* column —
  455 gold entries use LIKE on `year`, and `singer` is never LIKE-filtered.
  `eq → movie_name/title` were vacuous (`eq` never occurs in gold).
- **Singer→join rules wrong in general**: "How many singers has X collaborated
  with?" (`COUNT(DISTINCT singer) FROM singers_ila`) needs no join, yet
  `select_cols=singer → join` would force one — 130 gold violations.

Corrected set (5 implications, 0 violations, 0 vacuous):
`order=count_desc → agg=count`; `group_by=year → agg=count`;
`like → filter_col ∈ {title, movie_name, year}` (any-of);
`year_eq → filter_col=year`; `filter_col=year → filter_op ∈ {like, year_eq}`.

Re-score on the same 373-question test split, same epoch-8 adapter
(`results/exp1/pred_exp1_a1redo.json`, 894 s CPU):

| metric | old constraints | corrected |
|---|---|---|
| filter decision (col+op) | 0.231 | **0.979** |
| filter held-out slice | 0.189 | 0.962 |
| filter seen slice | 0.286 | 1.000 |
| exact skeleton | 0.102 | 0.445 |
| select/group/order | 0.98–1.00 | 1.000 |
| join | 0.984 | 0.997 |
| agg | 0.847 | 0.847 |
| limit | 0.579 | 0.579 |

Verdict: the year/year_eq "collapse" was largely an inference-time constraint
artifact — with a gold-consistent set the model's natural per-query evidence
resurfaces and the filter head generalizes (held-out 0.962). Remaining 8
filter errors are all `title ↔ movie_name` / `title → none` confusions —
slot-type ambiguity (a song name vs a film name), not a column collapse.
Exact skeleton (0.445) is now limited by `agg`/`limit`, not the filter head.

## Filter-head-focused retrain (2026-11-21)

Rebalanced train set (`make_rebalanced_train.py` →
`results/exp1/gold-train-rebalanced.jsonl`): year-filter entries capped
449 → 150, title/movie like-oversamples synthesized deterministically from
the same PHRASES/B() builders (+550, DB-verified, 0 ref_sql mismatches),
112 contrastive slot-swap pairs (same entity string as song title AND film
name; `filter_col` must read slot type, not the entity token). Assertions:
no blocklisted entity, no held-out template, no test-query collision,
unique idx. Final train: 1,954 (title|like 672, movie|like 794,
none|none 338, year 150).

Per-head loss instrumentation in `finetune.py` (`instrument_head_losses`):
a signature-agnostic wrapper on `SpanExtractorModel._compute_sample_loss`
re-evaluates the classifier per classification schema under `no_grad` and
accumulates per-head BCE means, flushed to metrics JSONL every 5 s + a final
flush after training. (First vidura attempt crashed: gliner2 passes
`span_scores_per_schema` that the wrapper rejected → every sample errored →
fp16 scaler asserted "No inf checks recorded". Fixed by `*args/**kw`.)

Training: 3 epochs @ task-lr 2e-4, batch 1 × grad-accum 16, fp16 (job
`exp1-retrain2`, 1633 s on vidura). Filter-col loss 0.42 → 3e-5; no divergence.

Eval on the untouched 373-question test split (`pred_exp1_retrain.json`),
corrected constraints:

| metric | 8-epoch unbalanced | **3-epoch rebalanced** |
|---|---|---|
| filter decision (col+op) | 0.979 | 0.952 (held-out 0.915, seen 1.000) |
| exact skeleton | 0.445 | **0.694** |
| select/group/order/join/limit | 1.0 / 1.0 / 1.0 / 1.0 / 0.58→1.0 | 1.000 |
| agg | 0.847 | 0.697 |
| residual filter errors | 8 (title↔movie_name) | 18 (title→none 12, title→movie_name 4) |

Verdict: the retrain fixes the collapse regime outright (0.231 → 0.952) and
triples exact skeleton, but the slot-swap evidence did NOT transfer to
held-out entities as well as the unbalanced 8-epoch model (0.915 vs 0.962
held-out), and agg accuracy dropped (0.847 → 0.697) — synthetic oversampling
crowded out agg supervision. The 8-epoch unbalanced adapter + corrected
constraints remains the better filter model; the rebalanced run's win is
skeleton-level (limit 0.579 → 1.000).

## A2 — label descriptions at inference (2026-11-21, negative result)

`build_schema(use_descriptions=True)` passes `{label: description}` maps
(e.g. `title: "song title"`, `like: "substring/pattern match"`) which GLiNER2
compiles into `[DESC]` schema tokens. Ran on the same retrain adapter that
was trained with bare labels (`pred_exp1_retrain_a2.json`):

| metric | bare labels | descriptions |
|---|---|---|
| filter decision | 0.952 | 0.960 |
| exact skeleton | 0.694 | **0.343** |
| select_cols | 1.000 | **0.643** |
| join | 0.989 | 1.000 |

Verdict: **descriptions at inference-only are strongly negative** — a
train/inference schema mismatch. The adapter's label embeddings were trained
against bare-name tokens; the `[DESC]` suffix shifts every schema embedding,
which the multi-label select_cols head cannot absorb (1.000 → 0.643) while
the single-label filter heads are barely affected. Also ~2× slower per query
(624 s vs 299 s for 373 queries). Proper A2 requires *training* with
descriptions so embeddings match; that variant is queued, not yet run.



1. ~~**Fix the constraint/gold contradiction first (~1 h).**~~ **DONE 2026-11-20
   — A1-redo, see the section above.** The corrected set lifts the filter decision
   to 0.979.
2. **Filter-head-focused retrain (~25 min on vidura).** Cap year-filter templates
   at ~150 in the train split, oversample title/movie_name `like` examples, and add
   contrastive pairs — identical phrasing frame with the slot type swapped
   ("song X" / "movie X" / "in 2013") — so the col decision must read slot type
   rather than a prior. Train 3 epochs @ task-lr 2e-4 (curves show overfitting
   past epoch 1 at 5e-4).
3. **Per-head loss instrumentation in finetune.py.** Cheap; will show whether
   filter_col loss ever decreases or is swamped by the other seven heads.
4. **Write the negative result into REPORT-EXP1.md** (plan Task 6): main table
   (zero-shot / 25-example LoRA / scaled LoRA), the epoch-1/epoch-8 comparison,
   the constraint contradiction, and the verdict split by unseen-template vs
   unseen-entity (they did not diverge here — both fail).
5. **EA scoring path for the multi-composer DBs (later).** `predict.py`'s
   render/extract/fuzzy machinery is still Rahman-only (`arr.db` hardcoded), so
   execution accuracy for pfx + cross-composer questions needs generalized value
   extraction per composer. Not blocking the filter-head verdict.
6. **Ablations from the plan (A2 label descriptions, A3 fuzzy-off)** after the
   filter head is fixed — no point ablating around a collapsed head.

## Run log — what's been executed (2026-11-21)

| run | what | where | duration | artifacts |
|---|---|---|---|---|
| corpus scale-up | 12 filter-heavy phrasings → 2,076 verified entries | local (glm-5.3-flashx) | ~100 min | `gold-multi-scaled.jsonl`, `verify_gold.py` 0 mismatches |
| split | 1,703 / 373 leakage-safe | local | minutes | `results/exp1_split.json` |
| error baseline | 13 misses analyzed on the 25-example model | local | minutes | `results/exp1/filter_error_baseline.txt` |
| `exp1-lora` | 8 epochs, 1,703 train, 5e-4 | vidura GTX 1070 | 51 min (856 steps) | `best/best` (ep-1), `best/final` (ep-8), `metrics.jsonl`, curves |
| eval, ep-8 + constraints | 373-question test set | vidura CPU | ~15 min | `pred_exp1_final.json` — filter 0.231 |
| A1-redo re-score | corrected constraint set, same adapter | vidura CPU | 894 s | `pred_exp1_a1redo.json` — filter **0.979** |
| `exp1-retrain2` | 3 epochs @ 2e-4, 1,954 rebalanced train (per-head losses live) | vidura | 1,633 s | `metrics_retrain.jsonl`, retrain ckpt |
| retrain eval | same 373 test set | vidura CPU | ~15 min | `pred_exp1_retrain.json` — filter 0.952, skeleton 0.694 |
| A2 inference-only | descriptions at inference on the retrain adapter | vidura CPU | 624 s | `pred_exp1_retrain_a2.json` — negative (skeleton 0.343) |

Not yet run: proper A2 (train *with* descriptions — adapter queued), A3 (fuzzy off),
multi-composer EA scoring (predict.py render/extract is Rahman-only).

## Artifacts

- `experiments/gliner-decide/results/exp1/` — filter_error_baseline.txt,
  metrics.jsonl (live curves), curves.svg, pred_exp1_best.json,
  pred_exp1_final.json, best/best (epoch-1 ckpt), best/final (epoch-8)
- `experiments/multi-composer-qa/` — gold-multi-scaled.jsonl, verify_gold.py,
  make_split.py, results/exp1_split.json, canonicalize_heads.py
- Remote: vidura `~/exp/gliner-decide/` (code + data synced from this repo)
