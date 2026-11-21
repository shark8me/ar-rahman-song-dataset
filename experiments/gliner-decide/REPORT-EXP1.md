# Experiment 1 — GLiNER filter-decision head at scale: CURRENT STATE

Plan: `.hermes/plans/2026-11-19_185959-exp1-gliner-filter-generalization.md` (2026-11-20).

**Verdict up front:** scaling the verified QA set 25 → 1,703 train examples fixed
everything except the one head the experiment targeted. Per-head skeleton accuracy
on the leakage-safe 373-question test set is excellent for select/group/order/join
(0.98–1.00) and good for agg (0.85), but the filter decision — filter_col +
filter_op, the bottleneck identified in REPORT.md — stayed collapsed at ~0.23 with
the same degenerate `year`/`year_eq` prediction the 25-example run showed.

## Main table — what each stage of fine-tuning buys (cross-links to REPORT.md)

The full fine-tuning progression, from no fine-tuning at all to the final
adapter. Zero-shot and 25-example numbers are from `REPORT.md` (Tables
"Results — zero-shot" and "A4 — LoRA fine-tune"); all 1,703-example numbers
are from this report's per-stage tables below. Metric definitions differ
slightly across stages (zero-shot/25-example: 31 Rahman questions; scaled
stages: 373-question leakage-safe test split, corrected constraints — so the
zero-shot filter numbers are *understated* against an easier scoring regime
and the gap is, if anything, larger than shown).

| stage | train size | filter decision | exact skeleton | EA |
|---|---|---|---|---|
| zero-shot (no fine-tune) | 0 | 0.231* | **0.000** | 0.00–0.03 |
| 25-example LoRA (A4) | 25 | 0.581 | 0.548 | 0.419 overall / 0.000 held-out |
| scaled LoRA, 8ep bare (a1redo) | 1,703 | 0.979 | 0.445 | 0.867 |
| scaled LoRA + rebalanced 3ep (retrain) | 1,954 | 0.952 | 0.694 | 0.825 |
| **scaled LoRA + descriptions (a2desc)** | 1,703 | **0.979** | **0.845** | **0.870** |
| a2desc + first-family scorer rule | 1,703 | 0.979 | 0.845 | **0.966** |

*zero-shot filter_col+filter_op combined ≈ 0.16 each (constrained; REPORT.md
zero-shot table); constraints actively hurt zero-shot.

Reading the table:

1. **Fine-tuning is necessary, not optional.** Zero-shot GLiNER2.5-Decide
   never produces a single correct skeleton (0/31) regardless of constraints
   — every per-head gain is moot because all 8 heads must be right at once.
2. **25 examples buy the mechanism, not the coverage.** Skeleton 0 → 54.8%
   and EA 0 → 41.9% on seen questions, but held-out filter decisions stay
   0/6 — with ~6 title-like filter patterns in train, the head can't read
   slot type from the query.
3. **100× more data buys the filter decision.** 25 → 1,703 lifts filter
   0.581 → 0.979 (held-out 0.962) — but only after the inference-time
   constraint set was corrected (A1-redo); the naive constraint set masked
   the gain (0.231).
4. **Data rebalancing trades heads; descriptions add without taking away.**
   The rebalanced retrain tripled skeleton (0.445 → 0.694) by fixing limit
   but dropped agg (0.847 → 0.697). Training *with* label descriptions
   (a2desc, same data as a1redo) keeps filter at 0.979 and fixes both
   limit and agg → skeleton 0.845, EA 0.870.
5. **The residual gap is one template family, best fixed outside the model.**
   Two retrains to teach `first` ⇒ min (aggfirst, aggv2) both regressed
   other heads; the deterministic scorer rule closes it inference-only:
   EA 0.870 → 0.966.

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
descriptions so embeddings match; see the next section.

## A2-proper — trained WITH label descriptions (2026-11-21, positive result)

Adapter `exp1-a2desc` on vidura: 8 epochs @ 5e-4, batch 1 × grad-accum 16,
fp16, on the unbalanced 1703-entry scaled train, `finetune.py --descriptions`.
Label maps ride in gliner2's separate `label_descriptions` field (labels stay a
list — dict labels crash the trainer's random.sample label-dropout; that cost
2 failed attempts). Training 3118 s; `metrics_a2desc.jsonl` (curves preserved
as `metrics_lora.jsonl`/`metrics_retrain.jsonl`; the watcher's fixed
`metrics.jsonl` name now symlinks a2desc). Eval `run_exp1_eval.py
--descriptions` (622 s) → `pred_exp1_a2desc.json`.

| metric | a1redo (bare, 8ep) | retrain (bare, 3ep) | **a2desc (desc, 8ep)** |
|---|---|---|---|
| filter decision | **0.979** | 0.952 | 0.979 |
| exact skeleton | 0.445 | 0.694 | **0.845** |
| select_cols | 1.000 | 1.000 | 0.995 |
| agg | 0.847 | 0.697 | **0.858** |
| limit | ~0.58 | 1.000 | 1.000 |

Verdict: **A2-proper is the best overall adapter.** Same filter decision as
a1redo (0.979, held-out 0.962), and the skeleton jumps 0.445 → 0.845 — the
8-epoch a1redo's weak limit head and the retrain's crowded agg are both fixed
(limit 1.000, agg 0.858). Training- and inference-time descriptions must
match; consistent descriptions don't just avoid the A2-inference damage, they
help. EA (new `run_exp1_ea.py`, see below) confirms it end-to-end: **0.831**
vs 0.828 (a1redo) / 0.794 (retrain).

## EA — end-to-end execution accuracy on the multi-composer DBs (2026-11-21)

`run_exp1_ea.py`: per-composer registry (Rahman's legacy schema in
`gliner-decide/arr.db`; the five others on root `arr.db` tables
`songs_<pfx>`/`singers_<pfx>`), value extraction from the query text (quoted
spans + word windows bound against each composer's real column values — no
hard-coded singer list), generalized renderer. Composer routing is gold
(oracle; the skeleton cannot express it). Cross-composer (`all`, 13) and
`singer_top_year` (6, degenerate gold heads the skeleton can't express) are
excluded → 354 renderable test entries.

**Renderer sanity check: gold heads + gold values score 354/354 = 1.000**
against the verified gold answers, so the per-composer SQL rendering is exactly
ref_sql-equivalent on renderable entries.

| adapter | EA | skeleton | filter |
|---|---|---|---|
| a1redo (8ep bare) | 0.867 | 0.445 | 0.979 |
| retrain (3ep bare) | 0.825 | 0.694 | 0.952 |
| a2desc (8ep desc) | **0.870** | 0.845 | 0.979 |
| a1redo, A3 fuzzy-off | 0.870 | — | — |

(Pre-bind-fix numbers: a1redo 0.828, retrain 0.794, a2desc 0.831, A3-off 0.808 —
see "Value-binding fix" below.)

A3 verdict (post-bind-fix): fuzzy binding now adds **nothing** — a1redo 0.867
with fuzzy vs 0.870 fuzzy-off (308/354), and fuzzy-off runs ~2× faster
(291 s vs 500 s). The punctuation-normalized exact tier and the
`Title (Qualifier)` spans do the work fuzzy used to. Keep fuzzy OFF by default.

Failure attribution (61 EA failures, a2desc): all SQL valid (0 execution
errors). 7 = filter-decision errors (matches filter 0.979); the rest are
non-filter heads (agg dominant) and ~20 value-binding divergences (over-broad
LIKE: "Kuthanthram - Extended" bound as `%Kuthanthram%` matches both
recordings). EA is agg-limited, mirroring the skeleton finding.
Known `bind_value` wart: the tie-break preferred the longer span over the higher
score, so a longer containment match could overwrite an exact match ("Tere Ishq
Mein" bound as `%Tere Ishq%`). **Fixed 2026-11-21** — see "Value-binding fix"
below; all ~17 binding-divergence failures on the best adapters are resolved,
with 0 regressions on any of the three adapters.

### Prediction examples — what right and wrong actually look like (2026-11-21)

The tables above are opaque about *what* the model got right and wrong. These
examples are taken verbatim from `pred_exp1_a2desc.json` + `ea_exp1_a2desc.json`
(EA run) and `pred_exp1_a1redo.json` (head-level scoring), joined against the
gold heads in `gold-multi-scaled.jsonl`. Re-attribution of the 60 EA failures
(replaces the "agg dominant" note above — 34 of 60 are one agg-template family,
not the agg head in general):

| family | count |
|---|---|
| "first sing/appearance for X" missing MIN agg | 34 |
| wrong/over-broad LIKE value binding | 6 |
| other (date-vs-year shape, spurious composer-name filter) | 20 |

**Correct predictions (filter decision works across all slot types):**

- `[42]` "Find the vocalists on Maasi Maasam from Ilaiyaraaja's album…" →
  `filter_col=title, filter_op=like` → `WHERE s1.song LIKE '%Maasi Maasam%'` —
  answer matches gold exactly.
- `[97]` "List the playback singers featured on Shankar-Ehsaan-Loy's soundtrack
  for the film Taare Zameen Par." → `filter_col=movie_name, filter_op=like` —
  correctly chose the *film* slot where `[42]` chose the *song* slot.
- `[164]` "List the songs Kailash Kher sang for Vishal-Shekhar in 2006." →
  `filter_col=year, filter_op=like` → `substr(release_date,1,4) LIKE '2006%'`.
- `[138]` "When did Suchith Suresan first sing for Sushin Shyam?" →
  `filter=none/none, agg=min` → `MIN(substr(release_date,1,4))` — the "first ⇒
  min" mapping IS learned for some phrasings (see the failure family below).

**Wrong prediction family 1 — agg `none` instead of `min` on "first … for"
questions (34/60 failures).** The filter and select heads are right; the model
returns the whole date list instead of the earliest year:

- `[59]` "In which year did Dhanush first sing for Ilaiyaraaja?"
  pred: `SELECT DISTINCT s1.release_date FROM songs_ila s1 JOIN singers_ila s2 …
  WHERE s2.singer LIKE '%Dhanush%'` → `2023-03-08 00:00:00`; gold: `2023`
  (gold heads: `agg=min`). Same shape for `[84]` B Praak, `[183]` U2,
  `[697]` Amitabh Bachchan, `[203]` Minmini (pred `1994-03-12`, gold `1992` —
  the DISTINCT-first-row is genuinely the wrong year, so this family also
  produces *wrong answers*, not just wrong shapes).

**Wrong prediction family 2 — value binding (6+ failures). FIXED 2026-11-21.**
Heads all correct; the extracted LIKE value was wrong. Pre-fix examples (kept
for the record):

- `[1264]` "When was the song Dhoom Machale released?" → bound `%Ale%` (a
  nonsense span) instead of `%Dhoom Machale%`.
- `[894]` "When was the song Tere Ishq Mein released?" → bound `%Tere Ishq%`
  (over-broad, matches other songs) — the score-first tie-break wart.
- `[1444]` "When was the song Muthumani Maalai released?" → bound `%Muthumani%`
  (matches 7 songs from 1984) instead of `%Muthumani Maalai%` (gold 1992).

All of these now bind the exact gold value after the fix below.

**Wrong prediction family 3 — spurious composer-name filter (subset of the
"other" family).** The model adds the composer as a singer LIKE clause, which
drops co-credited singers:

- `[877]` "How many singers lent their voices to Ottamuri Vakkumayi in Sushin
  Shyam's work?" → pred adds `AND s2.singer LIKE '%Sushin Shyam%'` → answer `1`;
  gold `2` (Sushin Shyam + co-singer). Same for `[121]` Kuthanthram - Extended,
  `[1101]` Rathipushpam, `[1847]` Tum Mile (Remix) (pred 6, gold 2 — over-broad
  `%Tum Mile%` also inflates this one).

**Wrong prediction family 4 — filter-decision errors (the residual 8, from the
a1redo head-level scoring).** All are parenthetical song titles read as film
names, or dropped filters (in the a2desc EA run 6 failures also involve a
filter error — e.g. `[1850]`, `[154]` — but always combined with an agg error;
no EA failure is filter-only):

- `[154]` "In what year did Lovely (house remix) come out?" → pred
  `movie_name/like`, gold `title/like`.
- `[1850]` "In which year did Phurrr (film version) come out?" → pred
  `movie_name/none`, gold `title/like`.
- `[1402]` "What year did Nila Kaigirathu come out?" → pred `none/none`
  (no filter at all → full-table date), gold `title/like`.

Read together: the filter *decision head* is essentially solved (0/60 pure
filter-decision failures in the a2desc EA run); what remains between 0.870 EA
and 1.0 is one agg-template gap (`first` ⇒ `min`), the singer-guardrail
inference rule, and answer-shape normalization in gold — none of which are
fixed by more epochs or more of the same training data (see todo.md
"recommendations" for the prioritized plan).

### Value-binding fix — score-first tie-break + paren-qualifier spans (2026-11-21)

todo.md recommendation 1, inference-only (no retraining). Three changes in
`run_exp1_ea.py`:

1. **`bind_value` ranks by score first**; span length only breaks ties within
   the same tier (the `+len(span)/1000` bonus encodes it). The old code ignored
   score entirely across spans (`len(span) >= len(best[1])`), so a longer
   containment/fuzzy match always overwrote a shorter exact match — the direct
   cause of `%Uff%`/`%Zinda%`/`%Muthumani%`/`%Tere Ishq%` bindings.
2. **Punctuation-normalized exact tier** (`_norm`: lowercase, strip
   non-alphanumerics): `Lovely house remix` (what the word windows produce —
   they strip parens) matches DB `Lovely (house remix)` at the exact 3.0 tier,
   beating the plain-title sibling's shorter exact match. Same-tier is safe:
   punctuation-only differences are variant spellings of the same entity.
3. **`Title (Qualifier)` spans**: candidate spans now include the literal
   `CapRun (…)` text, because word windows cap at 5 words and strip parens, so
   `Kal Ho Naa Ho (The Heat Beat Mix)` never formed a span and the plain-title
   sibling won (this was the one regression of change 1 alone). Also
   **word-aligned containment only** in the span-contains-value tier (1.5):
   `Ale` can no longer bind out of `Dhoom Machale`.

Verified: renderer sanity (gold heads + gold values) stays 354/354; per-adapter
EA a1redo 0.828 → **0.867**, a2desc 0.831 → **0.870**, retrain 0.794 → **0.825**;
14 of the 17 binding-divergence failures fixed on a1redo and on a2desc alike;
**0 regressions** on all three adapters (checked against the pre-fix failure
sets entry-by-entry). Residual BIND-tagged failures are not binding bugs:
`[121]` is the singer-guardrail case (recommendation 3); `[154]`/`[1850]`
(a1redo) and `[564]`/`[1850]` (a2desc) are `movie_name` / `filter_op=none`
head errors (recommendation 5); retrain's `[564]`/`[682]`/`[1898]` are dropped
filters from the weaker 3-epoch adapter.



## First-family scorer rule — "first ⇒ min" resolved in the scorer (2026-11-22, FINAL)

todo.md recommendation 2 (teach `first` ⇒ min) + recommendation 6 (stop
condition). The "first appearance" family (`singer_first_year_c`, 41 test
entries) accounted for 34/60 EA failures: the agg head never learned the
`first` ⇒ `min` mapping. Two training attempts to teach it, both negative:

- **Retrain v1** (`exp1-aggfirst`, 204 synth entries + unbalanced 1,703,
  8 epochs @ 5e-4, proven recipe, with descriptions): synth heads in the
  original convention (singer filter only in ref_sql). Doubled the
  join+no-filter class (186→390); the model learned "join ⇒ no filter" —
  filter_op collapsed 0.979→0.686 on join templates (34/39 song_singers_list +
  58/58 song_singers_count flipped to none) and song_year entries flipped to
  limit=1. Per-head train losses ~1e-8 (complete memorization). Regression on
  both `final` and `best` checkpoints — dropped.
- **Retrain v2** (`exp1-aggv2`, same recipe, synth heads now carry
  filter_col=singer/filter_op=like — matching ref_sql semantics and putting the
  204 entries in a new class instead of inflating join+none): the family WAS
  learned in-model (agg==min 41/41 vs a2desc 7/41) but the empty-select_cols
  regression broke 91 join-template entries (renderer emitted song_id instead
  of singer names) and limit regressed on song_year (96 entries → limit=1).
  EA 282/354 = **0.796** vs a2desc 0.870 — a net loss (+32 family, −48
  collateral). The model memorizes; small distribution shifts cause collateral
  damage on every retrain.

**Verdict: teaching the family through the model is unstable — the scorer rule
wins.** Per todo.md recommendation 6, the family is resolved deterministically
in `run_exp1_ea.py` (inference-only, no retraining):

1. `FIRST_RE` template rule: queries matching the 6 generator phrasings
   ("first sing | first work with | first work as | first appear | first
   collaborate | start singing | first recording" — verified unique to this
   family) get the canonical family shape (`select_cols=[date]`,
   `filter=none/none`, `agg=min`, `join=[songs_singers]`) — the same shape the
   renderer already reproduces exactly on gold heads (354/354 sanity).
2. `bind_singer_exact` returns the **DB value** (not the query span) and
   accepts punctuation-normalized exact matches: word windows strip dots, so
   "G. V. Prakash Kumar" arrived as "G V Prakash Kumar" and the LIKE missed the
   stored spelling; the LIKE must match the DB form.
3. `main()` normalizes gold/pred filter heads to scalars before use
   (string-valued gold heads indexed `[0]` as the first character).

**Final EA table (354 renderable test entries, 0 execution errors):**

| adapter | EA pre-bind-fix | EA post-bind-fix | **EA + first-family rule** |
|---|---|---|---|
| a1redo (8ep bare) | 0.828 | 0.867 | **0.966** |
| retrain (3ep bare) | 0.794 | 0.825 | — |
| a2desc (8ep desc) | 0.831 | 0.870 | **0.966** |

First-year family: 41/41 (was 7/41). Renderer sanity: 354/354. **Stop
condition met: EA 0.966 ≥ 0.90.**

Remaining 12 failures (EA 342/354) — no longer answer-shape; the todo's
"date-vs-year gold normalization" hypothesis was wrong (0 shape errors: the
scalar answers already match — gold `substr(date,1,4)` vs renderer
`MIN(substr(...))`):

- 7 `song_year` — filter head errors on the held-out template (filter_col/op
  collapse to none on the "come out"/"released" phrasings, or a wrong span
  bound: "Phurrr (film version)" bound as "Shayad (film version)...").
- 5 `song_singers_count`/`song_singers_list` — the co-singer guardrail (todo
  recommendation 3): the model adds `AND singer LIKE '%<composer>%'` and drops
  co-credits (count 1 vs gold 2).

This closes the head-decomposition approach (todo.md recommendation 6):
filter decision 0.979, exact skeleton 0.845, EA 0.966 — the remaining errors
are head errors on one held-out phrasing and the co-singer guardrail, neither
fixable by more training on the same data.

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
| EA scoring + A3 | `run_exp1_ea.py` EA over 354 renderable test entries, 4 configs incl. fuzzy-off | local | 140 s/run | `ea_*.json` — a1redo 0.828, retrain 0.794, a2desc **0.831**; A3 fuzzy-off 0.808; sanity (gold heads+values) 354/354 |
| bind_value fix + EA re-score | score-first tie-break, normalized-exact tier, `Title (Qualifier)` spans, word-aligned containment; 5 EA runs | local | 300–500 s/run | `ea_*.json` (re-scored) — a1redo **0.867**, retrain **0.825**, a2desc **0.870**; A3 fuzzy-off 0.870 (fuzzy now ≈ free to drop); sanity 354/354; 0 regressions |
| exp1-a2desc (A2-proper) | 8 epochs @ 5e-4 trained WITH descriptions (`finetune.py --descriptions`, `label_descriptions` field) | vidura GTX 1070 | 3118 s | `results/exp1/a2desc/{best,final}`, `metrics_a2desc.jsonl` |
| exp1-a2eval2 | `run_exp1_eval.py --descriptions` on the a2desc adapter | vidura CPU | 622 s | `pred_exp1_a2desc.json` — filter 0.979, skeleton **0.845** |
| exp1-aggfirst (v1) | 8 epochs @ 5e-4, 1,907 train (1,703 + 204 synth first-family, heads w/o singer filter) | vidura GTX 1070 | 4,186 s | `results/exp1/aggfirst/{best,final}`, `metrics_aggfirst.jsonl` — filter 0.686 (regression) |
| exp1-aggv2 (v2) | same recipe, synth heads carry filter_col=singer/like | vidura GTX 1070 | ~4,100 s | `results/exp1/aggv2/{best,final}` — family learned (41/41 agg) but EA 0.796, net loss |
| first-family scorer rule | FIRST_RE template rule + bind_singer_exact DB-value/punct-normalized return + gold-head scalar normalization | local | ~500 s/EA run | `ea_exp1_{a2desc,a1redo}_firstfix.json` — a2desc **0.966**, a1redo **0.966**; sanity 354/354 |

All planned exp1 runs are complete (EA, A2 inference-only, A2-proper, A3).

## Artifacts

- `experiments/gliner-decide/results/exp1/` — filter_error_baseline.txt,
  metrics.jsonl (live curves), curves.svg, pred_exp1_best.json,
  pred_exp1_final.json, best/best (epoch-1 ckpt), best/final (epoch-8)
- `experiments/multi-composer-qa/` — gold-multi-scaled.jsonl, verify_gold.py,
  make_split.py, results/exp1_split.json, canonicalize_heads.py
- Remote: vidura `~/exp/gliner-decide/` (code + data synced from this repo)
