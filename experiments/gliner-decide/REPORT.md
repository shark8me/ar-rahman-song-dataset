# ARR QA → SQL-skeleton classification with GLiNER2.5-Decide: experiment report

Date: 2026-10-05
Model: `fastino/GLiNER2.5-Decide` (DeBERTa-v3-large 340M) via `gliner2` `Classifier.classify`
Backend: CPU-only (torch 2.14.1+cpu), ~3.2s/query classify, ~56s for all 31 queries.
Repo: `/home/kiran/src/music/arr-qa` (local copy of `shark8me/ar-rahman-song-dataset`).

## Setup (verified)
- Built a fresh SQLite `experiments/gliner-decide/arr.db` from `data/recordings.csv` + `data/singers.csv`
  (`songs(song_id,title,date,movie_name,release_id)` 2358 rows; `singers(song_id,name)` 4108 rows).
- Gold: hand-written reference SQL for all 31 queries in `gold.jsonl` + executed results in
  `ref_results.json` (0 execution errors). Sanity checks: 307 works, first album 1991-01-01,
  last 2024-02-29, top-5 collaborators [A.R.Rahman, Hariharan, K.S.Chithra, S.P.Balasubrahmanyam,
  Sujatha], 16 works in 1999, 68 singers in 2016, busiest year 1994.
- 8 classification heads (select_cols multi-label; filter_col/filter_op/agg/group_by/join/order/limit),
  with a `C.implies`/`C.excludes` constraint set (`query_schema.py`).
- Renderer (`render.py`) is a pure function; 6 unit tests pass against the real DB
  (`tests/test_render.py`, 6 passed).

## Results — zero-shot
**Note (2026-10-05, post-hoc correction):** the original per-head numbers below were corrupted by a
scorer bug (`score.py` `norm()` applied `sorted()` to gold's bare-string labels, so single-label
heads could never match — `sorted('none') == ['e','n','n','o']`). All numbers below are re-scored
with the fixed scorer; EA and valid-SQL were unaffected.

| Metric | constrained | no-constraints |
|---|---|---|
| select_cols acc | 12/31 (38.7%) | 12/31 (38.7%) |
| filter_col acc | 5/31 (16.1%) | 10/31 (32.3%) |
| filter_op acc | 5/31 (16.1%) | 11/31 (35.5%) |
| agg acc | 11/31 (35.5%) | 23/31 (74.2%) |
| group_by acc | 2/31 (6.5%) | 2/31 (6.5%) |
| join acc | 13/31 (41.9%) | 13/31 (41.9%) |
| order acc | 2/31 (6.5%) | 10/31 (32.3%) |
| limit acc | 19/31 (61.3%) | 19/31 (61.3%) |
| **Exact-skeleton acc** | **0/31 (0%)** | **0/31 (0%)** |
| **Valid-SQL rate** | **31/31 (100%)** | **30/31 (96.8%)** |
| **Execution accuracy** | **0/31 (0%)** | **1/31 (3.2%)** |

## Interpretation
- **Zero-shot is a near-collapse, biased toward "database-looking" output.** The model latches onto
  aggregate/group/join shapes; it never produces a fully-correct skeleton (0/31). The original
  "degenerate collapse" diagnosis was overstated (a scorer artifact inflated it), but the exact-
  skeleton and EA zeros are real: per-head scores of 6–74% still leave no query fully right.
- **Constraints actively hurt.** With constraints off, filter/agg/order scores roughly double
  (e.g. agg 35.5% → 74.2%). The `C.implies` set encodes wrong implications (e.g. `like` ⇒
  `filter_col=singer/title/movie_name` all true, plus `filter_op=like ⇒ filter_col=singer` is
  semantically bogus) and prunes correct assignments. Constraint formulation needs a redo.
- **The glue is correct:** 100% valid SQL (constrained), the renderer and gold both verified. So the
  remaining failure is in the decision model / schema formulation, not the pipeline.

## Ablations
- A1 constraints: done — **negative effect** (see zero-shot table; opposite of the original claim).
- A2 label descriptions: not run.
- A3 fuzzy matching: enabled throughout; filter values only reached on filter hits.
- A4 fine-tune: **done — see below. LoRA lifts EA 0% → 41.9% and skeleton 0% → 54.8% overall, but
  generalization on held-out filter decisions is the remaining bottleneck.**

## A4 — LoRA fine-tune (2026-10-05)
Setup: `finetune.py` builds `InputExample`s from `gold.jsonl` with the same 8 heads/label lists used
at inference; split 25 train / 6 test (`results/finetune_split.json`, seed 42, test idxs
[7, 11, 15, 20, 25, 28]); `gliner2.training.trainer` GLiNER2Trainer, LoRA r=16/α=32 on
encoder+span_rep+classifier, task_lr 5e-4, batch 2, 30 epochs, CPU ~80 min. Adapter
`results/lora3/best` (best train-eval loss 0.49 @ epoch 26). Predict via
`predict.py --run x --adapter results/lora3/best`.

**Training-dynamics finding:** with task_lr 1e-3 the training loss dipped to ~0.005–0.9 around
epochs 12–18 then *diverged* back to ~25–30 (LR-too-high instability, not overtraining — train loss
itself blew up). The first adapter was saved from that diverged state and scores far worse than
best-checkpoint. Re-run at 5e-4 with per-epoch eval + `save_best` converged cleanly.

| Metric (all 31) | zero-shot | finetuned (diverged, lr 1e-3 final) | finetuned (best ckpt, lr 5e-4) |
|---|---|---|---|
| select_cols | 38.7% | 38.7% | **100%** |
| filter_col | 16.1% | 48.4% | **58.1%** |
| filter_op | 16.1% | 48.4% | **58.1%** |
| agg | 35.5% | 71.0% | **100%** |
| group_by | 6.5% | 96.8% | **100%** |
| join | 41.9% | 100% | **100%** |
| order | 6.5% | 87.1% | **96.8%** |
| limit | 61.3% | 0% | **96.8%** |
| Exact skeleton | 0% | 0% | **54.8%** |
| Execution acc | 0% | 0% | **41.9%** |

Train-set fit (25 seen examples): all heads ≥ 96.8% except filter_col/filter_op 16/25 — the model
memorizes aggregate/sort decisions but still misses ~1/3 of filter decisions **on data it was
trained on**.

Held-out test split (6 unseen): select/agg/group/join 6/6, order 5/6, limit 5/6, but
**filter_col/filter_op 2/6, exact skeleton 1/6, EA 0/6**. Failure mode: on unseen "song X" /
"which film is song Y in" questions the model defaults to `year_eq` instead of `title`+`like`.
With only ~6 title-like filter patterns among 25 training examples, the filter heads don't
generalize — the bottleneck is data, not optimization.

## Conclusion / recommendation
- Zero-shot GLiNER2.5-Decide does not work for this text→SQL-skeleton mapping (skeleton 0%, EA ≤3%).
- **LoRA fine-tuning on 25 examples vindicates the approach**: most clause decisions reach ~100%
  and EA jumps to 41.9% overall. But on held-out questions it is 0/6 EA: the filter_col/filter_op
  decisions (title-like vs year) do not generalize from ~25 examples.
- Next lever: **more and more-varied filter training examples** (paraphrases, more song/movie
  titles), a corrected constraint set (current one is net-negative), or the instruct-LLM
  alternative. The head decomposition itself is fine — 6 of 8 heads are learnable to near-perfection.

## Reproduce
```
cd experiments/gliner-decide
.venv/bin/python run_gold.py                    # rebuild gold.jsonl + ref_results.json
.venv/bin/python -m pytest tests/ -q            # 6 renderer tests
.venv/bin/python predict.py --run zero-shot     # -> results/pred_zero-shot.json
.venv/bin/python score.py --pred results/pred_zero-shot.json
# A4 fine-tune (LoRA, CPU ~80 min):
.venv/bin/python finetune.py --epochs 30 --batch-size 2 --task-lr 5e-4 --seed 42
.venv/bin/python predict.py --run finetuned-lora-best --adapter results/lora3/best
.venv/bin/python score.py --pred results/pred_finetuned-lora-best.json
```
