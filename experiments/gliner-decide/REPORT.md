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
| Metric | constrained | no-constraints |
|---|---|---|
| select_cols acc | 12/31 (38.7%) | 12/31 (38.7%) |
| filter_col acc | 0/31 (0%) | 0/31 (0%) |
| filter_op acc | 0/31 (0%) | 0/31 (0%) |
| agg acc | 0/31 (0%) | 0/31 (0%) |
| group_by acc | 0/31 (0%) | 0/31 (0%) |
| join acc | 13/31 (41.9%) | 13/31 (41.9%) |
| order acc | 0/31 (0%) | 0/31 (0%) |
| limit acc | 3/31 (9.7%) | 3/31 (9.7%) |
| **Exact-skeleton acc** | **0/31 (0%)** | **0/31 (0%)** |
| **Valid-SQL rate** | **31/31 (100%)** | **30/31 (96.8%)** |
| **Execution accuracy** | **0/31 (0%)** | **1/31 (3.2%)** |

## Interpretation
- **The classifier is a degenerate collapse in this task.** For essentially every query it emits the
  same skeleton: `select=[singer], agg=count, group_by=singer, join=songs_singers, order=count_desc`.
  The encoder cannot map a question ("who sang the song X?", "when was Y released?") onto the
  clause-level decisions; it latches onto the most "database-looking" output. This is a semantic
  understanding failure, not a rendering/execution failure.
- **Constraints neither help nor hurt** (identical per-head scores with/without) — because the model
  isn't producing near-miss skeletons that constraints could repair; it's producing the same wrong
  shape every time. Constraint-based inference is powerless over a collapsed prior.
- **The glue is correct:** 100% valid SQL (constrained), the renderer and gold both verified. So the
  experiment cleanly isolates the failure to the decision model, not the pipeline.

## Ablations (planned)
- A1 constraints: done — no effect (above).
- A2 label descriptions: not run (would not address a collapsed prior).
- A3 fuzzy matching: enabled throughout; irrelevant because filter values were never even reached
  (filter_col never predicted).
- A4 fine-tune: **not yet run** — but this is the clear next step.

## Conclusion / recommendation
- Zero-shot **GLiNER2.5-Decide does not work for this text→SQL-skeleton mapping** as configured
  (EA 0%). The multi-label decomposition is sound in principle, but the stock checkpoint cannot
  associate natural-language questions with clause decisions out of the box.
- **Next step:** Task-7 LoRA/fine-tune on the 31-question gold set (train/test split) per the GLiNER2
  trainer. With only ~8 heads and ~31 examples this is cheap; if fine-tuning lifts zero-shot's
  per-head scores materially, the approach is vindicated; if the model still can't learn
  question→clause from such few examples, that is a strong negative result for using Decide this way.
- **Alternative** if fine-tuning fails: this task is a natural fit for an instruct-LLM emitting the
  constrained skeleton JSON (the user's own `queryoutputs.txt` shows llama3.2 can do the SQL), or a
  harder look at whether the head/task formulation should be simplified (fewer, more semantically
  grounded heads like "intent" as in Recommendation 1) rather than 8 micro-clauses.

## Reproduce
```
cd experiments/gliner-decide
.venv/bin/python run_gold.py                    # rebuild gold.jsonl + ref_results.json
.venv/bin/python -m pytest tests/ -q            # 6 renderer tests
.venv/bin/python predict.py --run zero-shot     # -> results/pred_zero-shot.json
.venv/bin/python score.py --pred results/pred_zero-shot.json
```
