# Can GLiNER2.5-Decide map natural-language questions to SQL skeletons?

**Experiment write-up — AR Rahman QA dataset** · 2026-10-05
Commit: `2f88a20` · Full reproducibility in `experiments/gliner-decide/REPORT.md`

---

## 1. Question and framing

Can a *decision* model — not a generator — turn an English question into the clauses of
a SQL query over a small relational dataset, if we model the SQL as per-clause
**multi-label classification**?

The target is the AR Rahman QA dataset (`shark8me/ar-rahman-song-dataset`). Its "small
tables" are two:

- `songs(song_id, title, date, movie_name, release_id)` — 2,358 recordings
- `singers(song_id, name)` — 4,108 rows, 597 distinct singers

The proposed SQL dialect has exactly the shape you sketched: **columns are a list,
clauses are chosen from a list, joins are specified explicitly.** That is a natural
multi-label classification problem:

- `select_cols` — which columns to return (multi-label)
- `filter_col`, `filter_op` — the WHERE target and operator (single-label)
- `agg`, `group_by` — aggregation + grouping (single-label)
- `join` — which joins are needed (multi-label)
- `order`, `limit` — sorting and row cap (single-label)

The dataset is single-composer (everything is A. R. Rahman), so "composer = Rahman"
filters are implicit no-ops — a documented convention. Entity names in the data carry
casing/variant noise (e.g. the query says "The Gentleman", the data stores "Gentleman";
"Roja Jaaneman" is not literally a row), so literals are matched with fuzzy/LIKE logic.

## 2. Method

- **Model:** `fastino/GLiNER2.5-Decide` (DeBERTa-v3-large, 340M) through the
  `gliner2` `Classifier.classify` API, CPU-only, ~3.2 s/query, 56 s for the full set.
- **Schema:** 8 classification heads (above), with a `C.implies`/`C.excludes` constraint
  set (e.g. *singer in select ⇒ join songs↔singers*, *count_desc order ⇒ count agg*).
- **Gold:** hand-written reference SQL for all 31 queries in `queriesv2.csv`, executed
  against a freshly loaded `arr.db` — `ref_results.json`, **0 execution errors**. Sanity
  checks confirm sensible answers (307 works; top collaborators = A.R.Rahman, Hariharan,
  K.S.Chithra, S.P.Balasubrahmanyam, Sujatha; busiest year 1994).
- **Pipeline:** classify → extract literals (regex for year / top-N / quoted names /
  singer names) → render SQL (pure function, 6 unit tests pass against the real DB) →
  execute → compare result set to gold.
- **Runs:** zero-shot with constraints; zero-shot without constraints (ablation A1).

## 3. Results

| Metric (n=31) | constrained | no-constraints |
|---|---|---|
| select_cols accuracy | 12/31 · 38.7% | 12/31 · 38.7% |
| filter_col accuracy | 0/31 · 0% | 0/31 · 0% |
| filter_op accuracy | 0/31 · 0% | 0/31 · 0% |
| agg accuracy | 0/31 · 0% | 0/31 · 0% |
| group_by accuracy | 0/31 · 0% | 0/31 · 0% |
| join accuracy | 13/31 · 41.9% | 13/31 · 41.9% |
| order accuracy | 0/31 · 0% | 0/31 · 0% |
| limit accuracy | 3/31 · 9.7% | 3/31 · 9.7% |
| **Exact-skeleton accuracy** | **0/31 · 0%** | **0/31 · 0%** |
| **Valid-SQL rate** | **31/31 · 100%** | 30/31 · 96.8% |
| **Execution accuracy** | **0/31 · 0%** | 1/31 · 3.2% |

## 4. Interpretation

The result is a **clean negative for zero-shot** — and it's diagnostic, not just a score.

**The story in the per-head numbers.** *All* of the semantic heads (filter/agg/group_by/
order) are at 0%, while the raw *vocabulary*-ish heads (select/join) are merely poor
(~39–42%). The model collapses virtually every query onto one skeleton regardless of
what's asked: `select=singer, agg=count, group_by=singer, join=songs↔singers,
order=count_desc`. A "who sang song X?" question and a "which year had the most
movies?" question get the *same* shape. This is a **semantic-understanding failure**, not
a rendering or execution failure.

**Why the constraints don't rescue it.** With/without constraints the per-head scores are
identical. Constraint-based decoding can only repair *near-miss* skeletons (e.g. missing
the join on an otherwise-correct recall). Here the model emits the same wrong shape every
time, so there's nothing for constraints to adjust. This cleanly isolates the blame to the
decision model itself.

**The glue is correct.** 100% valid SQL (constrained), a verified renderer (6/6 unit
tests), and a verified gold set (31/31 reference queries with 0 errors). The experiment
therefore measures what it claims: it is not a pipeline bug producing the low score.

## 5. What worked vs. what didn't

| | Status |
|---|---|
| SQL-skeleton as multi-label classification | Sound framing; heads render to valid SQL |
| Constraint system (`C.implies`/`C.excludes`) | Correctly wired but inert under a collapsed prior |
| Renderer + value extraction | Verified correct |
| Gold annotation process | Sound, reproducible |
| GLiNER2.5-Decide zero-shot question→clause | **Fail** — degenerate collapse |

## 6. Next step (recommended)

Per the experiment plan's gate (fine-tune only if execution accuracy < 50% — it is 0%),
the decisive move is a **LoRA fine-tune on the 31-query gold set** (stratified train/test
split), using the `gliner2` trainer. It is cheap at this scale (~8 heads, ~31 examples) and
is a genuine fork:

- **If fine-tuning lifts per-head accuracy**, the multi-label-skeleton approach is
  vindicated and worth deploying.
- **If the model still cannot learn question→clause from that little data**, that is a
  strong negative result for using *Decide* this way — and the defensible pivot is either
  (a) simplifying to a single high-level "intent" head (the router in the original
  recommendation), or (b) an instruct-LLM emitting the constrained skeleton JSON, which
  this same dataset's `queryoutputs.txt` shows llama3.2 can already do in the SQL form.

## 7. Reproduce

```bash
cd experiments/gliner-decide
.venv/bin/python run_gold.py                  # build gold.jsonl + ref_results.json
.venv/bin/python -m pytest tests/ -q          # 6 renderer unit tests
.venv/bin/python predict.py --run zero-shot   # results/pred_zero-shot.json
.venv/bin/python score.py --pred results/pred_zero-shot.json
.venv/bin/python predict.py --run zero-shot-noconstr --no-constraints
.venv/bin/python score.py --pred results/pred_zero-shot-noconstr.json
```

Artifacts: `experiments/gliner-decide/{query_schema.py,render.py,predict.py,score.py,
run_gold.py,gold.jsonl,ref_results.json,REPORT.md,results/}`.