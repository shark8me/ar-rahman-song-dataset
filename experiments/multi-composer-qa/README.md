# Multi-composer QA dataset

Extends `experiments/gliner-decide/gold.jsonl` (31 hand-written Rahman questions) to
**all six composers** in one dataset. Every entry carries a `composer` field:
`arr` (A. R. Rahman), `ila`, `pri`, `sel`, `sus`, `vsh`, or `all` (cross-composer).

## Files

- `gold-multi.jsonl` — 119 entries in the same skeleton format as gold.jsonl
  (`heads` → SQL skeleton, `values`, `ref_sql`, `answer_shape`), plus `composer`
  and a verified `answer`.
- `ref_results.json` — `idx` → answer, mirroring the gliner-decide layout.
- `generate_qa.py` — generator. The cheap LLM (z-ai/glm-5.3-flashx via OpenRouter)
  only *phrases* questions: it picks a template id and fills slots (year, movie,
  song, singer) from entity lists sampled from the databases. `ref_sql` is
  rendered deterministically from the template, run against the right DB, and the
  answer is stored only after verification. Empty/unanswerable results are dropped.

## Question families

- **Rahman originals**: all 31 questions from `experiments/gliner-decide/gold.jsonl`,
  re-indexed with `composer: "arr"`.
- **Per-composer** (8–14 per composer): counts, year filters, first/last album,
  top collaborators, best year — same shapes as the Rahman gold set.
- **Singer's perspective** (per composer): how many songs a singer sang for a
  composer, how many movies, which year first/most, list songs in a given year.
- **Cross-composer** (`composer: "all"`): `ref_sql` is a UNION ALL over all six
  composers' `songs_x`/`singers_x` tables (Rahman's live in
  `experiments/gliner-decide/arr.db`) — e.g. "How many music composers did the
  singer Arijit Singh sing for?", "How many songs did the singer Benny Dayal
  sing in 2019?", "Which composer did the singer X sing the most songs for?".

## Databases

- Root `arr.db` holds `songs_<pfx>` / `singers_<pfx>` for ila, pri, sel, sus, vsh
  (columns: `song`, `release_date`, `movie`, `singer`).
- Rahman's data lives in `experiments/gliner-decide/arr.db` as `songs` / `singers`
  (columns: `title`, `date`, `movie_name`, `name`).

## Regenerate

```sh
eval "$(grep '^export OPENROUTER_API_KEY' ~/.bashrc)"
python3 generate_qa.py
```

Every answer in gold-multi.jsonl was re-verified by re-running each `ref_sql`
against its database (0 mismatches at generation time).
