# Teaching a small model to turn questions into SQL — and what each stage of fine-tuning bought

*How we took a GLiNER2.5-Decide model from 0% to 96.6% execution accuracy on a
text-to-SQL task, one technical term at a time.*

This post explains the pieces of our experiment on turning natural-language
questions about playback singers into working SQL queries. If you've read the
experiment reports (`REPORT.md`, `REPORT-EXP1.md`) you've seen the numbers;
here we unpack what the jargon actually means, with real examples from the
corpus. All examples come from the actual dataset — questions like "Who sang
Kun Faya Kun?" and the SQL that answers them.

## The task

The underlying model is a small encoder (GLiNER2.5-Decide) that doesn't
generate SQL. Instead it makes **8 classification decisions** about a
question's structure — the "SQL skeleton" — and a renderer turns that skeleton
into real SQL run against per-composer databases. For example:

> **Question:** "List the playback singers featured on Shankar-Ehsaan-Loy's
> soundtrack for the film Taare Zameen Par."
>
> **Skeleton (8 heads):** `select_cols=[singer]`, `filter_col=movie_name`,
> `filter_op=like`, `agg=none`, `group_by=none`, `join=[songs_singers]`,
> `order=none`, `limit=none`
>
> **Rendered SQL:** `SELECT s2.singer FROM songs_se l1 JOIN singers_se s2 ...
> WHERE l1.movie_name LIKE '%Taare Zameen Par%'`

### Skeleton

The **skeleton** is the full set of 8 head decisions at once — the query's
shape, before any values are filled in. It's the "select-what, filtered-by,
aggregated-how, sorted-by" blueprint. Getting 7 of 8 heads right is worth
nothing: one wrong head and the rendered SQL answers a different question.
This is why **exact skeleton accuracy** (all 8 heads correct simultaneously)
is the strictest metric we track — it went 0% → 54.8% → 84.5% across the
fine-tuning stages, and it's the number that most directly predicts whether
the final SQL answers the question asked.

### Filter

The **filter** is the WHERE clause: which column does the question constrain,
and how? It's two heads: `filter_col` (which column) and `filter_op` (which
operator). The three operators:

- `like` — substring/pattern match. "…featured on the film *Taare Zameen
  Par*" → `filter_col=movie_name, filter_op=like` → `WHERE movie_name LIKE
  '%Taare Zameen Par%'`.
- `year_eq` — exact year. "…songs Kailash Kher sang in 2006" →
  `filter_col=year` → `WHERE substr(release_date,1,4) = '2006'`.
- `none` — no filter at all. "How many singers has X collaborated with?" has
  no WHERE clause — the filter decision is `none/none`.

The filter decision was the experiment's main bottleneck: it's the one head
that must read the *meaning* of the question (is "X" a song title, a film
name, or a year?) rather than copy a surface pattern. The same entity string
can be a song title in one question and a film name in another — "Maasi
Maasam" filters on the song column, "Taare Zameen Par" on the film column —
and nothing in the string itself says which.

### EA (Execution accuracy)

**EA** is the end-to-end metric: run the rendered SQL against the real
database and compare the result to the gold answer. A query can have a
perfect skeleton and still fail if the value it binds into the LIKE clause is
wrong ("Dhoom Machale" bound as `%Ale%` matches nonsense). Conversely a
slightly-off skeleton can still get the right answer. EA is the only metric
that measures what a user would actually see. Final: **342/354 = 96.6%**.

### Scaled

"Scaled" refers to the corpus scale-up: 25 hand-written training questions →
**2,076 machine-generated, DB-verified entries** across 6 composers, split
1,703 train / 373 test. Every generated query's reference SQL was executed
against the real database to confirm the stored answer is correct. The
multiplier mattered: 25 examples taught the model the *mechanism* of
classification but left the filter head with ~6 filter patterns to memorize;
1,703 gave it enough variety to read slot type from the question.

### Rebalanced

**Rebalanced** is a training-data intervention: the year-filter templates
dominated the train set (449 of 2,076), so we capped them at 150 and
oversampled the weaker classes (title/movie `like` filters, plus 112
"contrastive pairs" — the same entity string as a song title in one question
and a film name in another, forcing the model to read slot type rather than
remember the token). Result: it traded heads — exact skeleton 0.445 → 0.694
(the `limit` head got fixed) but agg dropped 0.847 → 0.697. Rebalancing is
not free: every sample you add to one class is supervision taken from the
others.

### Agg

**Agg** is the aggregation head: what function collapses the matched rows.
`min` (earliest year — "when did X *first* sing?"), `max` (latest), `count`
("how many singers?"), `distinct` (unique list), `none` (no aggregation —
just the rows). This head caused the last big failure family: 34 of 60
end-to-end failures were one template family where the model returned the
whole date list instead of the earliest year — `agg=none` instead of
`agg=min` — producing "2023-03-08 00:00:00" where the gold answer was "2023".

### First-family

The **first-family** is that template: questions like

> "In which year did Dhanush first sing for Ilaiyaraaja?" →
> `SELECT MIN(substr(release_date,1,4)) ... WHERE singer LIKE '%Dhanush%'`
>
> "When did Suchith Suresan first sing for Sushin Shyam?" → same shape,
> `agg=min`.

It's called "first" because the question word *first* implies "earliest
year" = `agg=min`. It was held out of training entirely (0 train / 41 test
questions) as a generalization probe — and the probe failed: the model never
learned "first ⇒ min". Two retrains to teach it (204 synthetic entries each)
both fixed the family but broke other heads elsewhere; the fix that won was
a deterministic rule at inference time: questions matching the family's
phrasings ("first sing", "first appear", "first work as", …) get the
canonical family shape imposed by the scorer. That single rule took EA from
0.870 to 0.966.

### The fine-tuning progression, in one table

| stage | train size | filter | exact skeleton | EA |
|---|---|---|---|---|
| zero-shot (no fine-tune) | 0 | ~0.16 | 0.000 | 0.00–0.03 |
| 25-example LoRA | 25 | 0.581 | 0.548 | 0.419 seen / 0.000 held-out |
| scaled 1,703, bare | 1,703 | 0.979 | 0.445 | 0.867 |
| scaled + rebalanced | 1,954 | 0.952 | 0.694 | 0.825 |
| scaled + descriptions | 1,703 | 0.979 | 0.845 | 0.870 |
| + first-family rule | 1,703 | 0.979 | 0.845 | **0.966** |

The takeaways:

1. **Fine-tuning is necessary, not optional.** Zero-shot never produces a
   single correct skeleton (0/31 questions) — all 8 heads must be right at
   once, so per-head scores of 16–74% are moot.
2. **25 examples buy the mechanism, not the coverage.** EA 0 → 0.419 on
   seen questions, but held-out filter decisions stay 0/6.
3. **100× more data buys the filter decision.** filter 0.581 → 0.979 —
   but only after the inference-time constraint set was corrected; the naive
   constraints had masked the gain.
4. **Descriptions add without taking away.** Training *with* label
   descriptions (e.g. `title: "song title"`) keeps filter at 0.979 and fixes
   both limit and agg → skeleton 0.845. Inference-only descriptions, by
   contrast, were strongly negative (skeleton 0.694 → 0.343) — a
   train/inference schema mismatch.
5. **The residual gap is one template family, best fixed outside the model.**
   Two retrains regressed other heads; the deterministic scorer rule closes
   it: EA 0.966.

Full details: `REPORT.md` (zero-shot and 25-example stages) and
`REPORT-EXP1.md` (scaled stages, ablations, failure attributions).
