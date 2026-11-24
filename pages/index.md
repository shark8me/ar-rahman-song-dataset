---
layout: doc
title: "ARR-QA: mapping natural-language questions to SQL with GLiNER2.5-Decide"
description: "A text-to-SQL experiment series on an A. R. Rahman song dataset — zero-shot failure, LoRA fine-tuning from 25 to 1,703 questions, per-head accuracy analysis, and the routing problem that remains."
---

## Experiment series

A small music database answers most user questions with plain SQL. These
experiments test how far a 340M-parameter decision model
([GLiNER2.5-Decide](https://huggingface.co/fastino/GLiNER2.5-Decide)) gets
mapping English questions to SQL skeletons — the routing signal that decides
whether the app answers locally or escalates to a full LLM.

### Blog posts

- [Why we taught a 300M-parameter model to route SQL questions — and what each fine-tuning stage bought]({{ site.baseurl }}/blog/text-to-sql-finetuning/) — how we took a small classifier from 0% to 96.6% exact skeletons, and why the remaining 3.4% is a router problem, not a model problem.
- [Teaching a "decision" model to write SQL: fine-tuning GLiNER2.5-Decide on 25 questions]({{ site.baseurl }}/blog/25-questions/) — the first fine-tune: what it taught us about the model's internals and a scorer bug.

### Reports

- [Scaling GLiNER2.5-Decide from 25 to 1,703 training questions]({{ site.baseurl }}/report/filter-generalization/) — per-head accuracy at scale; non-filter heads reach 0.98–1.00, the filter decision head stays collapsed.
- [Zero-shot and 25-question LoRA experiment]({{ site.baseurl }}/report/zero-shot-lora/) — where the series started: zero-shot failure (0% exact skeletons) and the first LoRA fine-tune.

The dataset and code live in the [ar-rahman-song-dataset repo](https://github.com/shark8me/ar-rahman-song-dataset).
