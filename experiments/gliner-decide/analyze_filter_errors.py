#!/usr/bin/env python
"""Analyze missed filter_col/filter_op decisions of the finetuned LoRA model.

For each test-set query where the model's filter_col or filter_op disagrees
with gold, print: query text, gold label, predicted label, and whether the
query's filter value string (from the entry's `values` map, or extracted from
the query) appears anywhere in the training split.

Usage: .venv/bin/python analyze_filter_errors.py   (from experiments/gliner-decide/)
Writes results/exp1/filter_error_baseline.txt
"""
import json
import os
import re
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
GOLD = os.path.join(BASE, "gold.jsonl")
PRED = os.path.join(BASE, "results", "pred_finetuned-lora-best.json")
SPLIT = os.path.join(BASE, "results", "finetune_split.json")
OUTDIR = os.path.join(BASE, "results", "exp1")
OUT = os.path.join(OUTDIR, "filter_error_baseline.txt")


def first(x):
    """Predictions store single-element lists; unwrap."""
    if isinstance(x, list):
        return x[0] if x else None
    return x


def filter_value_str(entry):
    """Filter value string: from the entry's `values` map, else extract from query."""
    vals = entry.get("values") or {}
    if vals:
        # 'none' pseudo-values are not real strings
        real = [v for v in vals.values() if isinstance(v, str) and v != "none"]
        if real:
            return " | ".join(real)
    q = entry.get("query", "")
    m = re.search(r"(?:called|titled|named)\s+['\"]?([^'\"?,.]+)", q)
    if m:
        return m.group(1).strip()
    return None


def norm(s):
    return (s or "").strip().lower()


def main():
    gold = {}
    with open(GOLD) as f:
        for line in f:
            line = line.strip()
            if line:
                g = json.loads(line)
                gold[g["idx"]] = g

    with open(PRED) as f:
        preds = json.load(f)

    split = json.load(open(SPLIT))
    train_idx = split["train"]

    # train split corpus: concatenated values + query text, for substring checks
    train_texts = []
    for i in train_idx:
        g = gold.get(i)
        if not g:
            continue
        parts = [g.get("query", "")]
        for v in (g.get("values") or {}).values():
            if isinstance(v, str):
                parts.append(v)
        train_texts.append(norm(" ".join(parts)))
    train_blob = "\n".join(train_texts)

    lines = []
    misses = []
    for key in sorted((k for k in preds if k.isdigit()), key=int):
        idx = int(key)
        p = preds[key]
        g = gold.get(idx)
        if g is None:
            continue
        ph, gh = p.get("heads", {}), g.get("heads", {})
        p_col, p_op = first(ph.get("filter_col")), first(ph.get("filter_op"))
        g_col, g_op = gh.get("filter_col"), gh.get("filter_op")
        if p_col == g_col and p_op == g_op:
            continue
        fval = filter_value_str(g)
        in_train = None
        if fval:
            # value string appears anywhere in the train split (values or query text)
            in_train = norm(fval) in train_blob
        misses.append({
            "idx": idx,
            "query": g["query"],
            "gold": f"filter_col={g_col} filter_op={g_op}",
            "pred": f"filter_col={p_col} filter_op={p_op}",
            "filter_value": fval,
            "in_train": in_train,
            "test": idx in split["test"],
        })

    lines.append(f"Filter decision error analysis — pred_finetuned-lora-best.json")
    lines.append(f"Gold entries: {len(gold)} | train idx: {len(train_idx)} | test idx: {len(split['test'])}")
    lines.append(f"Missed filter_col/filter_op decisions: {len(misses)}")
    lines.append("")
    for m in misses:
        lines.append(f"[{m['idx']}] test={m['test']}")
        lines.append(f"  query : {m['query']}")
        lines.append(f"  gold  : {m['gold']}")
        lines.append(f"  pred  : {m['pred']}")
        lines.append(f"  filter value: {m['filter_value']!r}  in_train={m['in_train']}")
        lines.append("")

    n_in_train = sum(1 for m in misses if m["in_train"] is True)
    n_absent = sum(1 for m in misses if m["in_train"] is False)
    n_nofv = sum(1 for m in misses if m["in_train"] is None)
    lines.append(f"Summary: {len(misses)} misses | filter value present in train: {n_in_train} | "
                 f"absent from train: {n_absent} | no filter value extractable: {n_nofv}")

    os.makedirs(OUTDIR, exist_ok=True)
    with open(OUT, "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\nSaved to {OUT}", file=sys.stderr)


if __name__ == "__main__":
    main()
