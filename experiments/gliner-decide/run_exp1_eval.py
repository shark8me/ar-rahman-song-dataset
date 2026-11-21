#!/usr/bin/env python3
"""Exp1 eval: skeleton-head accuracy of the scaled LoRA on the leakage-safe test split.

Runs the classifier (zero-shot or LoRA adapter) over gold-multi-scaled.jsonl
test entries (per exp1_split.json) and reports:
  - per-head accuracy, exact-skeleton accuracy
  - filter decision (filter_col + filter_op) accuracy — the experiment's target
  - breakdown: held-out template vs seen templates; entity-in-train vs blocked
  - confusion on filter_col/filter_op

Usage:
  run_exp1_eval.py --adapter results/exp1/best/best --out results/exp1/pred_exp1_best.json
"""
import argparse, json, os, sqlite3, sys, time
from collections import Counter

from gliner2.classification import Classifier

from query_schema import build_schema, build_classifier

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("EXP1_DATA",
                      os.path.join(HERE, "..", "multi-composer-qa", "gold-multi-scaled.jsonl"))
SPLIT = os.environ.get("EXP1_SPLIT",
                       os.path.join(HERE, "..", "multi-composer-qa", "results", "exp1_split.json"))

HEAD_ORDER = ["select_cols", "filter_col", "filter_op", "agg", "group_by", "join", "order", "limit"]


def build_finetuned(adapter_dir):
    import torch
    from gliner2 import AutoExtractor
    from peft import PeftModel
    base = AutoExtractor.from_pretrained("fastino/GLiNER2.5-Decide")
    model = PeftModel.from_pretrained(base, adapter_dir)
    model.float()
    return Classifier(model).eval()


def norm(x):
    if isinstance(x, str):
        x = [x]
    return sorted(x or [])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--adapter", default=None)
    ap.add_argument("--no-constraints", action="store_true")
    ap.add_argument("--descriptions", action="store_true",
                    help="A2 ablation: label descriptions in the schema")
    ap.add_argument("--out", default=os.path.join(HERE, "results", "exp1", "pred_exp1.json"))
    a = ap.parse_args()

    gold = {g["idx"]: g for g in (json.loads(l) for l in open(DATA))}
    split = json.load(open(SPLIT))
    test = split["test"]
    train_set = set(split["train"])

    clf = build_finetuned(a.adapter) if a.adapter else build_classifier()
    schema = build_schema(use_constraints=not a.no_constraints,
                          use_descriptions=a.descriptions)

    outputs = {}
    t0 = time.time()
    for idx in test:
        g = gold[idx]
        result = clf.classify(g["query"], schema)
        heads = {t: list(result.tasks[t].labels) if t in result.tasks else []
                 for t in schema.task_order}
        outputs[str(idx)] = {"query": g["query"], "heads": heads,
                             "template_id": g.get("template_id")}
    outputs["_meta"] = {"n": len(test), "adapter": a.adapter,
                        "constraints": not a.no_constraints,
                        "descriptions": a.descriptions,
                        "total_time": round(time.time() - t0, 1)}
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w") as f:
        json.dump(outputs, f, indent=1)

    # ---- scoring ----
    per_head = {k: [0, 0] for k in HEAD_ORDER}
    skeleton = 0
    filt = [0, 0]
    by_tpl = {}
    conf_col, conf_op = Counter(), Counter()
    for idx in test:
        g = gold[idx]
        p = outputs[str(idx)]["heads"]
        all_ok = True
        for k in HEAD_ORDER:
            ok = norm(p.get(k)) == norm(g["heads"].get(k))
            per_head[k][0] += ok
            per_head[k][1] += 1
            all_ok &= ok
        skeleton += all_ok
        fok = (norm(p.get("filter_col")) == norm(g["heads"].get("filter_col"))
               and norm(p.get("filter_op")) == norm(g["heads"].get("filter_op")))
        filt[0] += fok
        filt[1] += 1
        if not fok:
            conf_col[(norm(g["heads"]["filter_col"])[0], norm(p["filter_col"])[0] if p.get("filter_col") else "?")] += 1
            conf_op[(norm(g["heads"]["filter_op"])[0], norm(p["filter_op"])[0] if p.get("filter_op") else "?")] += 1
        tid = g.get("template_id", "?")
        b = by_tpl.setdefault("held_out" if tid in split["held_out_templates"] else "seen", [0, 0, 0])
        b[0] += fok; b[1] += 1; b[2] += all_ok

    print("n_test:", len(test))
    print("\nper-head accuracy:")
    for k in HEAD_ORDER:
        c, t = per_head[k]
        print(f"  {k:12s} {c}/{t} = {c/t:.3f}")
    print(f"\nexact skeleton: {skeleton}/{len(test)} = {skeleton/len(test):.3f}")
    print(f"filter decision (col+op): {filt[0]}/{filt[1]} = {filt[0]/filt[1]:.3f}")
    print("\nfilter decision by split:")
    for key, (c, t, sk) in sorted(by_tpl.items()):
        print(f"  {key:9s} filter {c}/{t} = {c/t:.3f}   skeleton {sk}/{t} = {sk/t:.3f}")
    print("\nfilter_col confusion (gold -> pred):")
    for (gk, pk), n in conf_col.most_common(10):
        print(f"  {gk} -> {pk}: {n}")
    print("filter_op confusion (gold -> pred):")
    for (gk, pk), n in conf_op.most_common(10):
        print(f"  {gk} -> {pk}: {n}")
    print("\nmeta:", outputs["_meta"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
