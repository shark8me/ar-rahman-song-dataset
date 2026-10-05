#!/usr/bin/env python3
"""Prediction pipeline: classify skeleton -> extract values -> render -> execute.

Usage:
  predict.py --run zero-shot [--no-constraints] [--no-fuzzy] [--out results/x.json]
Loads query_schema + Classifier, iterates gold.jsonl, executes against arr.db.
"""
import argparse, json, sqlite3, os, time, sys
from gliner2.classification import Classifier

from query_schema import build_schema, build_classifier
from render import render, extract_values, _FuzzyDict, DB

HERE = os.path.dirname(os.path.abspath(__file__))


def pred_labels(result, task):
    tr = result.tasks.get(task)
    if tr is None:
        return []
    return list(tr.labels)


def build_finetuned_classifier(adapter_dir, map_location="cpu"):
    """Load base model + PEFT LoRA adapter saved by finetune.py."""
    import torch
    from gliner2 import AutoExtractor
    from peft import PeftModel
    from gliner2.classification import Classifier
    base = AutoExtractor.from_pretrained("fastino/GLiNER2.5-Decide")
    model = PeftModel.from_pretrained(base, adapter_dir)
    model.float()
    return Classifier(model).eval()


def run(use_constraints=True, use_fuzzy=True, adapter=None):
    gold = [json.loads(l) for l in open(os.path.join(HERE, 'gold.jsonl'))]
    if adapter:
        clf = build_finetuned_classifier(adapter)
    else:
        clf = build_classifier()
    schema = build_schema(use_constraints=use_constraints)
    conn = sqlite3.connect(DB)
    fdict = _FuzzyDict(conn)

    outputs = {}
    t0 = time.time()
    for g in gold:
        idx = g['idx']
        q = g['query']
        tc = time.time()
        result = clf.classify(q, schema)
        order = schema.task_order
        heads = {}
        for task in order:
            heads[task] = pred_labels(result, task)
        t_cls = time.time() - tc

        values = extract_values(q)
        sql = render(heads, values, fdict)
        # execute
        out, err = None, None
        try:
            rows = conn.execute(sql).fetchall()
            out = sorted(str(r[0]) if len(r) == 1 else tuple(str(x) for x in r) for r in rows)
        except Exception as e:
            err = f"{type(e).__name__}: {e}"

        outputs[str(idx)] = {
            "query": q,
            "heads": heads,
            "values": values,
            "sql": sql,
            "predicted": out,
            "error": err,
            "feasible": result.feasible,
            "t_classify": round(t_cls, 3),
            "violations": [str(v) for v in result.violations],
        }
    outputs["_meta"] = {
        "use_constraints": use_constraints, "use_fuzzy": use_fuzzy,
        "total_time": round(time.time() - t0, 2),
        "gold_count": len(gold),
        "adapter": adapter,
    }
    conn.close()
    return outputs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default="zero-shot")
    ap.add_argument("--no-constraints", action="store_true")
    ap.add_argument("--no-fuzzy", action="store_true")
    ap.add_argument("--out", default=None)
    ap.add_argument("--adapter", default=None, help="LoRA adapter dir from finetune.py")
    a = ap.parse_args()
    res = run(use_constraints=not a.no_constraints, use_fuzzy=not a.no_fuzzy,
              adapter=a.adapter)
    out_path = a.out or os.path.join(HERE, "results", f"pred_{a.run}.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(res, f, indent=1)
    print(f"Wrote {out_path}")
    print(json.dumps(res["_meta"]))
    return 0


if __name__ == '__main__':
    sys.exit(main())
