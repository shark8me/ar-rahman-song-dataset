#!/usr/bin/env python3
"""Score predictions against gold. Metrics:
  - per-head accuracy
  - exact skeleton accuracy (all heads correct)
  - valid-SQL rate
  - execution accuracy (result-set match, order-insensitive)
  - value extraction presence
Usage: score.py --pred results/pred_zero-shot.json
"""
import argparse, json, os
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))

HEAD_ORDER = ["select_cols", "filter_col", "filter_op", "agg", "group_by", "join", "order", "limit"]


def norm(label_list):
    return sorted(label_list)


def exact_heads(pred_h, gold_h):
    for k in HEAD_ORDER:
        if norm(pred_h.get(k, [])) != norm(gold_h.get(k, [])):
            return False
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True)
    ap.add_argument("--label", default=None)
    a = ap.parse_args()
    pred = json.load(open(a.pred))
    ref = json.load(open(os.path.join(HERE, 'ref_results.json')))
    gold = {g['idx']: g for g in (json.loads(l) for l in open(os.path.join(HERE, 'gold.jsonl')))}

    meta = pred.pop("_meta", {})
    n = len(pred)
    head_acc = Counter()
    head_tot = Counter()
    skeleton_ok = 0
    valid_sql = 0
    exec_ok = 0
    head_fail = Counter()
    per_head_match = {k: 0 for k in HEAD_ORDER}
    value_unmatched = []

    per_query = []
    for idx, rec in pred.items():
        g = gold[int(idx)]
        gheads = g['heads']
        ph = rec['heads']
        # per-head
        for k in HEAD_ORDER:
            head_tot[k] += 1
            if norm(ph.get(k, [])) == norm(gheads.get(k, [])):
                head_acc[k] += 1
                per_head_match[k] += 1
            else:
                head_fail[k] += 1
        # exact skeleton
        if exact_heads(ph, gheads):
            skeleton_ok += 1
        # valid SQL
        if rec['error'] is None:
            valid_sql += 1
        # execution accuracy
        exp = ref[idx]['expected']
        got = rec['predicted']
        if rec['error'] is None and got is not None and exp is not None and got == exp:
            exec_ok += 1

    exec_sql = None

    print(f"=== Results ({a.label or os.path.basename(a.pred)}) ===  n={n}")
    print(f"constraints={meta.get('use_constraints')} fuzzy={meta.get('use_fuzzy')} total_time={meta.get('total_time')}s")
    print(f"\nPer-head accuracy:")
    for k in HEAD_ORDER:
        if head_tot[k]:
            print(f"  {k:14s} {head_acc[k]}/{head_tot[k]} = {100*head_acc[k]/head_tot[k]:5.1f}%")
    print(f"\nExact skeleton accuracy : {skeleton_ok}/{n} = {100*skeleton_ok/n:.1f}%")
    print(f"Valid-SQL rate          : {valid_sql}/{n} = {100*valid_sql/n:.1f}%")
    print(f"Execution accuracy      : {exec_ok}/{n} = {100*exec_ok/n:.1f}%")
    print(f"\nHead failure distribution:")
    for k, v in head_fail.most_common():
        print(f"  {k:14s} failed {v}")

    # per-query failures (identity-free: idx)
    print(f"\nExecution failures:")
    for idx, rec in sorted(pred.items(), key=lambda x: int(x[0])):
        exp = ref[idx]['expected']
        if rec['error'] is not None:
            print(f"  Q{idx}: SQL ERROR {rec['error']} | heads={rec['heads']} values={rec['values']}")
        elif rec['predicted'] != exp:
            print(f"  Q{idx}: exec mismatch | pred={str(rec['predicted'])[:60]} exp={str(exp)[:60]}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
