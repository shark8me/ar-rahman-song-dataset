#!/usr/bin/env python3
"""Check the constraint set in query_schema.py against the scaled gold corpus.

For every implication (A=a -> B), count gold entries where A=a holds but B does
not. B is either a single label or a list of labels (any-of semantics, matching
C.any_of in query_schema.py). A violated implication (>0) would be *forced
wrong* at inference time, so the constraint set must be corrected until all
violations are 0. Implications with hold==0 are vacuous — gold cannot validate
them, so they are reported and should be dropped.

Gold heads mix types: select_cols/join are lists, filter_col/filter_op/
group_by/order/agg/limit are bare strings ("none" = empty). Everything is
normalized to a label list here.

Usage:
  python check_constraints.py [--data ../multi-composer-qa/gold-multi-scaled.jsonl]
                              [--split ../multi-composer-qa/results/exp1_split.json]

Exit 1 if any implication is violated.
"""
import argparse
import json
import sys

# Same implication set as query_schema.py build_schema(use_constraints=True).
# Consequent label list = any-of.
IMPLICATIONS = [
    (("order", "count_desc"), ("agg", ["count"])),
    (("group_by", "year"), ("agg", ["count"])),
    (("filter_op", "like"), ("filter_col", ["title", "movie_name", "year"])),
    (("filter_op", "year_eq"), ("filter_col", ["year"])),
    (("filter_col", "year"), ("filter_op", ["like", "year_eq"])),
]


def labs(v):
    """Normalize a gold head value to a list of labels."""
    if v is None:
        return []
    if isinstance(v, str):
        return [] if v == "none" else [v]
    return list(v)


def load_entries(data_path, split_path):
    entries = [json.loads(l) for l in open(data_path) if l.strip()]
    if split_path:
        split = json.load(open(split_path))
        keep = set(split.get("train", [])) | set(split.get("test", []))
        if keep:
            entries = [e for e in entries if e.get("idx") in keep]
    return entries


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="../multi-composer-qa/gold-multi-scaled.jsonl")
    ap.add_argument("--split", default="../multi-composer-qa/results/exp1_split.json")
    args = ap.parse_args()

    entries = load_entries(args.data, args.split)
    print(f"checked {len(entries)} entries against {len(IMPLICATIONS)} implications\n")

    violations = vacuous = 0
    for (atask, alabel), (btask, blabels) in IMPLICATIONS:
        n_violate = n_hold = 0
        for e in entries:
            heads = e.get("heads", {})
            if alabel in labs(heads.get(atask)):
                n_hold += 1
                if not any(b in labs(heads.get(btask)) for b in blabels):
                    n_violate += 1
        if n_violate:
            violations += 1
            flag = "VIOLATED"
        elif n_hold == 0:
            vacuous += 1
            flag = "VACUOUS (hold=0)"
        else:
            flag = "ok"
        cons = "|".join(blabels)
        print(f"{atask}={alabel} -> {btask} in [{cons}]: "
              f"hold {n_hold:5d}  violate {n_violate:5d}  [{flag}]")

    if violations:
        print(f"\n{violations} implication(s) violated by gold — fix query_schema.py C.implies")
        sys.exit(1)
    if vacuous:
        print(f"\nall implications consistent with gold, but {vacuous} vacuous "
              f"(hold=0, cannot be validated by gold) — consider dropping")
        sys.exit(1)
    print("\nall implications consistent with gold")


if __name__ == "__main__":
    main()
