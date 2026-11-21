#!/usr/bin/env python3
"""Rewrite heads labels in gold-multi-scaled.jsonl to the canonical schema.

The scaled corpus was generated before heads_for() was canonicalized: pfx
composers emitted DB column names (movie/song/release_date) where the frozen
query_schema label lists expect movie_name/title/date. ref_sql, answers and
queries are untouched — only heads.select_cols labels are mapped.
"""
import json, sys

CANON = {"song": "title", "movie": "movie_name", "release_date": "date"}
SRC = "gold-multi-scaled.jsonl"

def main():
    out = []
    n_sel = 0
    for line in open(SRC):
        e = json.loads(line)
        sel = e["heads"].get("select_cols") or []
        new = [CANON.get(c, c) for c in sel]
        if new != sel:
            n_sel += 1
            e["heads"]["select_cols"] = new
        out.append(e)
    with open(SRC, "w") as f:
        for e in out:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"rewrote {n_sel} entries with canonical select_cols "
          f"(of {len(out)} total) -> {SRC}")

if __name__ == "__main__":
    sys.exit(main())
