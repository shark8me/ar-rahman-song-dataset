#!/usr/bin/env python3
"""Verify a gold-multi jsonl: re-run every ref_sql against its DB and compare
to the stored answer. Exit 1 on any mismatch."""
import json, os, sqlite3, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ARR_DB = os.path.join(ROOT, "arr.db")
RAHMAN_DB = os.path.join(ROOT, "experiments/gliner-decide/arr.db")

def db_for(composer):
    return RAHMAN_DB if composer == "arr" else ARR_DB

def norm(v):
    if isinstance(v, list):
        return sorted(str(x) for x in v)
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True, ensure_ascii=False)
    return str(v)

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "experiments/multi-composer-qa/gold-multi-scaled.jsonl")
    con_cache = {}
    mismatches = 0
    errors = 0
    checked = 0
    for line in open(path):
        e = json.loads(line)
        if "ref_sql" not in e or "answer" not in e:
            continue
        db = db_for(e.get("composer", ""))
        con = con_cache.setdefault(db, sqlite3.connect(db))
        try:
            rows = con.execute(e["ref_sql"]).fetchall()
        except Exception as ex:
            errors += 1
            print(f"idx {e['idx']}: SQL error: {ex}\n  {e['ref_sql']}")
            continue
        checked += 1
        shape = e.get("answer_shape")
        if shape in ("count", "scalar", "name"):
            got = rows[0][0] if rows else None
        elif shape == "list":
            got = sorted({r[0] for r in rows if r[0] is not None})
        elif shape == "pairs":
            got = [{"composer": r[0], "songs": r[1]} for r in rows]
        else:
            got = rows
        if norm(got) != norm(e.get("answer")):
            mismatches += 1
            print(f"idx {e['idx']} [{e.get('composer')}/{e.get('template_id')}]: mismatch")
            print(f"  expected: {str(e.get('answer'))[:200]}")
            print(f"  got:      {str(got)[:200]}")
            print(f"  sql: {e['ref_sql'][:200]}")
    for con in con_cache.values():
        con.close()
    print(f"checked {checked} entries: {mismatches} mismatches, {errors} SQL errors")
    sys.exit(1 if (mismatches or errors) else 0)

if __name__ == "__main__":
    main()
