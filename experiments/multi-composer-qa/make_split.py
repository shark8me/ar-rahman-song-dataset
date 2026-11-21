#!/usr/bin/env python3
"""Plan Task 3: leakage-safe train/test split for gold-multi-scaled.jsonl.

Split unit = ENTITY GROUP, not entry: an entity string (movie/song/singer
name from the `values` map) may recur across entries and templates, so all
entries mentioning a given entity go to the same side of the split. This is
the only way the entity-blocklist assertion can hold.

Held-out template families (appear ONLY in test, never in train):
  song_year, singer_first_year_c

Entity blocklist: every entity string of length >= 4 occurring in any TEST
entry (values). Normalization / substring rule (documented choice):
  - compare case-insensitively, whitespace-trimmed;
  - a train entry leaks if a blocklist string of length >= 4 occurs as a
    SUBSTRING of the train entry's query text or of any of its value strings;
  - shorter blocklist strings (< 4 chars) are ignored as too generic for
    substring matching (exact-value collisions are still prevented by the
    group-based split).

Tradeoff (documented): with only 2076 entries, train >= 2000 and test >= 300
cannot both hold for a disjoint split (2000 + 300 > 2076). The plan
authorizes relaxing the least important constraint, so we relax train size
to >= 1500 and keep test >= 300, test like-filter share >= 50%, and all
leakage assertions ABSOLUTE.

Assertions run on every invocation and exit nonzero on violation.
"""

import json
import os
import sys
from collections import Counter

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "gold-multi-scaled.jsonl")
OUTDIR = os.path.join(BASE, "results")
OUT = os.path.join(OUTDIR, "exp1_split.json")

HELD_OUT_TEMPLATES = ["song_year", "singer_first_year_c"]
ENTITY_KEYS = ["movie", "song", "singer", "work_name"]  # 'year' is not an entity
MIN_TRAIN = 1500          # relaxed from 2000 (see module docstring)
MIN_TEST = 300
MIN_TEST_LIKE_SHARE = 0.5
TEST_BUFFER = 20          # aim slightly above MIN_TEST
SUBSTR_MIN_LEN = 4


def norm(s):
    return " ".join(str(s).split()).lower()


def load():
    rows = []
    with open(DATA) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def build_entity_groups(rows):
    """Union-find over entity strings; merge groups connected by substring
    containment (shorter string len >= SUBSTR_MIN_LEN, case-insensitive)."""
    ents = set()
    for r in rows:
        for k, v in r["values"].items():
            if k in ENTITY_KEYS:
                ents.add(norm(v))
    ents = sorted(ents)
    parent = {e: e for e in ents}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    by_len = sorted(ents, key=len)
    for i, a in enumerate(by_len):
        if len(a) < SUBSTR_MIN_LEN:
            continue
        for b in by_len[i + 1:]:
            if a in b:
                union(a, b)

    groups = {}
    for e in ents:
        groups.setdefault(find(e), set()).add(e)
    return list(groups.values())


def entry_entities(row, group_sets):
    """Entity group id(s) touched by an entry (via values AND query text)."""
    touched = set()
    vals = [norm(v) for k, v in row["values"].items() if k in ENTITY_KEYS]
    q = norm(row["query"])
    for gi, members in enumerate(group_sets):
        for m in members:
            if any(m == v for v in vals) or (len(m) >= SUBSTR_MIN_LEN and m in q):
                touched.add(gi)
    return touched


def main():
    rows = load()
    by_idx = {r["idx"]: r for r in rows}
    group_sets = build_entity_groups(rows)

    ent_groups = [set() for _ in group_sets]  # group -> entry idxs via values
    for r in rows:
        for k, v in r["values"].items():
            if k in ENTITY_KEYS:
                nv = norm(v)
                for gi, members in enumerate(group_sets):
                    if nv in members:
                        ent_groups[gi].add(r["idx"])
                        break

    def like(r):
        return r["heads"].get("filter_op") == "like"

    def blocklist_of(idxs):
        return sorted({norm(v) for i in idxs for k, v in by_idx[i]["values"].items()
                       if k in ENTITY_KEYS})

    def leak_closure(idxs):
        """Expand a test index set until no train entry's query/values contain
        a blocklist entity (substring, len >= SUBSTR_MIN_LEN) as leakage."""
        idxs = set(idxs)
        while True:
            bl = [b for b in blocklist_of(idxs) if len(b) >= SUBSTR_MIN_LEN]
            if not bl:
                return idxs, []
            extra = set()
            for r in rows:
                if r["idx"] in idxs:
                    continue
                hay = [norm(r["query"])] + [norm(v) for v in r["values"].values()]
                if any(b in h for b in bl for h in hay):
                    extra.add(r["idx"])
            if not extra:
                return idxs, []
            idxs |= extra

    # ---- Phase 1 (forced): minimal closure over held-out template entities ----
    held = [r for r in rows if r["template_id"] in HELD_OUT_TEMPLATES]
    forced_ents = {norm(v) for r in held for k, v in r["values"].items() if k in ENTITY_KEYS}
    seed = set()
    for r in rows:
        vs = [norm(v) for k, v in r["values"].items() if k in ENTITY_KEYS]
        if any(e in vs for e in forced_ents) or any(
                len(e) >= SUBSTR_MIN_LEN and e in norm(r["query"]) for e in forced_ents):
            seed.add(r["idx"])
    test_idx, _ = leak_closure(seed)
    test_groups = {gi for gi, idxs in enumerate(ent_groups) if idxs & test_idx}

    # ---- Phase 2: greedily add like-heavy entity groups to reach test targets ----
    candidates = sorted(
        (gi for gi in range(len(group_sets)) if ent_groups[gi] and gi not in test_groups),
        key=lambda gi: (-sum(1 for i in ent_groups[gi] if like(by_idx[i])), gi),
    )
    for gi in candidates:
        size = len(test_idx)
        n_like = sum(1 for i in test_idx if like(by_idx[i]))
        if size >= MIN_TEST + TEST_BUFFER and n_like / max(size, 1) >= MIN_TEST_LIKE_SHARE:
            break
        new = ent_groups[gi] - test_idx
        new_size, new_like = size + len(new), n_like + sum(1 for i in new if like(by_idx[i]))
        # keep like share >= 50% after the addition (drop non-like-heavy groups otherwise)
        if new_like / max(new_size, 1) < MIN_TEST_LIKE_SHARE:
            continue
        test_groups.add(gi)
        test_idx |= new
        test_idx, _ = leak_closure(test_idx)

    train_idx = sorted(r["idx"] for r in rows if r["idx"] not in test_idx)
    test_idx = sorted(test_idx)

    # ---- Blocklist: entity strings occurring in test values ----
    blocklist = sorted(
        {norm(v) for i in test_idx for k, v in by_idx[i]["values"].items() if k in ENTITY_KEYS}
    )
    bl_check = [b for b in blocklist if len(b) >= SUBSTR_MIN_LEN]

    def leaks(entry):
        hay = [norm(entry["query"])] + [norm(v) for v in entry["values"].values()]
        return [b for b in bl_check if any(b in h for h in hay)]

    leaking = [(i, leaks(by_idx[i])) for i in train_idx if leaks(by_idx[i])]

    # ---- Report ----
    t_like = sum(1 for i in test_idx if like(by_idx[i]))
    tr_like = sum(1 for i in train_idx if like(by_idx[i]))
    test_tpl = Counter(by_idx[i]["template_id"] for i in test_idx)
    train_tpl = Counter(by_idx[i]["template_id"] for i in train_idx)
    record = {
        "train": train_idx,
        "test": test_idx,
        "held_out_templates": HELD_OUT_TEMPLATES,
        "entity_blocklist": blocklist,
        "logic": {
            "split_unit": "entity group (union-find over entity strings merged on "
                          "case-insensitive substring containment, len>=4)",
            "entity_keys": ENTITY_KEYS,
            "forced_test": "minimal closure: all entries whose values/query contain an "
                           "entity used by a held_out_templates entry, plus iterated "
                           "leak-closure expansion",
            "greedy_fill": "remaining entity groups added by descending like-entry count "
                           "until test>=320 and like share>=50%, with leak-closure "
                           "expansion after each addition",
            "substring_rule": f"blocklist strings len>={SUBSTR_MIN_LEN} checked "
                              "case-insensitively as substrings of train query/values; "
                              "shorter strings skipped as too generic",
            "relaxed": f"train size relaxed from 2000 to {MIN_TRAIN}: 2076 entries "
                       f"cannot yield train>=2000 and test>={MIN_TEST} disjointly",
            "held_out_entries_by_tpl": {t: test_tpl[t] for t in HELD_OUT_TEMPLATES},
        },
        "counts": {
            "total": len(rows),
            "train": len(train_idx),
            "test": len(test_idx),
            "test_like": t_like,
            "test_like_share": round(t_like / len(test_idx), 4),
            "train_like": tr_like,
            "n_entity_groups": len(group_sets),
            "n_test_entity_groups": len(test_groups),
            "blocklist_size": len(blocklist),
            "blocklist_checked_substrings": len(bl_check),
            "train_leak_violations": len(leaking),
        },
    }

    # ---- ABSOLUTE assertions (exit nonzero on violation) ----
    failures = []
    if len(train_idx) < MIN_TRAIN:
        failures.append(f"train size {len(train_idx)} < {MIN_TRAIN}")
    if len(test_idx) < MIN_TEST:
        failures.append(f"test size {len(test_idx)} < {MIN_TEST}")
    share = t_like / max(len(test_idx), 1)
    if share < MIN_TEST_LIKE_SHARE:
        failures.append(f"test like share {share:.3f} < {MIN_TEST_LIKE_SHARE}")
    bad_tpl = [t for t in HELD_OUT_TEMPLATES if train_tpl.get(t)]
    if bad_tpl:
        failures.append(f"held-out templates present in train: {bad_tpl}")
    if leaking:
        failures.append(f"entity leakage in {len(leaking)} train entries, e.g. {leaking[:3]}")
    if set(train_idx) & set(test_idx):
        failures.append("train/test overlap")
    if len(train_idx) + len(test_idx) != len(rows):
        failures.append("split does not partition the corpus")

    os.makedirs(OUTDIR, exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(record, f, indent=2)
    print(json.dumps(record["counts"], indent=2))
    print("held-out template entries in test:", record["logic"]["held_out_entries_by_tpl"])

    if failures:
        print("ASSERTION FAILURES:", file=sys.stderr)
        for f_ in failures:
            print(" -", f_, file=sys.stderr)
        sys.exit(1)
    print("All assertions passed:", OUT)


if __name__ == "__main__":
    main()
