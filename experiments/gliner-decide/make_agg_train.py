#!/usr/bin/env python3
"""Build the agg-supervised TRAIN set: unbalanced train + `first` => min synth.

todo.md exp1 recommendation item 2: 34/60 EA failures on a2desc were one
template family — `singer_first_year_c` ("In which year did X first sing for
Y?"), which make_split.py deliberately HELD OUT of train (0 train / 41 test).
The agg head never saw any "first"-phrasing => agg=min supervision, so it
answers with the bare date list. Remedy: teach the family.

Steps (mirrors make_rebalanced_train.py, minus the rebalancing — item 4 calls
for the proven unbalanced recipe):
1. kept = ALL exp1 train entries (1,703), no caps.
2. synth: per composer, sample distinct singers from singers_tbl, render
   singer_first_year_c via the SAME B()/PHRASES builders as generate_qa.py
   (ref_sql stays verified), rotate the 6 phrasing surfaces, drop empty
   answers. No LLM.
3. Assertions: blocklist leakage (same norm/substring rule as make_split.py),
   query-dedup vs the WHOLE corpus + test, every ref_sql re-run with a
   4-digit-year scalar answer, unique idx.

Design changes (documented):
- `singer_first_year_c` moves from the held-out template set into train — the
  held-out-template experiment measured zero-shot generalization and found
  this exact gap; teaching it is the fix. The 41 test entries of this family
  stay in the untouched test split (they are what we score). `song_year`
  remains held out.
- v2 (after the aggfirst regression): synth heads carry the singer filter
  (`filter_col=singer/filter_op=like`) instead of the gold-head convention of
  leaving it in ref_sql only. The v1 run (gold-compatible heads) doubled the
  join+no-filter class (186→390 train) and the model learned "join => no
  filter", collapsing test-side filter_op 0.979→0.686 on join templates
  (34/39 song_singers_list + 58/58 song_singers_count flipped to none). With
  the singer filter in heads, synth entries form a NEW class (join+singer/like)
  with no conflict, and the EA renderer already resolves filter_col=singer via
  bind_singer_exact on the query text (same SQL as the singer_hint path).
  Known cost: test-side skeleton accuracy for the 41 first-year entries drops
  (gold heads still say filter_col=none) — EA is the decision metric.

Output: results/exp1/gold-train-agg.jsonl + results/exp1/exp1_split_agg.json
(fineteune.py --data/--split pair, test empty — eval uses exp1_split.json).
"""
import argparse, json, os, random, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "multi-composer-qa"))
import generate_qa as G  # noqa: E402  (COMPOSERS, B, PHRASES, fmt_answer, q, heads_for)

ROOT = G.ROOT
SCALED = os.path.join(ROOT, "experiments/multi-composer-qa/gold-multi-scaled.jsonl")
SPLIT = os.path.join(ROOT, "experiments/multi-composer-qa/results/exp1_split.json")
OUT = os.path.join(HERE, "results", "exp1", "gold-train-agg.jsonl")
SPLIT_OUT = os.path.join(HERE, "results", "exp1", "exp1_split_agg.json")

norm_q = lambda t: re.sub(r"[^a-z0-9 ]", "", t.lower()).strip()

AGG_TIDS = ["singer_first_year_c"]   # deliberately un-holds-out this family


def composer_singers(cname):
    """Distinct singers credited on dated songs for this composer."""
    c = G.COMPOSERS[cname]
    dc, sic = G.date_col(c), G.singer_col(c)
    rows = G.q(c["db"], f"SELECT DISTINCT s.{sic} FROM {G.singers_tbl(c)} s "
                        f"JOIN {G.songs_tbl(c)} s1 ON s1.song_id=s.song_id "
                        f"WHERE s.{sic} IS NOT NULL AND s1.{dc} IS NOT NULL "
                        f"AND LENGTH(s.{sic})>3")
    return [r[0] for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-composer", type=int, default=34,
                    help="synth entries per composer (~200 total vs 1703 train)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--split-out", default=SPLIT_OUT)
    a = ap.parse_args()

    rng = random.Random(a.seed)
    rows = [json.loads(l) for l in open(SCALED)]
    split = json.load(open(SPLIT))
    train, test = set(split["train"]), set(split["test"])
    blocklist = [s.lower() for s in split["entity_blocklist"] if len(s) >= 4]
    held_out = set(split["held_out_templates"])

    def blocklisted(text):
        low = " ".join(str(text).lower().split())
        return any(s in low for s in blocklist)

    kept = [r for r in rows if r["idx"] in train]
    seen = {norm_q(r["query"]) for r in rows}          # train AND test
    test_norm = {norm_q(r["query"]) for r in rows if r["idx"] in test}

    idx = max(r["idx"] for r in rows)
    synth = []
    for cname in ["ila", "pri", "sel", "sus", "vsh", "arr"]:
        singers = [s for s in composer_singers(cname)
                   if len(s) <= 60 and not blocklisted(s)]
        rng.shuffle(singers)
        phr_list = G.PHRASES["singer_first_year_c"]
        made = 0
        for i, singer in enumerate(singers):
            if made >= a.per_composer or i >= len(phr_list) * a.per_composer:
                break
            phr = phr_list[(i + rng.randrange(len(phr_list))) % len(phr_list)]
            query = phr.format(composer=G.COMPOSERS[cname]["name"], singer=singer)
            if norm_q(query) in seen or blocklisted(query):
                continue
            try:
                sql, shape = G.B("singer_first_year_c", cname, {"singer": singer})
            except ValueError:
                continue
            ans = G.fmt_answer(G.q(G.COMPOSERS[cname]["db"], sql), shape)
            if ans is None or not re.fullmatch(r"\d{4}", str(ans)):
                continue
            idx += 1
            heads = G.heads_for("singer_first_year_c", cname)
            # v2: singer filter lives in heads (see docstring design change)
            heads["filter_col"] = "singer"
            heads["filter_op"] = "like"
            synth.append({"idx": idx, "composer": cname, "query": query,
                          "template_id": "singer_first_year_c",
                          "heads": heads,
                          "values": {"singer": singer},
                          "ref_sql": sql, "answer_shape": shape, "answer": ans,
                          "augmented": True})
            seen.add(norm_q(query))
            made += 1
        print(f"{cname}: +{made} first-year synth (pool {len(singers)})")

    # ---- assertions ----
    for r in synth:
        assert r["template_id"] in AGG_TIDS, r["template_id"]
        assert not blocklisted(r["query"]), f"blocklist leak idx {r['idx']}"
        assert not blocklisted(r["values"]["singer"]), f"blocklist value idx {r['idx']}"
        assert norm_q(r["query"]) not in test_norm, f"duplicates test query idx {r['idx']}"
        assert re.fullmatch(r"\d{4}", str(r["answer"]))
        h = r["heads"]
        assert h["agg"] == "min" and h["filter_col"] == "singer" \
            and h["filter_op"] == "like" and h["join"] == ["songs_singers"], h
        # singer filter in heads must equal the ref_sql singer predicate
        assert r["values"]["singer"].lower() in r["query"].lower(), r["query"]
        # re-run ref_sql one more time (verification, not trust)
        rows2 = G.q(G.COMPOSERS[r["composer"]]["db"], r["ref_sql"])
        assert G.fmt_answer(rows2, r["answer_shape"]) == r["answer"]
    # explicit design-change note: this family is INTENTIONALLY in train now
    newly_taught = set(AGG_TIDS) & held_out
    ids = [r["idx"] for r in kept + synth]
    assert len(ids) == len(set(ids)), "duplicate idx"

    out_rows = kept + synth
    rng.shuffle(out_rows)
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w") as f:
        for e in out_rows:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    with open(a.split_out, "w") as f:
        json.dump({"train": ids, "test": [],
                   "derived_from": "exp1_split.json train (unbalanced) + "
                                   "first=>min agg synth (singer_first_year_c)",
                   "un_held_out_templates": sorted(newly_taught)}, f, indent=1)

    # stats
    n_first_train = sum(1 for r in out_rows if r["template_id"] == "singer_first_year_c")
    phr_dist = {}
    for r in synth:
        for p in G.PHRASES["singer_first_year_c"]:
            stem = p.split("{singer}")[0].strip().rstrip("?,")
            if r["query"].lower().startswith(stem[:24].lower()):
                phr_dist[stem] = phr_dist.get(stem, 0) + 1
                break
    stats = {"train_size": len(out_rows), "kept": len(kept), "synth": len(synth),
             "singer_first_year_c_in_train": n_first_train,
             "test_size": len(test), "phrasings": phr_dist}
    print(json.dumps(stats, indent=1))
    print("wrote", a.out, "and", a.split_out)


if __name__ == "__main__":
    main()
