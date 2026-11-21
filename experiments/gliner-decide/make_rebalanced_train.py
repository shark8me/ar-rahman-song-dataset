#!/usr/bin/env python3
"""Build a filter-head-focused rebalanced TRAIN set from the scaled corpus.

Steps (todo.md exp1 continuation):
1. Start from gold-multi-scaled.jsonl TRAIN indices only (test split is the
   untouched exp1 eval set).
2. Cap year-filter entries (filter_col=year) at --year-cap (~150) — the
   year/year_eq collapse dominated the train distribution (444 year_like).
3. Oversample title/movie_name like-filter entries by synthesizing new
   questions deterministically: pick a phrasing surface from the same PHRASES
   used by generate_qa.py, fill the slot with a verbatim entity sampled from
   the real DB, render ref_sql via the same B() builders, run it, and drop
   empty/zero results. No LLM, no unverified answers.
4. Contrastive slot-swap pairs: for entity strings that exist as BOTH a song
   title and a movie name in a composer's DB, emit a matched pair of questions
   with the SAME string in the song slot (filter_col=title) and the movie slot
   (filter_col=movie_name), so filter_col must be read from the slot, not the
   entity token.
5. Assert: no test-entity blocklist string in any emitted question, no
   held-out template used, every emitted entry re-verified against its DB.

Output: results/exp1/gold-train-rebalanced.jsonl (+ stats to stdout / JSON).
"""
import argparse, json, os, random, re, sqlite3, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "multi-composer-qa"))
import generate_qa as G  # noqa: E402  (COMPOSERS, B, PHRASES, entities, fmt_answer, q)

ROOT = G.ROOT
SCALED = os.path.join(ROOT, "experiments/multi-composer-qa/gold-multi-scaled.jsonl")
SPLIT = os.path.join(ROOT, "experiments/multi-composer-qa/results/exp1_split.json")
OUT = os.path.join(HERE, "results", "exp1", "gold-train-rebalanced.jsonl")

norm_q = lambda t: re.sub(r"[^a-z0-9 ]", "", t.lower()).strip()

# year-filter templates (filter_col=year in heads_for)
YEAR_TIDS = {"year_movies_count", "year_movies_list",
             "singer_songs_list_year", "singer_songs_count_year"}
TITLE_TIDS = ["song_singers_list", "song_singers_count"]
MOVIE_TIDS = ["movie_songs_list", "movie_songs_count", "movie_singers_list"]


def composer_songs_movies(cname):
    c = G.COMPOSERS[cname]
    sc, mc = G.song_col(c), G.movie_col(c)
    songs = [r[0] for r in G.q(c["db"], f"SELECT DISTINCT {sc} FROM {G.songs_tbl(c)} "
                               f"WHERE {sc} IS NOT NULL AND LENGTH({sc})>3")]
    movies = [r[0] for r in G.q(c["db"], f"SELECT DISTINCT {mc} FROM {G.songs_tbl(c)} "
                                 f"WHERE {mc} IS NOT NULL AND LENGTH({mc})>2")]
    return songs, movies


def make_entry(cname, tid, slots, phrase, idx):
    """Render + verify one synthetic entry; None if unanswerable."""
    try:
        sql, shape = G.B(tid, cname, slots)
    except ValueError:
        return None
    rows = G.q(G.COMPOSERS[cname]["db"], sql)
    ans = G.fmt_answer(rows, shape)
    if ans is None or (shape == "list" and not ans) or (shape == "count" and ans == 0):
        return None
    query = phrase.format(composer=G.COMPOSERS[cname]["name"], **slots)
    return {"idx": idx, "composer": cname, "query": query, "template_id": tid,
            "heads": G.heads_for(tid, cname),
            "values": {k: v for k, v in slots.items() if v},
            "ref_sql": sql, "answer_shape": shape, "answer": ans,
            "augmented": True}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-cap", type=int, default=150)
    ap.add_argument("--title-target", type=int, default=700,
                    help="desired total title_like train entries (kept+synth)")
    ap.add_argument("--movie-target", type=int, default=700)
    ap.add_argument("--pairs-per-composer", type=int, default=30)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=OUT)
    a = ap.parse_args()

    rng = random.Random(a.seed)
    rows = [json.loads(l) for l in open(SCALED)]
    split = json.load(open(SPLIT))
    train, test = set(split["train"]), set(split["test"])
    blocklist = [s.lower() for s in split["entity_blocklist"]]
    held_out = set(split["held_out_templates"])

    def fh(r):
        fc = r["heads"]["filter_col"]
        fo = r["heads"]["filter_op"]
        if isinstance(fc, list): fc = fc[0] if fc else "none"
        if isinstance(fo, list): fo = fo[0] if fo else "none"
        return fc, fo

    kept = [r for r in rows if r["idx"] in train]
    year_rows = [r for r in kept if fh(r)[0] == "year"]
    other_rows = [r for r in kept if fh(r)[0] != "year"]
    rng.shuffle(year_rows)
    year_rows = year_rows[:a.year_cap]
    kept = other_rows + year_rows

    # every existing question text, so synth never collides with train OR test
    seen = {norm_q(r["query"]) for r in rows}
    title_now = sum(1 for r in kept if fh(r) == ("title", "like"))
    movie_now = sum(1 for r in kept if fh(r) == ("movie_name", "like"))
    print(f"after cap: {len(kept)} train (title_like {title_now}, movie_like {movie_now}, "
          f"year-filter {len(year_rows)})")

    # same substring rule as make_split.py: blocklist strings len>=4 only
    bl_check = [s for s in blocklist if len(s) >= 4]

    def blocklisted(text):
        low = " ".join(text.lower().split())
        return any(s in low for s in bl_check)

    idx = max(r["idx"] for r in rows)
    synth = []
    pairs = []
    for cname in ["ila", "pri", "sel", "sus", "vsh", "arr"]:
        ents = G.entities(cname)  # reuses generate_qa sampling (shuffles, caps at 60)
        # widen the song pool: synth needs many distinct entities
        songs, movies = composer_songs_movies(cname)
        rng.shuffle(songs); rng.shuffle(movies)
        songs = [s for s in songs if len(s) <= 80]
        movies = [m for m in movies if len(m) <= 80]
        # --- oversample title_like ---
        need = max(0, a.title_target - title_now)
        per = need // len(G.COMPOSERS) + 1
        made = title_made = 0
        for i in range(per * 3):
            if made >= per:
                break
            s = songs[i % len(songs)]
            tid = TITLE_TIDS[i % len(TITLE_TIDS)]
            phr = G.PHRASES[tid][rng.randrange(len(G.PHRASES[tid]))]
            e = make_entry(cname, tid, {"song": s}, phr, idx + 1)
            if not e or norm_q(e["query"]) in seen or blocklisted(e["query"]):
                continue
            idx += 1; e["idx"] = idx; seen.add(norm_q(e["query"]))
            synth.append(e); made += 1; title_made += 1
        title_now += made
        # --- top up movie_like ---
        need = max(0, a.movie_target - movie_now)
        per = need // len(G.COMPOSERS) + 1
        made = movie_made = 0
        for i in range(per * 3):
            if made >= per:
                break
            m = movies[i % len(movies)]
            tid = MOVIE_TIDS[i % len(MOVIE_TIDS)]
            phr = G.PHRASES[tid][rng.randrange(len(G.PHRASES[tid]))]
            e = make_entry(cname, tid, {"movie": m}, phr, idx + 1)
            if not e or norm_q(e["query"]) in seen or blocklisted(e["query"]):
                continue
            idx += 1; e["idx"] = idx; seen.add(norm_q(e["query"]))
            synth.append(e); made += 1; movie_made += 1
        movie_now += made
        # --- contrastive slot-swap pairs: same string as song AND movie ---
        smap = {s.lower(): s for s in songs}
        mmap = {m.lower(): m for m in movies}
        both = [x for x in smap if x in mmap and len(x) > 3 and not blocklisted(x)]
        rng.shuffle(both)
        n_pair = 0
        for x in both[:a.pairs_per_composer]:
            song_slot = smap[x]
            movie_slot = mmap[x]
            q_movie = G.PHRASES["movie_songs_list"][0].format(
                composer=G.COMPOSERS[cname]["name"], movie=movie_slot)
            q_song = G.PHRASES["song_singers_list"][0].format(
                composer=G.COMPOSERS[cname]["name"], song=song_slot)
            em = make_entry(cname, "movie_songs_list", {"movie": movie_slot},
                            G.PHRASES["movie_songs_list"][0], idx + 1)
            es = make_entry(cname, "song_singers_list", {"song": song_slot},
                            G.PHRASES["song_singers_list"][0], idx + 2)
            if not em or not es:
                continue
            if norm_q(em["query"]) in seen or norm_q(es["query"]) in seen:
                continue
            if blocklisted(em["query"]) or blocklisted(es["query"]):
                continue
            idx += 2; em["idx"] = idx - 1; es["idx"] = idx
            em["pair_id"] = es["pair_id"] = f"swap-{cname}-{n_pair}"
            seen.add(norm_q(em["query"])); seen.add(norm_q(es["query"]))
            synth += [em, es]
            pairs.append((em["idx"], es["idx"]))
            n_pair += 1
        print(f"{cname}: +{title_made} title synth, +{movie_made} movie synth, "
              f"{n_pair} slot-swap pairs")

    test_norm = {norm_q(x["query"]) for x in rows if x["idx"] in test}
    out_rows = kept + synth
    rng.shuffle(out_rows)
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w") as f:
        for e in out_rows:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

    # ---- assertions ----
    dist = {}
    for r in out_rows:
        dist[fh(r)] = dist.get(fh(r), 0) + 1
    for r in out_rows:
        assert not blocklisted(r["query"]), f"blocklisted entity in {r['idx']}: {r['query']}"
        assert r["template_id"] not in held_out, f"held-out template {r['template_id']} in train"
        assert norm_q(r["query"]) not in test_norm, \
            f"train query duplicates a test query: {r['query']}"
    ids = [r["idx"] for r in out_rows]
    assert len(ids) == len(set(ids)), "duplicate idx"
    stats = {"train_size": len(out_rows), "filter_dist": {f"{k[0]}|{k[1]}": v for k, v in dist.items()},
             "year_capped_from": len([r for r in rows if r["idx"] in train and fh(r)[0] == "year"]),
             "year_kept": len(year_rows), "synth": len(synth), "swap_pairs": len(pairs)}
    # matching split file for finetune.py: train = the rebalanced ids; test
    # left empty (the real eval set is the untouched exp1 test split, scored
    # by run_exp1_eval.py after training)
    with open(os.path.join(HERE, "results", "exp1", "exp1_split_rebalanced.json"), "w") as f:
        json.dump({"train": ids, "test": [],
                   "derived_from": "exp1_split.json train + synthetic augmentation",
                   "year_cap": a.year_cap}, f, indent=1)
    with open(os.path.join(HERE, "results", "exp1", "rebalance_stats.json"), "w") as f:
        json.dump(stats, f, indent=1)
    print(json.dumps(stats, indent=1))
    print("wrote", a.out)


if __name__ == "__main__":
    main()
