#!/usr/bin/env python3
"""Generate a multi-composer QA dataset in the gold.jsonl skeleton format.

The cheap LLM (OpenRouter) only PHRASES questions: it picks a template id and
fills slots (composer/year/movie/song/singer) from entity lists sampled from
the databases. ref_sql is rendered deterministically from the template, run
against the right DB, and the verified answer is stored alongside.

Output: gold-multi.jsonl (one JSON per line, `composer` field added) and
ref_results.json (idx -> answer), mirroring the gliner-decide layout.
"""
import argparse, json, os, random, re, sqlite3, sys, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root
ARR_DB = os.path.join(ROOT, "arr.db")                       # ila pri sel sus vsh
RAHMAN_DB = os.path.join(ROOT, "experiments/gliner-decide/arr.db")  # arr
OUT_DIR = os.path.join(ROOT, "experiments/multi-composer-qa")

COMPOSERS = {
    "arr": {"name": "A. R. Rahman", "prefix": "arr", "db": RAHMAN_DB, "kind": "arr"},
    "ila": {"name": "Ilaiyaraaja", "prefix": "ila", "db": ARR_DB, "kind": "pfx"},
    "pri": {"name": "Pritam", "prefix": "pri", "db": ARR_DB, "kind": "pfx"},
    "sel": {"name": "Shankar-Ehsaan-Loy", "prefix": "sel", "db": ARR_DB, "kind": "pfx"},
    "sus": {"name": "Sushin Shyam", "prefix": "sus", "db": ARR_DB, "kind": "pfx"},
    "vsh": {"name": "Vishal-Shekhar", "prefix": "vsh", "db": ARR_DB, "kind": "pfx"},
}

rng = random.Random(42)

def q(db, sql, args=()):
    con = sqlite3.connect(db)
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()

# ---------- schema helpers: arr uses title/date/movie_name/name; pfx use song/release_date/movie/singer
def song_col(c):   return "title" if c["kind"] == "arr" else "song"
def date_col(c):   return "date" if c["kind"] == "arr" else "release_date"
def movie_col(c):  return "movie_name" if c["kind"] == "arr" else "movie"
def singer_col(c): return "name" if c["kind"] == "arr" else "singer"
def songs_tbl(c):  return "songs" if c["kind"] == "arr" else "songs_" + c["prefix"]
def singers_tbl(c):return "singers" if c["kind"] == "arr" else "singers_" + c["prefix"]

# ---------- entity sampling ----------
def entities(cname):
    c = COMPOSERS[cname]
    sc, dc, mc = song_col(c), date_col(c), movie_col(c)
    movies = [r[0] for r in q(c["db"], f"SELECT DISTINCT {mc} FROM {songs_tbl(c)} WHERE {mc} IS NOT NULL AND LENGTH({mc})>2")]
    songs = [r[0] for r in q(c["db"], f"SELECT DISTINCT {sc} FROM {songs_tbl(c)} WHERE {sc} IS NOT NULL AND LENGTH({sc})>3")]
    singers = [r[0] for r in q(c["db"], f"SELECT DISTINCT s.{singer_col(c)} FROM {singers_tbl(c)} s")]
    years = [r[0] for r in q(c["db"], f"SELECT DISTINCT substr({dc},1,4) FROM {songs_tbl(c)} WHERE {dc} IS NOT NULL AND length({dc})>=4")]
    rng.shuffle(movies); rng.shuffle(songs); rng.shuffle(singers); rng.shuffle(years)
    rng.shuffle(years)
    return {"movies": movies[:60], "songs": songs[:60], "singers": singers[:60], "years": years[:40]}

# ---------- SQL builders (per template id) ----------
def B(tid, cname, s):
    c = COMPOSERS[cname]; sc, dc, mc, sic = song_col(c), date_col(c), movie_col(c), singer_col(c)
    T, S = songs_tbl(c), singers_tbl(c)
    y = s.get("year", "")
    movie = s.get("movie", "")
    song = s.get("song", "")
    singer = s.get("singer", "")
    esc = lambda v: v.replace("'", "''")
    like = lambda v: f"'%{esc(v)}%'"
    if tid == "year_movies_count":
        return f"SELECT COUNT(DISTINCT {mc}) FROM {T} WHERE {dc} LIKE '{y}%'", "count"
    if tid == "year_movies_list":
        return f"SELECT DISTINCT {mc} FROM {T} WHERE {dc} LIKE '{y}%' ORDER BY {mc}", "list"
    if tid == "movie_songs_count":
        return f"SELECT COUNT(*) FROM {T} WHERE {mc} LIKE {like(movie)}", "count"
    if tid == "movie_songs_list":
        return f"SELECT DISTINCT {sc} FROM {T} WHERE {mc} LIKE {like(movie)} ORDER BY {sc}", "list"
    if tid == "movie_singers_list":
        return f"SELECT DISTINCT s2.{sic} FROM {T} s1 JOIN {S} s2 ON s1.song_id=s2.song_id WHERE s1.{mc} LIKE {like(movie)} ORDER BY s2.{sic}", "list"
    if tid == "song_singers_list":
        return f"SELECT DISTINCT s2.{sic} FROM {T} s1 JOIN {S} s2 ON s1.song_id=s2.song_id WHERE s1.{sc} LIKE {like(song)} ORDER BY s2.{sic}", "list"
    if tid == "song_singers_count":
        return f"SELECT COUNT(DISTINCT s2.{sic}) FROM {T} s1 JOIN {S} s2 ON s1.song_id=s2.song_id WHERE s1.{sc} LIKE {like(song)}", "count"
    if tid == "song_year":
        return f"SELECT MIN({dc}) FROM {T} WHERE {sc} LIKE {like(song)}", "scalar"
    if tid == "total_movies":
        return f"SELECT COUNT(DISTINCT {mc}) FROM {T}", "count"
    if tid == "total_singers":
        return f"SELECT COUNT(DISTINCT {sic}) FROM {S}", "count"
    if tid == "top_singers":
        return f"SELECT s2.{sic} FROM {S} s2 JOIN {T} s1 ON s1.song_id=s2.song_id GROUP BY s2.{sic} ORDER BY COUNT(*) DESC, s2.{sic} ASC LIMIT 5", "list"
    if tid == "year_most_movies":
        return f"SELECT substr({dc},1,4) FROM {T} GROUP BY substr({dc},1,4) ORDER BY COUNT(DISTINCT {mc}) DESC LIMIT 1", "name"
    if tid == "first_album":
        return f"SELECT {mc} FROM {T} GROUP BY {mc} ORDER BY MIN({dc}) ASC LIMIT 1", "name"
    if tid == "first_album_year":
        return f"SELECT MIN({dc}) FROM {T}", "scalar"
    if tid == "singer_songs_count_c":
        return f"SELECT COUNT(DISTINCT s1.{sc}) FROM {T} s1 JOIN {S} s2 ON s1.song_id=s2.song_id WHERE s2.{sic} LIKE {like(singer)}", "count"
    if tid == "singer_songs_list_year":
        return f"SELECT DISTINCT s1.{sc} FROM {T} s1 JOIN {S} s2 ON s1.song_id=s2.song_id WHERE s2.{sic} LIKE {like(singer)} AND s1.{dc} LIKE '{y}%' ORDER BY s1.{sc}", "list"
    if tid == "singer_songs_count_year":
        return f"SELECT COUNT(DISTINCT s1.{sc}) FROM {T} s1 JOIN {S} s2 ON s1.song_id=s2.song_id WHERE s2.{sic} LIKE {like(singer)} AND s1.{dc} LIKE '{y}%'", "count"
    if tid == "singer_movies_count":
        return f"SELECT COUNT(DISTINCT s1.{mc}) FROM {T} s1 JOIN {S} s2 ON s1.song_id=s2.song_id WHERE s2.{sic} LIKE {like(singer)}", "count"
    if tid == "singer_top_year":
        return f"SELECT substr(s1.{dc},1,4) FROM {T} s1 JOIN {S} s2 ON s1.song_id=s2.song_id WHERE s2.{sic} LIKE {like(singer)} GROUP BY substr(s1.{dc},1,4) ORDER BY COUNT(*) DESC LIMIT 1", "name"
    if tid == "singer_first_year_c":
        return f"SELECT MIN(substr(s1.{dc},1,4)) FROM {T} s1 JOIN {S} s2 ON s1.song_id=s2.song_id WHERE s2.{sic} LIKE {like(singer)}", "scalar"
    raise ValueError(tid)

# ---------- cross-composer SQL (UNION ALL over all six composers) ----------
def union_sql(select_body, where="", group_extra="", order="", limit=""):
    parts = []
    for cname in ["arr", "ila", "pri", "sel", "sus", "vsh"]:
        c = COMPOSERS[cname]
        sc, dc, mc, sic = song_col(c), date_col(c), movie_col(c), singer_col(c)
        T, S = songs_tbl(c), singers_tbl(c)
        parts.append(
            f"SELECT s1.{sc} AS song, s2.{sic} AS singer, substr(s1.{dc},1,4) AS year, "
            f"'{cname}' AS composer FROM {T} s1 JOIN {S} s2 ON s1.song_id=s2.song_id")
    u = " UNION ALL ".join(parts)
    sql = f"SELECT {select_body} FROM ({u}) u"
    if where: sql += f" WHERE {where}"
    if group_extra: sql += f" GROUP BY {group_extra}"
    if order: sql += f" ORDER BY {order}"
    if limit: sql += f" LIMIT {limit}"
    return sql

def XB(tid, s):
    """Cross-composer templates; returns (sql, shape)."""
    singer = s.get("singer", "")
    y = s.get("year", "")
    esc = lambda v: v.replace("'", "''")
    like = lambda v: f"'%{esc(v)}%'"
    if tid == "xc_singer_composers_count":
        return union_sql("COUNT(DISTINCT composer)", f"singer LIKE {like(singer)}"), "count"
    if tid == "xc_singer_composers_list":
        return union_sql("DISTINCT composer", f"singer LIKE {like(singer)} ORDER BY composer"), "list"
    if tid == "xc_singer_songs_count_year":
        return union_sql("COUNT(DISTINCT song)", f"singer LIKE {like(singer)} AND year='{y}'"), "count"
    if tid == "xc_singer_songs_list_year":
        return union_sql("DISTINCT song", f"singer LIKE {like(singer)} AND year='{y}' ORDER BY song LIMIT 25"), "list"
    if tid == "xc_singer_top_composer":
        return union_sql("composer, COUNT(*) AS n", f"singer LIKE {like(singer)}", "composer", "n DESC"), "pairs"
    if tid == "xc_singer_total_songs":
        return union_sql("COUNT(DISTINCT song)", f"singer LIKE {like(singer)}"), "count"
    if tid == "xc_year_total_songs_all":
        return union_sql("COUNT(*)", f"year='{y}'"), "count"
    if tid == "xc_singer_total_songs_year":
        return union_sql("COUNT(DISTINCT song)", f"singer LIKE {like(singer)} AND year='{y}'"), "count"
    raise ValueError(tid)

# ---------- phrasing templates given to the model ----------
# Each template id maps to a list of surface forms; one variant is chosen per
# round so rounds yield differently phrased questions over the same SQL.
PHRASES = {
    "year_movies_count": [
        "How many movies did {composer} release in {year}?",
        "In {year}, how many films did {composer} score?",
    ],
    "year_movies_list": [
        "List the albums that {composer} released in {year}.",
        "Which albums did {composer} compose in {year}? Name each one.",
    ],
    "movie_songs_count": [
        "How many songs are there in the movie {movie} by {composer}?",
        "Count the tracks on {composer}'s soundtrack for the film {movie}.",
    ],
    "movie_songs_list": [
        "List the songs in the film {movie} by {composer}.",
        "Give the full tracklist of {composer}'s album for {movie}.",
    ],
    "movie_singers_list": [
        "Who are the singers in the movie {movie} by {composer}?",
        "List the playback singers featured on {composer}'s soundtrack for the film {movie}.",
    ],
    "song_singers_list": [
        "Who sang the song {song} for {composer}?",
        "Find the vocalists on {song} from {composer}'s album for the film containing it.",
        "Name every playback singer credited on the track {song} in {composer}'s catalogue.",
    ],
    "song_singers_count": [
        "How many artists performed the song {song} for {composer}?",
        "How many vocalists are credited on {song} from {composer}'s filmography?",
    ],
    "song_year": "When was the song {song} released?",
    "total_movies": "How many movies has {composer} composed for?",
    "total_singers": "How many singers has {composer} collaborated with?",
    "top_singers": "Who are {composer}'s top 5 collaborators?",
    "year_most_movies": "Which year did {composer} compose the most movies?",
    "first_album": "What was the name of {composer}'s first album?",
    "first_album_year": "When was {composer}'s first album released?",
    "singer_songs_count_c": [
        "How many songs did {singer} sing for {composer}?",
        "How many tracks in {composer}'s catalogue feature {singer} as a vocalist?",
    ],
    "singer_songs_list_year": [
        "List all songs that {singer} sang for {composer} in {year}.",
        "Which songs did {singer} record for {composer} in {year}? List them.",
    ],
    "singer_songs_count_year": "How many songs did {singer} sing for {composer} in {year}?",
    "singer_movies_count": [
        "How many movies did {singer} sing for {composer}?",
        "How many films did {singer} contribute vocals to in {composer}'s discography?",
    ],
    "singer_top_year": "In which year did {singer} sing the most songs for {composer}?",
    "singer_first_year_c": [
        "In which year did {singer} first sing for {composer}?",
        "When did {singer} first work with {composer}?",
        "When did {singer} first appear on a {composer} album?",
        "Which year did {singer} first collaborate with {composer}?",
        "When did {singer} start singing for {composer}?",
        "What year did {singer} make their first recording for {composer}?",
    ],
    "xc_singer_composers_count": [
        "How many music composers did the singer {singer} sing for?",
        "Across all six composers, how many composers feature the singer {singer} on their soundtracks?",
    ],
    "xc_singer_composers_list": "Which music composers did the singer {singer} sing for?",
    "xc_singer_songs_count_year": "How many songs did the singer {singer} sing in {year}?",
    "xc_singer_songs_list_year": "List the songs the singer {singer} sang in {year}.",
    "xc_singer_top_composer": "Which composer did the singer {singer} sing the most songs for?",
    "xc_singer_total_songs": "How many songs has the singer {singer} sung in total?",
    "xc_year_total_songs_all": "How many songs in total were released across all composers in {year}?",
    "xc_singer_total_songs_year": "Across all composers, how many songs did the singer {singer} sing in {year}?",
}

def phrase_for(tid):
    v = PHRASES[tid]
    return v[rng.randrange(len(v))] if isinstance(v, list) else v

# ---------- model call ----------
MODEL = os.environ.get("QA_MODEL", "z-ai/glm-5.3-flashx")
API = "https://openrouter.ai/api/v1/chat/completions"

def llm(prompt, retries=3):
    key = os.environ["OPENROUTER_API_KEY"]
    body = json.dumps({
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.5, "max_tokens": 4000,
    }).encode()
    for i in range(retries):
        req = urllib.request.Request(API, data=body, headers={
            "Authorization": f"Bearer {key}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                d = json.load(r)
            msg = d["choices"][0]["message"]
            content = msg.get("content")
            if not content and msg.get("reasoning"):
                content = msg["reasoning"]
            if content:
                return content
            print(f"  empty content, keys={list(msg)}", file=sys.stderr)
        except Exception as e:
            print(f"  llm retry {i}: {e}", file=sys.stderr)
    raise RuntimeError("LLM failed")

# ---------- question generation ----------
def per_composer_questions(cname, ents, n_target):
    phr = "\n".join(f"- {k}: {phrase_for(k)}" for k in PHRASES if not k.startswith("xc_"))
    prompt = f"""You are creating a QA dataset for a music database. Composer: {COMPOSERS[cname]['name']} (id '{cname}').

Available entities you MUST use verbatim (copy exact strings, including spelling/punctuation):
- movies: {json.dumps(ents['movies'])}
- songs: {json.dumps(ents['songs'])}
- singers: {json.dumps(ents['singers'])}
- years: {json.dumps(ents['years'])}

Question templates (each with required slots):
{phr}

Rules:
- Produce {n_target} questions, one JSON object per line: {{"template_id": "...", "composer": "{cname}", "slots": {{"year": "...", "movie": "...", "song": "...", "singer": "..."}}, "question": "..."}}
- slots must ONLY contain the fields a template needs (e.g. total_movies needs no slots); every slot value must be copied verbatim from the entity lists above.
- Each template_id may be used at most twice. Prefer variety across templates.
- Questions must be natural and varied; you may rephrase slightly (e.g. "film" vs "movie", "released" vs "came out") but keep all entity names EXACTLY as given.
- The singer-perspective templates (singer_*) are important: use at least 5 of them.
- Do NOT reason out loud or use world knowledge to invent entities. Your ENTIRE reply must be exactly {n_target} JSON lines, nothing else — no preamble, no analysis, no markdown fences."""
    out = llm(prompt)
    items = []
    for line in out.splitlines():
        line = line.strip().strip("`")
        if not line or line.startswith("-"):
            continue
        try:
            it = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(it, dict):
            items.append(it)
    return items

def cross_composer_questions(singers_multi, n_target):
    phr = "\n".join(f"- {k}: {phrase_for(k)}" for k in PHRASES if k.startswith("xc_"))
    prompt = f"""You are creating a QA dataset for a music database spanning six composers: A. R. Rahman (arr), Ilaiyaraaja (ila), Pritam (pri), Shankar-Ehsaan-Loy (sel), Sushin Shyam (sus), Vishal-Shekhar (vsh).

Singers who sang for MORE THAN ONE composer (use these verbatim): {json.dumps(singers_multi)}

Years available: 1991-2026 (pick common ones like 2013, 2016, 2019, 2022).

Templates (xc_ = cross-composer, the question does NOT name one composer; it spans all six):
{phr}

Rules:
- Produce {n_target} questions, one JSON object per line: {{"template_id": "...", "composer": "all", "slots": {{"year": "...", "singer": "..."}}, "question": "..."}}
- singer values must be copied verbatim from the list above. Prefer singers with many collaborations.
- Each template_id may be used at most twice. Use at least 6 different templates.
- Output ONLY JSON lines."""
    out = llm(prompt)
    items = []
    for line in out.splitlines():
        line = line.strip().strip("`")
        if not line:
            continue
        try:
            it = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(it, dict):
            items.append(it)
    return items

# ---------- validation + rendering ----------
def validate_entity(cname, kind, value):
    c = COMPOSERS[cname]
    col = {"movie": movie_col(c), "song": song_col(c), "singer": singer_col(c)}[kind]
    tbl = songs_tbl(c) if kind in ("movie", "song") else singers_tbl(c)
    if kind == "singer":
        n = q(c["db"], f"SELECT COUNT(*) FROM {tbl} WHERE {col} LIKE ?", (f"%{value}%",))[0][0]
    else:
        n = q(c["db"], f"SELECT COUNT(*) FROM {tbl} WHERE {col} LIKE ?", (f"%{value}%",))[0][0]
    return n > 0

def fmt_answer(rows, shape):
    if shape in ("count", "scalar", "name"):
        v = rows[0][0] if rows else None
        return v
    if shape == "list":
        return sorted({r[0] for r in rows if r[0] is not None})
    if shape == "pairs":
        return [{"composer": r[0], "songs": r[1]} for r in rows]
    return rows

def heads_for(tid, cname):
    c = COMPOSERS[cname]
    sc, dc, mc, sic = song_col(c), date_col(c), movie_col(c), singer_col(c)
    join = ["songs_singers"] if tid in ("movie_singers_list", "song_singers_list", "song_singers_count",
                                        "singer_songs_count_c", "singer_songs_list_year",
                                        "singer_songs_count_year", "singer_movies_count",
                                        "singer_top_year", "singer_first_year_c") else []
    selmap = {"year_movies_count": [mc], "year_movies_list": [mc], "movie_songs_count": [sc],
              "movie_songs_list": [sc], "movie_singers_list": ["singer"], "song_singers_list": ["singer"],
              "song_singers_count": ["singer"], "song_year": [dc], "total_movies": [mc],
              "total_singers": ["singer"], "top_singers": ["singer"], "year_most_movies": [dc],
              "first_album": [mc], "first_album_year": [dc], "singer_songs_count_c": [sc],
              "singer_songs_list_year": [sc], "singer_songs_count_year": [sc],
              "singer_movies_count": [mc], "singer_top_year": [dc], "singer_first_year_c": [dc]}
    fcol = {"year_movies_count": "year", "year_movies_list": "year", "movie_songs_count": "movie_name",
            "movie_songs_list": "movie_name", "movie_singers_list": "movie_name",
            "song_singers_list": "title", "song_singers_count": "title", "song_year": "title",
            "singer_songs_list_year": "year", "singer_songs_count_year": "year"}
    aggs = {"year_movies_count": "count", "movie_songs_count": "count", "song_singers_count": "count",
            "total_movies": "count", "total_singers": "count", "top_singers": "count",
            "year_most_movies": "count", "song_year": "min", "first_album_year": "min",
            "singer_songs_count_c": "count", "singer_songs_count_year": "count",
            "singer_movies_count": "count", "singer_first_year_c": "min"}
    gby = {"top_singers": "singer", "year_most_movies": "year"}
    order = {"top_singers": "count_desc", "year_most_movies": "count_desc",
             "first_album": "date_asc", "movie_songs_list": "name_asc",
             "year_movies_list": "name_asc", "movie_singers_list": "name_asc",
             "song_singers_list": "name_asc", "singer_songs_list_year": "name_asc"}
    # canonical skeleton column names (schema labels are Rahman-canonical;
    # per-composer DB columns map onto them for the heads only)
    canon = {"song": "title", "movie": "movie_name", "release_date": "date"}
    sel = [canon.get(c, c) for c in selmap.get(tid, [])]
    return {"select_cols": sel, "filter_col": fcol.get(tid, "none"),
            "filter_op": fcol.get(tid, "none") != "none" and "like" or "none",
            "agg": aggs.get(tid, "none"), "group_by": gby.get(tid, "none"),
            "join": join, "order": order.get(tid, "none"), "limit": "none"}

def main():
    global rng
    ap = argparse.ArgumentParser(description="Generate a multi-composer QA dataset.")
    ap.add_argument("--rounds", type=int, default=1, help="generation rounds (rng reseeded per round)")
    ap.add_argument("--out", default=os.path.join(OUT_DIR, "gold-multi.jsonl"))
    ap.add_argument("--seed-base", type=int, default=42,
                    help="base seed; round r uses seed_base + r")
    ap.add_argument("--resume", action="store_true",
                    help="load existing --out file and append new entries to it")
    ap.add_argument("--min-entries", type=int, default=0,
                    help="stop early once accumulated entries (after dedup) reach this count")
    a = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    norm_q = lambda t: re.sub(r"[^a-z0-9 ]", "", t.lower()).strip()
    entries = []
    seen = set()
    dedup_skipped = 0
    idx = 0

    def add_entry(e):
        nonlocal idx, dedup_skipped
        key = norm_q(e["query"])
        if key in seen:
            dedup_skipped += 1
            return False
        seen.add(key)
        idx += 1
        e["idx"] = idx
        entries.append(e)
        return True

    # 1) carry over the 31 original Rahman questions with composer field
    orig = [json.loads(l) for l in open(os.path.join(ROOT, "experiments/gliner-decide/gold.jsonl"))]
    for g in orig:
        e = dict(g); e["composer"] = "arr"; e["template_id"] = "original"
        add_entry(e)

    # 1b) resume: load previously generated entries, keeping their idx
    if a.resume and os.path.exists(a.out):
        prev = 0
        for line in open(a.out):
            e = json.loads(line)
            key = norm_q(e["query"])
            if key in seen:
                continue
            seen.add(key)
            idx = max(idx, e.get("idx", idx + 1))
            entries.append(e)
            prev += 1
        print(f"resume: {prev} existing entries loaded")

    # 2) multi-composer singers for cross-composer questions (computed once)
    counts = {}
    for cname in COMPOSERS:
        c = COMPOSERS[cname]
        for r in q(c["db"], f"SELECT DISTINCT {singer_col(c)} FROM {singers_tbl(c)}"):
            counts.setdefault(r[0], set()).add(cname)
    multi = [s for s, cs in counts.items() if len(cs) >= 2 and len(s) > 3]
    print(f"{len(multi)} multi-composer singers")

    N_TARGET = 30
    for rnd in range(a.rounds):
        rng = random.Random(a.seed_base + rnd)  # reseed per round so rounds differ
        all_ents = {cn: entities(cn) for cn in COMPOSERS}
        round_new = 0
        # per-composer questions
        for cname in ["ila", "pri", "sel", "sus", "vsh", "arr"]:
            ents = all_ents[cname]
            ok = 0
            for attempt in range(8):
                items = per_composer_questions(cname, ents, N_TARGET)
                ok = 0
                for it in items:
                    tid = it.get("template_id"); slots = it.get("slots", {})
                    if tid not in PHRASES or tid.startswith("xc_"):
                        continue
                    # validate slot entities exist
                    bad = False
                    for kind in ("movie", "song", "singer"):
                        v = slots.get(kind)
                        if v and not validate_entity(cname, kind, v):
                            bad = True; break
                    if bad:
                        continue
                    if slots.get("year") and slots["year"] not in ents["years"]:
                        continue
                    try:
                        sql, shape = B(tid, cname, slots)
                    except ValueError:
                        continue
                    rows = q(COMPOSERS[cname]["db"], sql)
                    ans = fmt_answer(rows, shape)
                    if ans is None or (shape == "list" and not ans) or (shape == "count" and ans == 0):
                        continue  # empty result => question is unanswerable, drop
                    values = {k: v for k, v in slots.items() if v}
                    if add_entry({"composer": cname, "query": it["question"], "template_id": tid,
                                  "heads": heads_for(tid, cname), "values": values,
                                  "ref_sql": sql, "answer_shape": shape, "answer": ans}):
                        ok += 1
                if ok >= 20:
                    break
            print(f"round {rnd} {cname}: {ok} generated questions")
            round_new += ok
            if a.min_entries and len(entries) >= a.min_entries:
                break
        # cross-composer questions (singers with >=2 composers)
        rng.shuffle(multi)
        items = cross_composer_questions(multi[:40], N_TARGET)
        ok = 0
        for it in items:
            tid = it.get("template_id"); slots = it.get("slots", {})
            if tid not in PHRASES or not tid.startswith("xc_"):
                continue
            singer = slots.get("singer")
            if singer and singer not in multi:
                # fuzzy: allow if it matches one multi singer case-insensitively
                match = [m for m in multi if m.lower() == singer.lower()]
                if not match:
                    continue
                singer = match[0]; slots["singer"] = singer
            if slots.get("year") and not (1991 <= int(slots["year"]) <= 2026):
                continue
            try:
                sql, shape = XB(tid, slots)
            except ValueError:
                continue
            rows = q(ARR_DB, sql)
            ans = fmt_answer(rows, shape)
            if ans is None or (shape == "list" and not ans) or (shape == "count" and ans == 0):
                continue
            if add_entry({"composer": "all", "query": it["question"], "template_id": tid,
                          "heads": {"select_cols": ["singer"], "filter_col": "none", "filter_op": "none",
                                    "agg": "count", "group_by": "none", "join": [], "order": "none",
                                    "limit": "none"},
                          "values": {k: v for k, v in slots.items() if v},
                          "ref_sql": sql, "answer_shape": shape, "answer": ans}):
                ok += 1
        print(f"round {rnd} cross-composer: {ok} generated questions")
        round_new += ok
        print(f"round {rnd}: +{round_new} new, {dedup_skipped} dedup-skipped, total {len(entries)}")
        if a.min_entries and len(entries) >= a.min_entries:
            break

    with open(a.out, "w") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    with open(os.path.join(OUT_DIR, "ref_results.json"), "w") as f:
        json.dump({str(e["idx"]): e.get("answer") for e in entries if "answer" in e}, f, ensure_ascii=False, indent=1)
    print(f"TOTAL: {len(entries)} entries ({dedup_skipped} duplicates skipped) -> {a.out}")

if __name__ == "__main__":
    main()
