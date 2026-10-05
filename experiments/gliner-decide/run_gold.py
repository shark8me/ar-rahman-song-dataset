#!/usr/bin/env python3
"""Build gold skeletons + reference SQL for the 31 ARR queries, execute against arr.db,
and emit gold.jsonl + ref_results.json.

Gold semantics (documented convention, since dataset is single-composer = A. R. Rahman):
- Two small tables: songs(song_id,title,date,movie_name,release_id), singers(song_id,name).
- "album"/"movie"/"film"/"soundtrack" -> the WORK = distinct movie_name (release_id is an
  album *edition*; work-level semantics used for all counts/names in normalize_work mode).
- "Rahman"/"composer" is implicit (all rows) so no composer WHERE is added.
- Entity literals are matched with LIKE (dataset has casing/variant noise).
"""
import csv, json, sqlite3, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, 'arr.db')

def norm(res):
    """Normalize a result set for equality comparison (order-insensitive for lists)."""
    out = []
    for row in res:
        row = tuple(None if v is None else str(v).strip() for v in row)
        # single-value rows -> scalar string; multi -> tuple
        out.append(row[0] if len(row) == 1 else row)
    return sorted(out, key=str)

GOLD = [
 # idx, query, heads(dict), values(dict), ref_sql, answer_shape
 (1,  "How many albums has Rahman released?",
   {"select_cols":["movie_name"],"filter_col":"none","filter_op":"none","agg":"count","group_by":"none","join":[],"order":"none","limit":"none"},
   {}, "SELECT COUNT(DISTINCT movie_name) FROM songs", "count"),
 (2,  "How many movies has Rahman released?",
   {"select_cols":["movie_name"],"filter_col":"none","filter_op":"none","agg":"count","group_by":"none","join":[],"order":"none","limit":"none"},
   {}, "SELECT COUNT(DISTINCT movie_name) FROM songs", "count"),
 (3,  "When was Rahman's first album released?",
   {"select_cols":["date"],"filter_col":"none","filter_op":"none","agg":"min","group_by":"none","join":[],"order":"none","limit":"1"},
   {}, "SELECT MIN(date) FROM songs", "scalar"),
 (4,  "When was Rahman's last album released?",
   {"select_cols":["date"],"filter_col":"none","filter_op":"none","agg":"max","group_by":"none","join":[],"order":"none","limit":"1"},
   {}, "SELECT MAX(date) FROM songs", "scalar"),
 (5,  "What was the name of Rahman's first album?",
   {"select_cols":["movie_name"],"filter_col":"none","filter_op":"none","agg":"none","group_by":"none","join":[],"order":"date_asc","limit":"1"},
   {}, "SELECT movie_name FROM songs GROUP BY movie_name ORDER BY MIN(date) ASC LIMIT 1", "name"),
 (6,  "What was the name of Rahman's second album?",
   {"select_cols":["movie_name"],"filter_col":"none","filter_op":"none","agg":"none","group_by":"none","join":[],"order":"date_asc","limit":"1","offset":1},
   {}, "SELECT DISTINCT movie_name FROM songs GROUP BY movie_name ORDER BY MIN(date) ASC LIMIT 1 OFFSET 1", "name"),
 (7,  "What was the name of Rahman's last album?",
   {"select_cols":["movie_name"],"filter_col":"none","filter_op":"none","agg":"none","group_by":"none","join":[],"order":"date_desc","limit":"1"},
   {}, "SELECT DISTINCT movie_name FROM songs GROUP BY movie_name ORDER BY MAX(date) DESC LIMIT 1", "name"),
 (8,  "When was the album Jodhaa Akbar released?",
   {"select_cols":["date"],"filter_col":"movie_name","filter_op":"like","agg":"min","group_by":"none","join":[],"order":"none","limit":"1"},
   {"work_name":"Jodhaa Akbar"}, "SELECT MIN(date) FROM songs WHERE movie_name LIKE '%Jodhaa Akbar%'", "scalar"),
 (9,  "How many artists has Rahman collaborated with?",
   {"select_cols":["singer"],"filter_col":"none","filter_op":"none","agg":"count","group_by":"none","join":["songs_singers"],"order":"none","limit":"none"},
   {}, "SELECT COUNT(DISTINCT name) FROM singers", "count"),
 (10, "How many singers has Rahman collaborated with?",
   {"select_cols":["singer"],"filter_col":"none","filter_op":"none","agg":"count","group_by":"none","join":["songs_singers"],"order":"none","limit":"none"},
   {}, "SELECT COUNT(DISTINCT name) FROM singers", "count"),
 (11, "How many songs are there in the movie Roja?",
   {"select_cols":["title"],"filter_col":"movie_name","filter_op":"like","agg":"count","group_by":"none","join":[],"order":"none","limit":"none"},
   {"work_name":"Roja"}, "SELECT COUNT(*) FROM songs WHERE movie_name LIKE '%Roja%'", "count"),
 (12, "Which singer sang the song Roja Jaaneman?",
   {"select_cols":["singer"],"filter_col":"title","filter_op":"like","agg":"none","group_by":"none","join":["songs_singers"],"order":"none","limit":"none"},
   {"work_name":"Roja Jaaneman"}, "SELECT DISTINCT s2.name FROM songs s1 JOIN singers s2 ON s1.song_id=s2.song_id WHERE s1.title LIKE '%Jaaneman%'", "list"),
 (13, "When was the song Roja Jaaneman released?",
   {"select_cols":["date"],"filter_col":"title","filter_op":"like","agg":"min","group_by":"none","join":[],"order":"none","limit":"1"},
   {"work_name":"Roja Jaaneman"}, "SELECT MIN(date) FROM songs WHERE title LIKE '%Jaaneman%'", "scalar"),
 (14, "How many artists performed the song Aadi Paaru Mangaatha?",
   {"select_cols":["singer"],"filter_col":"title","filter_op":"like","agg":"count","group_by":"none","join":["songs_singers"],"order":"none","limit":"none"},
   {"work_name":"Aadi Paaru Mangaatha"}, "SELECT COUNT(DISTINCT s2.name) FROM songs s1 JOIN singers s2 ON s1.song_id=s2.song_id WHERE s1.title LIKE '%Aadi Paaru Mangaatha%'", "count"),
 (15, "who sang song Aadi Paaru Mangaatha?",
   {"select_cols":["singer"],"filter_col":"title","filter_op":"like","agg":"none","group_by":"none","join":["songs_singers"],"order":"none","limit":"none"},
   {"work_name":"Aadi Paaru Mangaatha"}, "SELECT DISTINCT s2.name FROM songs s1 JOIN singers s2 ON s1.song_id=s2.song_id WHERE s1.title LIKE '%Aadi Paaru Mangaatha%'", "list"),
 (16, "Who are artist Rahman's top 5 collaborators?",
   {"select_cols":["singer"],"filter_col":"none","filter_op":"none","agg":"count","group_by":"singer","join":["songs_singers"],"order":"count_desc","limit":"5"},
   {}, "SELECT s2.name FROM singers s2 JOIN songs s1 ON s1.song_id=s2.song_id GROUP BY s2.name ORDER BY COUNT(*) DESC, s2.name ASC LIMIT 5", "list"),
 (17, "How many movies did Rahman release in 1999?",
   {"select_cols":["movie_name"],"filter_col":"year","filter_op":"year_eq","agg":"count","group_by":"none","join":[],"order":"none","limit":"none"},
   {"year":"1999"}, "SELECT COUNT(DISTINCT movie_name) FROM songs WHERE date LIKE '1999%'", "count"),
 (18, "List the albums that Rahman released in 1999.",
   {"select_cols":["movie_name"],"filter_col":"year","filter_op":"year_eq","agg":"none","group_by":"none","join":[],"order":"none","limit":"none"},
   {"year":"1999"}, "SELECT DISTINCT movie_name FROM songs WHERE date LIKE '1999%'", "list"),
 (19, "Who are the singers in movie Delhi-6?",
   {"select_cols":["singer"],"filter_col":"movie_name","filter_op":"like","agg":"none","group_by":"none","join":["songs_singers"],"order":"none","limit":"none"},
   {"work_name":"Delhi-6"}, "SELECT DISTINCT s2.name FROM songs s1 JOIN singers s2 ON s1.song_id=s2.song_id WHERE s1.movie_name LIKE '%Delhi-6%'", "list"),
 (20, "Who are the singers for the song Aaromale",
   {"select_cols":["singer"],"filter_col":"title","filter_op":"like","agg":"none","group_by":"none","join":["songs_singers"],"order":"none","limit":"none"},
   {"work_name":"Aaromale"}, "SELECT DISTINCT s2.name FROM songs s1 JOIN singers s2 ON s1.song_id=s2.song_id WHERE s1.title LIKE '%Aaromale%'", "list"),
 (21, "Who collaborated with singer Mathangi on song Aaha Tamizhamma?",
   {"select_cols":["singer"],"filter_col":"title","filter_op":"like","agg":"none","group_by":"none","join":["songs_singers"],"order":"none","limit":"none"},
   {"work_name":"Aaha Tamizhamma","singer":"Mathangi"}, "SELECT DISTINCT s2.name FROM songs s1 JOIN singers s2 ON s1.song_id=s2.song_id WHERE s1.title LIKE '%Aaha Tamizhamma%'", "list"),
 (22, "How many movies did Rahman compose for?",
   {"select_cols":["movie_name"],"filter_col":"none","filter_op":"none","agg":"count","group_by":"none","join":[],"order":"none","limit":"none"},
   {}, "SELECT COUNT(DISTINCT movie_name) FROM songs", "count"),
 (23, "List the film soundtracks that Rahman has composed.",
   {"select_cols":["movie_name"],"filter_col":"none","filter_op":"none","agg":"none","group_by":"none","join":[],"order":"none","limit":"none"},
   {}, "SELECT DISTINCT movie_name FROM songs", "list"),
 (24, "List the songs in the film The Gentleman",
   {"select_cols":["title"],"filter_col":"movie_name","filter_op":"like","agg":"none","group_by":"none","join":[],"order":"none","limit":"none"},
   {"work_name":"The Gentleman"}, "SELECT DISTINCT title FROM songs WHERE movie_name LIKE '%Gentleman%'", "list"),
 (25, "Which film is the song Agar Tum Saath Ho in?",
   {"select_cols":["movie_name"],"filter_col":"title","filter_op":"like","agg":"none","group_by":"none","join":[],"order":"none","limit":"1"},
   {"work_name":"Agar Tum Saath Ho"}, "SELECT DISTINCT movie_name FROM songs WHERE title LIKE '%Agar Tum Saath Ho%' LIMIT 1", "name"),
 (26, "Which year was the song Alaipayuthey released?",
   {"select_cols":["date"],"filter_col":"title","filter_op":"like","agg":"min","group_by":"none","join":[],"order":"none","limit":"1"},
   {"work_name":"Alaipayuthey"}, "SELECT MIN(date) FROM songs WHERE title LIKE '%Alaipayuthey%' AND title NOT LIKE '%(Dialogue%'", "scalar"),
 (27, "list all the singers who sang for the movie Alaipayuthey",
   {"select_cols":["singer"],"filter_col":"movie_name","filter_op":"like","agg":"none","group_by":"none","join":["songs_singers"],"order":"none","limit":"none"},
   {"work_name":"Alaipayuthey"}, "SELECT DISTINCT s2.name FROM songs s1 JOIN singers s2 ON s1.song_id=s2.song_id WHERE s1.movie_name LIKE '%Alaipayuthey%'", "list"),
 (28, "List all songs that the singer Sid Sriram sang in 2016",
   {"select_cols":["title"],"filter_col":"year","filter_op":"year_eq","agg":"none","group_by":"none","join":["songs_singers"],"order":"none","limit":"none","singer_filter":"Sid Sriram"},
   {"year":"2016","singer":"Sid Sriram"}, "SELECT DISTINCT s1.title FROM songs s1 JOIN singers s2 ON s1.song_id=s2.song_id WHERE s2.name LIKE '%Sid Sriram%' AND s1.date LIKE '2016%'", "list"),
 (29, "Which singers sang for Rahman in the year 2016",
   {"select_cols":["singer"],"filter_col":"year","filter_op":"year_eq","agg":"none","group_by":"none","join":["songs_singers"],"order":"none","limit":"none"},
   {"year":"2016"}, "SELECT DISTINCT s2.name FROM songs s1 JOIN singers s2 ON s1.song_id=s2.song_id WHERE s1.date LIKE '2016%'", "list"),
 (30, "How many singers sang for Rahman in 2016",
   {"select_cols":["singer"],"filter_col":"year","filter_op":"year_eq","agg":"count","group_by":"none","join":["songs_singers"],"order":"none","limit":"none"},
   {"year":"2016"}, "SELECT COUNT(DISTINCT s2.name) FROM songs s1 JOIN singers s2 ON s1.song_id=s2.song_id WHERE s1.date LIKE '2016%'", "count"),
 (31, "Which year did Rahman compose the most movies?",
   {"select_cols":["date"],"filter_col":"none","filter_op":"none","agg":"count","group_by":"year","join":[],"order":"count_desc","limit":"1"},
   {}, "SELECT substr(date,1,4) AS y FROM songs GROUP BY substr(date,1,4) ORDER BY COUNT(DISTINCT movie_name) DESC LIMIT 1", "name"),
]

def main():
    db = sqlite3.connect(DB)
    records, results = [], {}
    for idx, query, heads, values, ref_sql, shape in GOLD:
        try:
            rows = db.execute(ref_sql).fetchall()
            res = norm(rows)
            err = None
        except Exception as e:
            res, err = None, f"{type(e).__name__}: {e}"
        records.append({"idx": idx, "query": query, "heads": heads,
                        "values": values, "ref_sql": ref_sql, "answer_shape": shape})
        results[str(idx)] = {"query": query, "ref_sql": ref_sql, "expected": res,
                             "error": err, "shape": shape}
    db.close()
    with open(os.path.join(HERE,'gold.jsonl'),'w') as f:
        for r in records: f.write(json.dumps(r)+"\n")
    with open(os.path.join(HERE,'ref_results.json'),'w') as f:
        json.dump(results, f, indent=1)
    nerr = sum(1 for v in results.values() if v['error'])
    print(f"Wrote gold.jsonl ({len(records)} records) and ref_results.json")
    print(f"Reference errors: {nerr}")
    for k,v in results.items():
        if v['error']:
            print(f"  Q{k}: ERROR {v['error']}")
    return 0

if __name__ == '__main__':
    sys.exit(main())
