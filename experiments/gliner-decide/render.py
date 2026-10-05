#!/usr/bin/env python3
"""SQL-skeleton renderer + literal-value extraction over the ARR small tables.

render(pred_heads, values) -> SQL string  (pure function; unit-testable)
extract_values(query, kind) -> {work_name, singer, year, n} literals
fuzzy_match(value, candidates) -> best DB candidate via difflib
"""
import re, sqlite3, os
from difflib import SequenceMatcher

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, 'arr.db')

# ---- value extraction (regex + keyword heuristics) -----------------------
YEAR_RE = re.compile(r'\b(19|20)\d{2}\b')
TOP_RE  = re.compile(r'\btop\s+(\d{1,2})\b', re.I)
# quoted spans are strong entity signals
QUOTED_RE = re.compile(r'"([^"]+)"|“([^”]+)”|‘([^’]+)’')

# heads -> DB column
SEL2COL = {"title": "s1.title", "date": "s1.date", "movie_name": "s1.movie_name",
           "singer": "s2.name", "release_id": "s1.release_id"}


class _FuzzyDict:
    """Cache of DISTINCT column values for fuzzy matching."""
    def __init__(self, conn):
        self.conn = conn
        self._cache = {}
    def candidates(self, col):
        if col in self._cache:
            return self._cache[col]
        sql = {"title": "SELECT DISTINCT title FROM songs",
               "movie_name": "SELECT DISTINCT movie_name FROM songs",
               "singer": "SELECT DISTINCT name FROM singers"}[col]
        vals = [r[0] for r in self.conn.execute(sql)]
        self._cache[col] = vals
        return vals


def extract_values(query, target_col=None):
    """Return {work_name, singer, year, n} with raw + fuzz-matched variants."""
    q = query
    vals = {}
    yr = YEAR_RE.search(q)
    if yr:
        vals["year"] = yr.group(0)
    m = TOP_RE.search(q)
    if m:
        vals["n"] = int(m.group(1))
    # quoted names
    mq = QUOTED_RE.search(q)
    if mq:
        name = next(x for x in mq.groups() if x)
        vals["work_name"] = name  # best-effort; caller matches against right column
    # singer literal: "singer X", "sang song X", "X collaborated", "song X"
    # patterns where a person surname / full name is mentioned
    for pat in (r'collaborated with (?:singer\s+)?([A-Z][A-Za-z .]+?) (?:on song|in|on|,|$|\?|\.)',
                r'singer\s+([A-Z][A-Za-z .]+?) (?:sang|in|on|,|$|\?|\.)',
                r'who sang for ([A-Z][A-Za-z .]+?)(?: in|\?|\.|$)'):
        m2 = re.search(pat, q)
        if m2:
            cand = m2.group(1).strip().rstrip('?,.]')
            if ' ' in cand or len(cand) > 4:
                vals.setdefault("singer", cand)
    # known singer names present verbatim
    for name in ("Sid Sriram", "Mathangi", "S. P. Balasubrahmanyam", "Hariharan",
                 "Sujatha", "Chitra", "Mohit Chauhan", "Benny Dayal"):
        if name in q:
            vals.setdefault("singer", name)
    return vals


def fuzzy_match(value, candidates, cutoff=0.62):
    """Return best fuzzy match of value against candidates, or None."""
    if not value:
        return None
    best, bestr = None, 0.0
    v = value.strip().lower()
    for cand in candidates:
        r = SequenceMatcher(None, v, cand.strip().lower()).ratio()
        if r > bestr:
            best, bestr = cand, r
    return best if bestr >= cutoff else None


def _filter_sql(filter_col, filter_op, values, fdict):
    """Build WHERE fragment; returns (fragment, used_values) and whether value was found."""
    if filter_col in (None, "none") or filter_op in (None, "none"):
        return "", {}
    col = {"year": "substr(s1.date,1,4)",
           "singer": "s2.name",
           "title": "s1.title",
           "movie_name": "s1.movie_name",
           "date": "s1.date"}.get(filter_col) or ("s1." + filter_col)
    # which literal applies
    if filter_col == "singer":
        lit_key = "singer"
    elif filter_col == "year":
        lit_key = "year"
    else:
        lit_key = "work_name"
    raw = values.get(lit_key)
    if not raw:
        return "", {}
    if filter_col == "year" and filter_op == "year_eq":
        return f" AND {col} LIKE '{raw}%'", {lit_key: raw}
    if filter_op == "eq":
        return f" AND {col} = '{raw}'", {lit_key: raw}
    # like: fuzzy-match raw to a real DB value for the target column
    cand_col = {"title": "title", "singer": "name", "movie_name": "movie_name"}.get(filter_col, filter_col)
    merged = None
    for c in fdict.candidates(cand_col):
        if raw.strip().lower() in c.lower():
            merged = c
            break
    if merged is None:
        merged = fuzzy_match(raw, fdict.candidates(cand_col))
    if merged is None:
        # fall back to a substring LIKE on raw
        return f" AND {col} LIKE '%{raw}%'", {lit_key: raw}
    return f" AND {col} LIKE '%{merged}%'", {lit_key: merged}


def render(pred, values, fdict):
    """Render a predicted skeleton to SQL. pred is dict of task->labels (list)."""
    select = pred.get("select_cols", [])
    filter_col = (pred.get("filter_col") or ["none"])[0]
    filter_op = (pred.get("filter_op") or ["none"])[0]
    agg = (pred.get("agg") or ["none"])[0]
    group_by = (pred.get("group_by") or ["none"])[0]
    join = pred.get("join", [])
    order = (pred.get("order") or ["none"])[0]
    limit = (pred.get("limit") or ["none"])[0]

    need_join = ("songs_singers" in join) or ("singer" in select) or (filter_col == "singer") or (group_by == "singer")
    from_join = " FROM songs s1 JOIN singers s2 ON s1.song_id = s2.song_id" if need_join else " FROM songs s1"

    where, used = _filter_sql(filter_col, filter_op, values, fdict)
    where = (" WHERE " + where[5:]) if where.startswith(" AND ") else where

    # select clause
    if agg != "none" and group_by == "none":
        if agg == "count":
            col = SEL2COL.get(select[0], "s1." + select[0]) if select else "s1.song_id"
            sel = f"SELECT COUNT(DISTINCT {col})"
        elif agg == "min":
            sel = "SELECT MIN(s1.date)"
        elif agg == "max":
            sel = "SELECT MAX(s1.date)"
        else:
            sel = "SELECT " + ", ".join(SEL2COL.get(c, c) for c in select)
    elif group_by != "none":
        gcol = SEL2COL.get(group_by, "s1." + group_by) if group_by in SEL2COL else (
            "substr(s1.date,1,4)" if group_by == "year" else "s1." + group_by)
        if agg == "count":
            sel = f"SELECT {gcol}, COUNT(*) AS cnt"
        else:
            scols = ", ".join(SEL2COL.get(c, c) for c in select if c != group_by)
            sel = f"SELECT {gcol}" + (f", {scols}" if scols else "")
    else:
        col = SEL2COL.get(select[0], "s1." + select[0]) if select else "s1.song_id"
        sel = f"SELECT DISTINCT {col}"

    # group by
    gb = ""
    if group_by != "none":
        gcol = SEL2COL.get(group_by, "s1." + group_by) if group_by in SEL2COL else (
            "substr(s1.date,1,4)" if group_by == "year" else "s1." + group_by)
        gb = f" GROUP BY {gcol}"

    # order
    ordr = ""
    if order == "date_asc":
        ordr = " ORDER BY MIN(s1.date) ASC" if (agg == "min" or group_by != "none") else " ORDER BY s1.date ASC"
    elif order == "date_desc":
        ordr = " ORDER BY MAX(s1.date) DESC" if (agg == "max" or group_by != "none") else " ORDER BY s1.date DESC"
    elif order == "count_desc":
        gkey = gb.replace(" GROUP BY ", "") if gb else ""
        ordr = f" ORDER BY cnt DESC" + (f", {gkey} ASC" if gkey else "")

    lim = f" LIMIT {limit}" if limit != "none" else ""

    sql = sel + from_join + where + gb + ordr + lim
    return sql
