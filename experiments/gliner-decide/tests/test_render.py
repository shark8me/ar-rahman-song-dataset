""""Unit tests for the SQL skeleton renderer (test render.py, no model needed)."""
import os, sys, sqlite3
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from render import render, extract_values, _FuzzyDict

DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'arr.db')


def conn():
    return sqlite3.connect(DB)


def fdict(c):
    return _FuzzyDict(c)


def test_count_no_filter():
    c = conn(); fd = fdict(c)
    sql = render({"select_cols": ["movie_name"], "filter_col": ["none"],
                  "filter_op": ["none"], "agg": ["count"], "group_by": ["none"],
                  "join": [], "order": ["none"], "limit": ["none"]}, {}, fd)
    assert "COUNT(DISTINCT s1.movie_name)" in sql, sql
    assert c.execute(sql).fetchone()[0] == 307
    c.close()

def test_join_select_singer():
    c = conn(); fd = fdict(c)
    sql = render({"select_cols": ["singer"], "filter_col": ["title"], "filter_op": ["like"],
                  "agg": ["none"], "group_by": ["none"], "join": ["songs_singers"],
                  "order": ["none"], "limit": ["none"]},
                 {"work_name": "Aaromale"}, fd)
    assert "JOIN singers" in sql, sql
    rows = c.execute(sql).fetchall()
    assert len(rows) == 2, rows
    c.close()

def test_order_limit_first_album():
    c = conn(); fd = fdict(c)
    sql = render({"select_cols": ["date"], "filter_col": ["none"], "filter_op": ["none"],
                  "agg": ["min"], "group_by": ["none"], "join": [],
                  "order": ["none"], "limit": ["1"]}, {}, fd)
    assert c.execute(sql).fetchone()[0] == "1991-01-01"
    c.close()

def test_year_eq_filter():
    c = conn(); fd = fdict(c)
    sql = render({"select_cols": ["movie_name"], "filter_col": ["year"], "filter_op": ["year_eq"],
                  "agg": ["count"], "group_by": ["none"], "join": [],
                  "order": ["none"], "limit": ["none"]}, {"year": "1999"}, fd)
    assert "LIKE '1999%'" in sql, sql
    assert c.execute(sql).fetchone()[0] == 16
    c.close()

def test_group_count_desc_top_n():
    c = conn(); fd = fdict(c)
    sql = render({"select_cols": ["singer"], "filter_col": ["none"], "filter_op": ["none"],
                  "agg": ["count"], "group_by": ["singer"], "join": ["songs_singers"],
                  "order": ["count_desc"], "limit": ["5"]}, {}, fd)
    rows = c.execute(sql).fetchall()
    assert len(rows) == 5, rows
    assert rows[0][0] == "A. R. Rahman", rows
    c.close()

def test_extract_values_year_n_singer():
    v = extract_values("List all songs Singers the singer Sid Sriram sang in 2016")
    assert v["year"] == "2016", v
    v2 = extract_values("Who are artist Rahman's top 5 collaborators?")
    assert v2["n"] == 5, v2
    v3 = extract_values("Who collaborated with singer Mathangi on song Aaha Tamizhamma?")
    assert v3.get("singer") == "Mathangi", v3
