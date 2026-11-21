#!/usr/bin/env python3
"""SQL-skeleton classification schema + constraint rules for GLiNER2.5-Decide.

Two schema builds:
  build_schema(use_constraints=True)  -> ClassificationSchema for Classifier.classify()
  build_schema(use_constraints=False) -> same tasks, no constraints (ablation A1)

Heads (task -> role):
  select_cols: multi-label, which columns to return
  filter_col / filter_op: single-label, the WHERE target + operator
  agg / group_by: aggregation + grouping
  join: multi-label; "songs_singers" = join songs<->singers on song_id
  order / limit: sort + row cap
"""
from gliner2.classification import ClassificationSchema, Classifier
from gliner2.classification import constraints as C

SELECT_LABELS = ["singer", "title", "date", "movie_name", "release_id"]
FILTER_COLS   = ["none", "title", "movie_name", "singer", "year", "date"]
FILTER_OPS    = ["none", "eq", "like", "year_eq", "between"]
AGG_LABELS    = ["none", "count", "min", "max"]
GROUP_LABELS  = ["none", "singer", "title", "year", "movie_name"]
JOIN_LABELS   = ["songs_singers"]
ORDER_LABELS  = ["none", "date_asc", "date_desc", "count_desc", "name_asc"]
LIMIT_LABELS  = ["none", "1", "2", "3", "5", "10"]


# A2 ablation: human-readable label descriptions (passed as {label: desc} maps,
# which GLiNER2 compiles into "[DESC]" schema tokens at inference).
LABEL_DESCRIPTIONS = {
    "select_cols": {"singer": "performer name", "title": "song title",
                    "date": "release date", "movie_name": "film name",
                    "release_id": "album/release identifier"},
    "filter_col": {"none": "no WHERE filter", "title": "song title",
                   "movie_name": "film name", "singer": "performer name",
                   "year": "release year", "date": "release date"},
    "filter_op": {"none": "no operator", "eq": "exact match",
                  "like": "substring/pattern match", "year_eq": "exact year",
                  "between": "range"},
    "agg": {"none": "no aggregation", "count": "row count", "min": "minimum",
            "max": "maximum"},
    "group_by": {"none": "no grouping", "singer": "by performer",
                 "title": "by song title", "year": "by release year",
                 "movie_name": "by film"},
    "join": {"songs_singers": "join songs with singers on song_id"},
    "order": {"none": "no ordering", "date_asc": "by date ascending",
              "date_desc": "by date descending", "count_desc": "by count descending",
              "name_asc": "by name ascending"},
    "limit": {"none": "no row limit", "1": "top 1 row", "2": "top 2 rows",
              "3": "top 3 rows", "5": "top 5 rows", "10": "top 10 rows"},
}


def build_schema(use_constraints: bool = True, use_descriptions: bool = False):
    def labels(head, base):
        if not use_descriptions:
            return base
        return {lab: LABEL_DESCRIPTIONS[head][lab] for lab in base}

    s = ClassificationSchema()
    s.multi("select_cols", labels("select_cols", SELECT_LABELS), max_labels=3)
    s.single("filter_col", labels("filter_col", FILTER_COLS))
    s.single("filter_op", labels("filter_op", FILTER_OPS))
    s.single("agg", labels("agg", AGG_LABELS))
    s.single("group_by", labels("group_by", GROUP_LABELS))
    s.multi("join", labels("join", JOIN_LABELS), max_labels=1)
    s.single("order", labels("order", ORDER_LABELS))
    s.single("limit", labels("limit", LIMIT_LABELS))
    if use_constraints:
        # A1-redo: every implication below is validated against the scaled gold
        # corpus by check_constraints.py (0 violations; vacuous ones dropped —
        # the old singer->join rules contradicted 130 gold entries because
        # singer-only queries like "how many singers collaborated" need no
        # songs<->singers join, and the old like->single-col rules forced
        # singer/title/movie_name even though gold uses LIKE on year).
        s.constrain(
            # ordering by count requires a count aggregation
            C.implies(("order", "count_desc"), ("agg", "count")),
            # grouping by year is always a per-year count
            C.implies(("group_by", "year"), ("agg", "count")),
            # LIKE only ever filters a text/year column (never singer/date)
            C.implies(("filter_op", "like"),
                      C.any_of(("filter_col", "title"),
                               ("filter_col", "movie_name"),
                               ("filter_col", "year"))),
            C.implies(("filter_op", "year_eq"), ("filter_col", "year")),
            # year filters only ever use LIKE (decade patterns) or year_eq
            C.implies(("filter_col", "year"),
                      C.any_of(("filter_op", "like"), ("filter_op", "year_eq"))),
        )
    return s


def build_classifier(map_location="cpu"):
    return Classifier.from_pretrained("fastino/GLiNER2.5-Decide",
                                      map_location=map_location)
