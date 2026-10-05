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
ORDER_LABELS  = ["none", "date_asc", "date_desc", "count_desc"]
LIMIT_LABELS  = ["none", "1", "2", "3", "5", "10"]


def build_schema(use_constraints: bool = True):
    s = ClassificationSchema()
    s.multi("select_cols", SELECT_LABELS, max_labels=3)
    s.single("filter_col", FILTER_COLS)
    s.single("filter_op", FILTER_OPS)
    s.single("agg", AGG_LABELS)
    s.single("group_by", GROUP_LABELS)
    s.multi("join", JOIN_LABELS, max_labels=1)
    s.single("order", ORDER_LABELS)
    s.single("limit", LIMIT_LABELS)
    if use_constraints:
        s.constrain(
            # singer in select or filter requires the join
            C.implies(("select_cols", "singer"), ("join", "songs_singers")),
            C.implies(("filter_col", "singer"), ("join", "songs_singers")),
            # a filter/aggregation generally implies no join needed unless above
            C.excludes(("filter_col", "singer"), ("filter_col", "year")),
            # count_desc ordering requires a count aggregation
            C.implies(("order", "count_desc"), ("agg", "count")),
            C.implies(("group_by", "singer"), ("join", "songs_singers")),
            C.implies(("group_by", "year"), ("agg", "count")),
            # filter with an operator requires a filter column
            C.implies(("filter_op", "like"), ("filter_col", "singer")),
            C.implies(("filter_op", "like"), ("filter_col", "title")),
            C.implies(("filter_op", "like"), ("filter_col", "movie_name")),
            C.implies(("filter_op", "year_eq"), ("filter_col", "year")),
            C.implies(("filter_op", "eq"), ("filter_col", "movie_name")),
            C.implies(("filter_op", "eq"), ("filter_col", "title")),
        )
    return s


def build_classifier(map_location="cpu"):
    return Classifier.from_pretrained("fastino/GLiNER2.5-Decide",
                                      map_location=map_location)
