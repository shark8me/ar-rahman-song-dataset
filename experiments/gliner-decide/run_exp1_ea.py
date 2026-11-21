#!/usr/bin/env python3
"""EA (execution accuracy) scoring for the multi-composer DBs.

Generalizes predict.py's Rahman-only render/extract path to every composer:

  - composer registry: Rahman lives in experiments/gliner-decide/arr.db
    (songs/singers: title/date/movie_name/name); the other five use root
    arr.db tables songs_<pfx>/singers_<pfx> (song/release_date/movie/singer).
  - values are extracted from the query text (regex + window candidates)
    and bound to the filter column via substring/fuzzy match against that
    composer's real column values -- no hard-coded singer name list.
  - SQL is rendered with per-composer table/column names, executed, and the
    result is compared to the verified gold answer under the same shape
    semantics generate_qa.py used (count/scalar/name/list).

Composer routing uses the gold `composer` field (oracle routing; the skeleton
cannot express it -- noted in the report). Cross-composer ("all") questions
need a UNION ALL over six tables and are out of the skeleton's expressiveness:
they are skipped and reported separately.

Usage:
  run_exp1_ea.py --pred results/exp1/pred_exp1_a1redo.json            # EA from predicted heads
  run_exp1_ea.py --gold-heads --gold-values                           # sanity: renderer check vs ref_sql
  run_exp1_ea.py --pred ... --no-fuzzy                                # A3: fuzzy matching off
"""
import argparse, json, os, re, sqlite3, sys, time
from collections import Counter
from difflib import SequenceMatcher

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))  # repo root

COMPOSERS = {
    "arr": {"name": "A. R. Rahman", "kind": "arr",
            "db": os.path.join(HERE, "arr.db")},
    "ila": {"name": "Ilaiyaraaja", "kind": "pfx", "db": os.path.join(ROOT, "arr.db")},
    "pri": {"name": "Pritam", "kind": "pfx", "db": os.path.join(ROOT, "arr.db")},
    "sel": {"name": "Shankar-Ehsaan-Loy", "kind": "pfx", "db": os.path.join(ROOT, "arr.db")},
    "sus": {"name": "Sushin Shyam", "kind": "pfx", "db": os.path.join(ROOT, "arr.db")},
    "vsh": {"name": "Vishal-Shekhar", "kind": "pfx", "db": os.path.join(ROOT, "arr.db")},
}

# composer-name mentions that must never be bound as filter values
COMPOSER_ALIASES = {
    "a. r. rahman", "rahman", "arr", "ilaiyaraaja", "pritam",
    "shankar-ehsaan-loy", "shankar ehsaan loy", "sushin shyam",
    "vishal-shekhar", "vishal shekhar",
}

STOPWORDS = {"how", "which", "what", "when", "who", "where", "list", "count",
             "find", "give", "name", "in", "across", "did", "has", "was", "is",
             "the", "a", "an", "on", "for", "from", "of", "by", "and", "s",
             "many", "much", "songs", "movies", "albums", "films", "tracks",
             "singers", "year", "years", "all", "were", "performed", ""}

# "first appearance" template family: fixed SQL shape (MIN of year over the
# singer's credits). The model never learned this mapping reliably (34/60 EA
# failures were answered without MIN; teaching it via synth data regressed
# other heads), so the scorer resolves the family deterministically from the
# query surface -- the same phrases generate_qa.py uses, unique to this family.
FIRST_RE = re.compile(r"first sing|first work with|first work as|first appear"
                      r"|first collaborate|start singing|first recording", re.I)

FIRST_FAMILY_HEADS = {"select_cols": ["date"], "filter_col": ["none"],
                      "filter_op": ["none"], "agg": ["min"],
                      "group_by": ["none"], "join": ["songs_singers"],
                      "order": ["none"], "limit": ["none"]}

YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
TOP_RE = re.compile(r"\btop\s+(\d{1,2})\b", re.I)
QUOTED_RE = re.compile(r'"([^"]+)"|"([^"]+)"|\'([^\']+)\'')
CAP_RUN_RE = re.compile(r"[A-Z][\w'’\-]*[\w'’\-]*(?: [A-Z][\w'’\-]*[\w'’\-]*)*")


# ---- per-composer physical columns ---------------------------------------
def phys(cname):
    """cname -> table/column map. arr uses legacy names; others use <pfx> tables."""
    if cname == "arr":
        return {"songs": "songs", "singers": "singers", "song": "title",
                "date": "date", "movie": "movie_name", "singer": "name",
                "release_id": "release_id"}
    return {"songs": "songs_" + cname, "singers": "singers_" + cname,
            "song": "song", "date": "release_date", "movie": "movie",
            "singer": "singer", "release_id": "release_id"}


HEAD2KEY = {"title": "song", "date": "date", "movie_name": "movie",
            "singer": "singer", "release_id": "release_id"}


def col(cname, head):
    """canonical head -> physical column expression (year handled separately)"""
    return phys(cname)[HEAD2KEY[head]]


def year_expr(cname):
    return f"substr(s1.{phys(cname)['date']},1,4)"


# ---- candidate extraction -------------------------------------------------
def clean_span(s):
    s = re.sub(r"['’]s\b", "", s).strip(" ?,.:!;—-")
    return s.strip()


def candidate_spans(query):
    """Entity-ish spans from the query text: quoted, then capitalized runs,
    then all short word windows (catches lowercase DB spellings)."""
    spans = []
    mq = QUOTED_RE.search(query)
    if mq:
        spans.append(clean_span(next(x for x in mq.groups() if x)))
    for run in CAP_RUN_RE.findall(query):
        s = clean_span(run)
        if s and " " in s:
            spans.append(s)
    words = query.split()
    n = len(words)
    for i in range(n):
        for ln in range(1, 6):
            if i + ln > n:
                break
            w = [re.sub(r"['’]s\b", "", x).strip(" ,.?:!;\"'()[]") for x in words[i:i + ln]]
            if w[0].lower() in STOPWORDS or w[-1].lower() in STOPWORDS:
                continue
            if any(x.lower() in COMPOSER_ALIASES for x in w):
                continue
            s = " ".join(x for x in w if x)
            if len(s) >= 2 and s not in spans:
                spans.append(s)
    # 'Title (Qualifier)' spans: the parenthetical qualifier is part of the
    # entity name (variant mixes/versions). The word windows strip parens and
    # cap at 5 words, so 'Kal Ho Naa Ho (The Heat Beat Mix)' would otherwise
    # never form a span and the plain-title sibling wins the exact-match tier.
    for m in re.finditer(r"\b[A-Z][\w'’\-]*(?: [A-Z][\w'’\-]*)*\s*\(([^()]+)\)",
                         query):
        s = clean_span(m.group(0))
        if s and s not in spans:
            spans.append(s)
    # dedupe, keep order
    out, seen = [], set()
    for s in spans:
        k = s.lower()
        if k not in seen:
            seen.add(k)
            out.append(s)
    return out


def bind_singer_exact(query, singer_values):
    """Exact (case-insensitive) singer mention in the query, longest span wins.
    Used because gold skeleton heads omit the singer filter on singer-perspective
    templates (it lives only in ref_sql/values) -- EA must recover it."""
    best, best_val = None, None
    for span in candidate_spans(query):
        sl = span.lower()
        ns = _norm(span)
        for v in singer_values:
            # exact, or punctuation-normalized exact: word windows strip dots,
            # so 'G. V. Prakash Kumar' arrives as 'G V Prakash Kumar' and must
            # still match the DB form. Return the DB value, not the span --
            # the LIKE must match the stored spelling.
            if v and (v.lower() == sl or (ns and _norm(v) == ns)) \
                    and (best is None or len(span) > len(best)):
                best, best_val = span, v
    return best_val


def _norm(s):
    """Punctuation/case-insensitive key: 'Lovely (house remix)' == 'Lovely house remix'."""
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def score_span(span, target_values, fuzzy=True, cutoff=0.62):
    """Score one span against DB values. Returns (value, score); (None, 0.0) if no hit.
    Score: exact (3.0) > punctuation-normalized exact (3.0 -- parenthetical variants
    count as exact, same tier) > db-contains-span (2.0) > word-aligned
    span-contains-db-value (1.5) > fuzzy (<1.0). The +len(span)/1000 bonus makes a
    longer same-tier span win, which is the span-length tiebreak."""
    best, best_score = None, 0.0
    sl = span.lower()
    nl = _norm(span)
    for v in target_values:
        vl = v.lower()
        if sl == vl:
            score = 3.0 + len(span) / 1000.0
        elif nl and _norm(v) == nl:
            score = 3.0 + len(span) / 1000.0
        elif vl and sl in vl:
            score = 2.0 + len(span) / 1000.0
        elif vl and re.search(rf"(?<!\w){re.escape(vl)}(?!\w)", sl):
            # word-aligned containment only: 'Ale' must never bind out of
            # 'Dhoom Machale' (a plain substring that crosses token bounds)
            score = 1.5
        elif fuzzy:
            score = SequenceMatcher(None, sl, vl).ratio()
            if score < cutoff:
                continue
        else:
            continue
        if score > best_score:
            best, best_score = v, score
    return best, best_score


def bind_value(query, target_values, fuzzy=True, cutoff=0.62):
    """Best DB value over all candidate spans of the query.
    Rank by score first; span length only breaks ties within the same tier
    (the +len/1000 bonus in score_span encodes this). A longer span with a
    mere containment/fuzzy match must never overwrite a shorter exact match."""
    best, best_score = None, 0.0
    for span in candidate_spans(query):
        v, s = score_span(span, target_values, fuzzy=fuzzy, cutoff=cutoff)
        if v is not None and s > best_score:
            best, best_score = v, s
    return best


# ---- rendering ------------------------------------------------------------
def filter_sql(c, cname, filter_col, filter_op, values, fdict, fuzzy=True):
    if filter_col in (None, "none") or filter_op in (None, "none"):
        return "", {}
    if filter_col == "year":
        cexpr = year_expr(cname)
        yr = values.get("year")
        if not yr:
            return "", {}
        return f" AND {cexpr} LIKE '{yr}%'", {"year": yr}
    s_alias = "s2" if filter_col == "singer" else "s1"
    cexpr = f"{s_alias}.{col(cname, filter_col)}"
    raw = values.get("work_name")
    if not raw:
        return "", {}
    key = HEAD2KEY[filter_col]
    if filter_op == "eq":
        return f" AND {cexpr} = '{raw}'", {"work_name": raw}
    # like: bind raw to a real DB value for this column
    merged = score_span(raw, fdict.candidates(cname, key), fuzzy=fuzzy)[0] \
        if key != "date" else raw
    if merged is None:
        return f" AND {cexpr} LIKE '%{raw}%'", {"work_name": raw}
    return f" AND {cexpr} LIKE '%{merged}%'", {"work_name": merged}


class FuzzyDict:
    """Cache of DISTINCT column values per composer for value binding."""
    def __init__(self):
        self._cache = {}
        self._conns = {}

    def conn(self, cname):
        if cname not in self._conns:
            self._conns[cname] = sqlite3.connect(COMPOSERS[cname]["db"])
        return self._conns[cname]

    def candidates(self, cname, key):
        ck = (cname, key)
        if ck in self._cache:
            return self._cache[ck]
        c = COMPOSERS[cname]
        p = phys(cname)
        tbl, cl = (p["songs"], p[key]) if key in ("song", "movie", "date", "release_id") \
            else (p["singers"], p["singer"])
        vals = [r[0] for r in self.conn(cname).execute(
            f"SELECT DISTINCT {cl} FROM {tbl} WHERE {cl} IS NOT NULL")]
        self._cache[ck] = vals
        return vals


def _as_list(x):
    """Gold heads store single-label heads as strings; preds store lists."""
    if isinstance(x, str):
        return [x]
    return list(x or [])


def render(c, cname, pred, values, fdict, fuzzy=True, singer_hint=None):
    """Render predicted skeleton heads to SQL for composer c."""
    select = _as_list(pred.get("select_cols", []))
    filter_col = _as_list(pred.get("filter_col") or ["none"])[0] or "none"
    filter_op = _as_list(pred.get("filter_op") or ["none"])[0] or "none"
    agg = _as_list(pred.get("agg") or ["none"])[0] or "none"
    group_by = _as_list(pred.get("group_by") or ["none"])[0] or "none"
    join = _as_list(pred.get("join", []))
    order = _as_list(pred.get("order") or ["none"])[0] or "none"
    limit = _as_list(pred.get("limit") or ["none"])[0] or "none"

    p = phys(cname)
    need_join = ("songs_singers" in join) or ("singer" in select) \
        or (filter_col == "singer") or (group_by == "singer") \
        or (singer_hint is not None)
    from_join = (f" FROM {p['songs']} s1 JOIN {p['singers']} s2"
                 " ON s1.song_id = s2.song_id") if need_join \
        else f" FROM {p['songs']} s1"

    where, used = filter_sql(c, cname, filter_col, filter_op, values, fdict, fuzzy)
    if singer_hint and filter_col != "singer":
        where += f" AND s2.{phys(cname)['singer']} LIKE '%{singer_hint}%'"
    where = (" WHERE " + where[5:]) if where.startswith(" AND ") else where

    def selcol(head):
        if head == "year":
            return year_expr(cname)
        alias = "s2" if head == "singer" else "s1"
        return f"{alias}.{col(cname, head)}"

    # select clause
    if agg != "none" and group_by == "none":
        if agg == "count":
            cexpr = selcol(select[0]) if select else "s1.song_id"
            sel = f"SELECT COUNT(DISTINCT {cexpr})"
        elif agg == "min":
            minexpr = year_expr(cname) if (singer_hint or filter_col == "singer") \
                else f"s1.{p['date']}"
            sel = f"SELECT MIN({minexpr})"
        elif agg == "max":
            maxexpr = year_expr(cname) if (singer_hint or filter_col == "singer") \
                else f"s1.{p['date']}"
            sel = f"SELECT MAX({maxexpr})"
        else:
            sel = "SELECT " + ", ".join(selcol(x) for x in select)
    elif group_by != "none":
        gcol = year_expr(cname) if group_by == "year" else selcol(group_by)
        if agg == "count":
            sel = f"SELECT {gcol}, COUNT(*) AS cnt"
        else:
            scols = ", ".join(selcol(x) for x in select if x != group_by)
            sel = f"SELECT {gcol}" + (f", {scols}" if scols else "")
    else:
        cexpr = selcol(select[0]) if select else "s1.song_id"
        sel = f"SELECT DISTINCT {cexpr}"

    gb = ""
    if group_by != "none":
        gcol = year_expr(cname) if group_by == "year" else selcol(group_by)
        gb = f" GROUP BY {gcol}"

    if order in ("date_asc", "date_desc") and agg == "none" and group_by == "none" \
            and len(select) == 1 and select[0] not in ("date", "year"):
        group_by = select[0]  # ORDER BY MIN(date) over grouped albums

    ordr = ""
    if order == "date_asc":
        ordr = (f" ORDER BY MIN(s1.{p['date']}) ASC"
                if (agg == "min" or group_by != "none")
                else f" ORDER BY s1.{p['date']} ASC")
    elif order == "date_desc":
        ordr = (f" ORDER BY MAX(s1.{p['date']}) DESC"
                if (agg == "max" or group_by != "none")
                else f" ORDER BY s1.{p['date']} DESC")
    elif order == "name_asc":
        ocol = selcol(select[0]) if select else f"s1.{p['song']}"
        ordr = f" ORDER BY {ocol} ASC"
    elif order == "count_desc":
        gkey = gb.replace(" GROUP BY ", "") if gb else ""
        ordr = " ORDER BY cnt DESC" + (f", {gkey} ASC" if gkey else "")

    lim = f" LIMIT {limit}" if limit != "none" else ""
    return sel + from_join + where + gb + ordr + lim


# ---- extraction + answer formatting ---------------------------------------
def extract_values(query):
    vals = {}
    yr = YEAR_RE.search(query)
    if yr:
        vals["year"] = yr.group(0)
    m = TOP_RE.search(query)
    if m:
        vals["n"] = int(m.group(1))
    mq = QUOTED_RE.search(query)
    if mq:
        vals.setdefault("work_name", clean_span(next(x for x in mq.groups() if x)))
    return vals


def fmt_answer(rows, shape):
    if shape in ("count", "scalar", "name"):
        return rows[0][0] if rows else None
    if shape == "list":
        return sorted({str(r[0]) for r in rows if r[0] is not None})
    return rows


def answers_match(pred, gold, shape):
    if pred is None:
        return False
    if shape in ("count",):
        try:
            return int(pred) == int(gold)
        except (TypeError, ValueError):
            return str(pred) == str(gold)
    if shape in ("scalar", "name"):
        return str(pred).strip() == str(gold).strip()
    if shape == "list":
        return sorted({str(x) for x in pred}) == sorted({str(x) for x in gold})
    return False


# ---- main ------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", default=None,
                    help="pred_exp1_*.json with predicted heads (default: the "
                         "a1redo run)")
    ap.add_argument("--adapter", default=None, help="classify fresh with this LoRA adapter")
    ap.add_argument("--gold-heads", action="store_true",
                    help="use gold heads instead of predictions (renderer sanity check)")
    ap.add_argument("--gold-values", action="store_true",
                    help="use gold slot values instead of extracted (oracle-value check)")
    ap.add_argument("--no-fuzzy", action="store_true", help="A3: disable fuzzy binding")
    ap.add_argument("--data", default=os.path.join(ROOT, "experiments", "multi-composer-qa",
                                                   "gold-multi-scaled.jsonl"))
    ap.add_argument("--split", default=os.path.join(ROOT, "experiments", "multi-composer-qa",
                                                    "results", "exp1_split.json"))
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    gold = {g["idx"]: g for g in (json.loads(l) for l in open(a.data))}
    split = json.load(open(a.split))
    test = split["test"]

    preds = {}
    if a.pred:
        preds = json.load(open(a.pred))
    elif a.adapter:
        from run_exp1_eval import build_finetuned
        from query_schema import build_schema
        clf = build_finetuned(a.adapter)
        schema = build_schema()
        t0 = time.time()
        for idx in test:
            r = clf.classify(gold[idx]["query"], schema)
            preds[str(idx)] = {"heads": {t: list(r.tasks[t].labels)
                                         if t in r.tasks else []
                                         for t in schema.task_order}}
        print(f"classified {len(test)} queries in {time.time()-t0:.0f}s")

    fdict = FuzzyDict()
    conns = {cn: fdict.conn(cn) for cn in COMPOSERS}

    eligible = [i for i in test if gold[i]["composer"] != "all"
                and gold[i].get("template_id") != "singer_top_year"]
    skipped_xc = [i for i in test if gold[i]["composer"] == "all"
                  or gold[i].get("template_id") == "singer_top_year"]

    rows_out = {}
    stats = {"ea": 0, "error": 0, "no_bind": 0, "no_filter_missing": 0, "n": 0}
    by_composer = {}
    by_shape = {}
    t0 = time.time()
    for idx in eligible:
        g = gold[idx]
        cname = g["composer"]
        c = COMPOSERS[cname]
        heads = g["heads"] if a.gold_heads else preds.get(str(idx), {}).get("heads")
        if heads is None:
            continue
        if FIRST_RE.search(g["query"]):
            heads = dict(FIRST_FAMILY_HEADS)  # first-family: canonical shape
        fcol = (heads.get("filter_col") or ["none"])
        fcol = fcol[0] if isinstance(fcol, list) else fcol
        fop = (heads.get("filter_op") or ["none"])
        fop = fop[0] if isinstance(fop, list) else fop
        # values
        if a.gold_values:
            vals = dict(g.get("values") or {})
            vals["work_name"] = vals.get("song") or vals.get("movie") \
                or vals.get("singer")
        else:
            vals = extract_values(g["query"])
            if fcol not in (None, "none", "year") and "work_name" not in vals:
                bound = bind_value(g["query"],
                                   fdict.candidates(cname, HEAD2KEY[fcol]),
                                   fuzzy=not a.no_fuzzy)
                if bound:
                    vals["work_name"] = bound
        if a.gold_values:
            hint = vals.get("singer")
        else:
            hint = bind_singer_exact(g["query"], fdict.candidates(cname, "singer"))
        sql = render(c, cname, heads, vals, fdict, fuzzy=not a.no_fuzzy,
                     singer_hint=hint)
        try:
            rows = conns[cname].execute(sql).fetchall()
            out = fmt_answer(rows, g.get("answer_shape", "list"))
            err = None
        except Exception as e:
            out, err = None, f"{type(e).__name__}: {e}"
        ok = answers_match(out, g.get("answer"), g.get("answer_shape", "list"))
        stats["n"] += 1
        if ok:
            stats["ea"] += 1
        elif err:
            stats["error"] += 1
        elif fcol != "none" and not vals.get("work_name") and not vals.get("year"):
            stats["no_bind"] += 1
        bc = by_composer.setdefault(cname, [0, 0])
        bc[0] += ok
        bc[1] += 1
        bs = by_shape.setdefault(g.get("answer_shape", "?"), [0, 0])
        bs[0] += ok
        bs[1] += 1
        rows_out[str(idx)] = {"query": g["query"], "composer": cname,
                              "sql": sql, "values": vals, "answer": out,
                              "gold": g.get("answer"), "ok": ok, "error": err}

    n = stats["n"]
    print(f"EA-eligible test entries: {n} (skipped {len(skipped_xc)} cross-composer)")
    print(f"EA: {stats['ea']}/{n} = {stats['ea']/n:.3f}")
    print(f"  exec errors: {stats['error']}   unbound filter values: {stats['no_bind']}")
    print("by composer:")
    for cn in sorted(by_composer, key=lambda x: -by_composer[x][1]):
        ok, t = by_composer[cn]
        print(f"  {cn}: {ok}/{t} = {ok/t:.3f}")
    print("by answer shape:")
    for sh in sorted(by_shape, key=lambda x: -by_shape[x][1]):
        ok, t = by_shape[sh]
        print(f"  {sh}: {ok}/{t} = {ok/t:.3f}")
    print(f"time: {time.time()-t0:.0f}s")

    out_path = a.out or os.path.join(
        HERE, "results", "exp1",
        ("ea_" + os.path.basename(a.pred).replace("pred_", "").replace(".json", "")
         if a.pred else "ea_fresh") + ("_a3nofuzzy" if a.no_fuzzy else "") + ".json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump({"_meta": {"ea": stats["ea"], "n": n, "no_fuzzy": a.no_fuzzy,
                             "gold_heads": a.gold_heads, "gold_values": a.gold_values,
                             "pred": a.pred, "skipped_xc": len(skipped_xc)},
                   "rows": rows_out}, f, indent=1, ensure_ascii=False)
    print("wrote", out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
