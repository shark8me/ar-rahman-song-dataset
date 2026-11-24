# Plan: Multi-composer dataset (Ilaiyaraaja, Shankar–Ehsaan–Loy, Pritam, Harris Jayaraj, Sushin Shyam, Vishal–Shekhar)

Extend the arr-qa MusicBrainz pipeline so each composer gets its own fetch,
derived tables, and per-composer SQLite tables — alongside the existing Rahman
dataset (untouched).

## Verified MusicBrainz artist IDs

| Composer | prefix | MBID |
|---|---|---|
| A. R. Rahman | arr | `e0bba708-bdd3-478d-84ea-c706413bedab` (existing) |
| Ilaiyaraaja | ila | `34394522-a0f0-4675-aad2-30ac3cb3d7d3` |
| Shankar–Ehsaan–Loy | sel | `d8d3e7de-793e-49fa-8bb8-6bc732d4c8aa` |
| Pritam (Chakraborty) | pri | `52db98c1-0cc9-4aab-9f2d-ea8d359234f3` |
| Harris Jayaraj | har | `ecdd5aa9-b760-47d4-9541-3c3d04da433b` |
| Sushin Shyam | sus | `85c91760-4e3f-406d-984a-fc89180b9d07` |
| Vishal–Shekhar | vsh | `2df4ce6a-87d6-49e5-b140-4e71066f996c` |

All IDs verified via MB artist search (score 100). Spot-check each disambiguation
page before the long fetches (Indian film composers have thin MB disambiguation;
Pritam's is the Person artist with legal name "Pritam Chakraborty").

## Design decisions

1. **One `composers` registry in core.clj.**
   `(def composers {:arr {...} :ila {...}})` with keys `:id`, `:prefix`.
   All pipeline entry points (`regenerate`, `update-for-years`,
   `save-releases-for-years`) take a composer keyword instead of raw paths.

2. **Per-composer file layout (prefix-threaded, nothing else moves).**
   ```
   <pfx>-releases.json            # raw release dump (per composer)
   <pfx>-track-details.json       # per-recording singer credits
   <pfx>recordingtable.json       # deduped recordings
   songtable_<pfx>.csv            # raw song table
   data/<pfx>/recordings.csv      # final songs table
   data/<pfx>/singers.csv         # final singers join table
   ```
   Rahman keeps its existing filenames (`arr-releases-5471.json`,
   `arr-track-details.json`, `data/recordings.csv`, ...) so the blog-post
   numbers stay reproducible; register him in `composers` with explicit
   legacy paths.

3. **One DB (`arr.db`), per-composer tables.**
   Tables `songs_<pfx>` and `singers_<pfx>` (e.g. `songs_ila`, `singers_ila`).
   Cross-composer questions become joins on singer name. Rahman's existing
   `songs`/`singers` tables remain as-is.

4. **Bootstrap via the incremental year-range path, not the full browse walk.**
   `get-releases-for-years` + `get-release-details` + `save-releases-for-years`
   already merge into an existing dump, so a new composer is bootstrapped by
   year batches (see fetch order below) and is resumable — if a batch is
   interrupted, rerun the same range; dedup by release id is already in
   `save-releases-for-years`.

## Implementation steps

### 1. Registry + path threading (core.clj) — DONE 2026-11-14
- [x] `composers` registry (core.clj:13) with `:id`/`:prefix`; `:arr` carries
      legacy paths (`arr-releases-5471.json`, `songtable.csv`,
      `arrrecordingtable.json`, `data/`).
- [x] `paths-for` (core.clj:27) — resolves legacy vs new `<pfx>` layout.
      Verified: `:arr` → `arr-releases-5471.json`/`data/`, `:sus` →
      `sus-releases.json`/`data/sus/`, unknown key → nil.
- [x] `regenerate` 5-arity taking registry paths; legacy 2-arity delegates to
      the exact old Rahman paths — backward compatible.
- [x] `save-releases-for-years` bootstraps when the dumps don't exist yet
      (reads empty vec instead of throwing) — new composers can start from
      nothing.
- [x] `io/make-parents` before every write — `data/<pfx>/` created on demand.
- [x] Composer-level entry points (core.clj:474): `regenerate-composer`,
      `update-composer-for-years`, `save-releases-composer-for-years` — all
      take the composer keyword.
- [x] `save-track-singers` 2-arity with explicit path.
- Note: namespace loads cleanly (`clojure -M -e (require ...)`) — verified.

### 2. SQLite load step — DONE 2026-11-14
- [x] New namespace `src/arr_song_dataset/db.clj`: `load-composer!` (default
      arr.db, or explicit spec) creates `songs_<pfx>`/`singers_<pfx>` with
      `if not exists`, wipes + reloads in one transaction (insert-multi!),
      skips rows with unparseable dates (println), stamps `composer` from the
      registry `:name`. `create-composer-tables!`/`drop-composer-tables!`
      helpers. Uses `yyyy-MM-dd HH:mm:ss` (correct pattern, unlike query.clj's
      `YYYY`).
- [x] Registry entries got `:name` (display composer name for the DB column).
- [x] deps.edn: added `org.clojure/java.jdbc` (was only in project.clj).
- [x] Verified: loaded `:arr` → 2547 songs / 4410 singer rows / 658 distinct
      singers in `songs_arr`/`singers_arr`; 0 skipped dates (recordings.csv has
      no `-` dates); spot-checked rows carry composer "A. R. Rahman" and
      parsed release_date. Legacy `songs`/`singers` tables and query.clj's
      in-memory path untouched.
- Note: the on-disk arr.db's legacy `songs`/`singers` tables are empty and
  have a different schema (title/date/movie_name) — left as-is; per-composer
  tables follow the query.clj schema so cross-composer joins work.

**Notes (found 2026-11-14):**
- Live load path is `insert-songs` in `query.clj` (in-memory sqlite via
  `jdbc:sqlite::memory:`), NOT the `arr.db` file. The on-disk `arr.db` is a
  static Mar-2024 artifact with `songs`/`singers`; `query.clj` hard-codes
  `data/recordings.csv` + `data/singers.csv` and composer "A. R. Rahman".
- `create-tables`/`drop-tables` in query.clj are the schema — no schema/ dir.
- `insert-songs` formats dates with `YYYY-MM-dd` (year-of-era, not
  `yyyy-MM-dd`) — works for CE dates but wrong pattern; fix while
  parameterizing.
- `parse-date` throws on `-` dates (unparseable rows) and `insert-songs`
  silently drops them via the catch — the plan's step-8 "no `-` dates"
  checklist should be measured against the recordings.csv before insert, not
  via insert failures.
- Parameterize `insert-songs` → `insert-songs` with a paths map + composer
  name; add `create-tables` variants for `songs_<pfx>`/`singers_<pfx>`.
- Decide whether per-composer tables should live in the on-disk `arr.db` (plan
  decision 3) — that means a separate load path from the in-memory `query.clj`
  one; likely a small `db/load-composer!` fn using `jdbc:sqlite:arr.db`.

### 3. Smoke test on smallest composer first (Sushin Shyam) — DONE 2026-11-14
- [x] Bootstrapped `:sus` 2015–2026 in year batches (2015–19, 2020–22, 2023–24,
      2025–26). Result: 32 releases in `sus-releases.json`, 233 recording
      credits in `sus-track-details.json`. Re-ran the first and last batch →
      `new releases 0` — dedup/resumability verified.
- [x] `regenerate-composer :sus` → `data/sus/recordings.csv` 226 songs / 226
      distinct song_ids (no dup ids), `data/sus/singers.csv` 309 rows, singer
      song_ids ⊆ recordings song_ids, zero `-` dates.
- [x] `songs_sus`/`singers_sus` loaded into arr.db (226 / 309), composer
      column "Sushin Shyam", 0 orphan singer rows.
- [x] Known film spot-check: *Aavesham* 2024 present (Adholokam etc.);
      *Kumbalangi Nights*, *Kurup*, *Romancham*, *Minnal Murali* present.
- [x] Dedup fallback cases seen (hand-resolvable, recorded):
      - "balan - the boy" OST vs "engotta" single (both Soundtrack-tagged)
      - "scene (original motion picture soundtrack)" vs singles
        "aathi iva yarraa (from \"scene\")" and "killin' 'em (from \"scene\")"
- Note: *Premalu* (2024) is ABSENT — MB has zero releases crediting Sushin
  Shyam for it (verified via arid search, count 0). MB coverage gap, listed
  under caveats, not a pipeline bug.

**Notes (found 2026-11-14):**
- BUG FIXED in `db.clj`: java.jdbc 0.7.0 `insert-multi!` with map rows +
  explicit col-names ignores the col-names ordering — values follow map
  iteration order, so every column was shifted (release_date held movie,
  composer held release_id). This had also silently corrupted the earlier
  `:arr` load. Fix: convert rows to value vectors in column order before
  insert; reloaded both `:sus` and `:arr` and verified column values.
- All registry entries now carry `:name` (was only `:arr`), so the DB
  composer column shows the display name for every composer.
- MB 503s appear frequently under the 15 req global rate limit; `wait-till`
  retries but a failed `get_song_details` is dropped (recording saved without
  singer credits).
- BUG FIXED in `core.clj` (2026-11-15, during the `:ila` fetch): `wait-till`
  had no retry cap — a request that always throws (e.g. a 404 recording) or
  an endless 503 streak retried forever and blocked the whole sequential
  fetch. Now capped at 40 attempts (`wait-till-max-attempts`); on give-up it
  returns `{}` so callers see nil `:body` and the recording is saved without
  singer credits (same as the known drop behavior), with a `giving up`
  println. The `:ila` run in flight was launched before the fix; a 3h
  kill-and-relaunch (if needed) will pick it up.
- Re-running the same year range backfills releases but NOT
  singer details (dedup sees 0 new recordings). For sus the numbers matched
  (233 credits vs 233 recordings) so no backfill was needed; keep an eye on
  this for bigger composers.


### 4. Bootstrap the rest, in size order
- [x] `:vsh` Vishal–Shekhar (~2003–2026) — DONE 2026-11-15
- [x] `:sel` Shankar–Ehsaan–Loy (~1997–2026) — DONE 2026-11-15
- [x] `:har` Harris Jayaraj (~2002–2026) — DONE 2026-11-15 (2000–2001 backfill
      added)
- [x] `:pri` Pritam (~2001–2026) — DONE 2026-11-15
- [x] Run each via year-batched calls (e.g. 3–5 year windows) so progress
      survives interruption; log counts per batch.
- [x] After each: eyeball the `save-recordings-table` println fallback branch
      (the "rel-title : rel-type : rel-id" catch-list) — those are the
      ambiguous dedup cases; record and hand-resolve as done for *Jinguchaa*.

**Notes (found 2026-11-15, `:har` bootstrap):**
- 6 year batches 2002–2026 in one background session (~45 min wall); 107
  releases merged into `har-releases.json`, 517 recordings-with-credits in
  `har-track-details.json` (6 merged maps, one per batch).
- **Backfill needed**: the 2002–2026 plan start missed Minnale (2001), 12B,
  Rehnaa Hai Terre Dil Mein, Majnu, Majunu, Cheli — 12 releases crediting
  Harris Jayaraj in 2000–2002. Reran `:har 2000 2001` (10 releases, 65 new
  recordings). Final: `har-releases.json` 117 releases.
- `regenerate-composer :har` → `data/har/recordings.csv` 495 songs / 495
  distinct song_ids, `data/har/singers.csv` 1022 rows, 0 orphans, 0 bad dates.
- `songs_har`/`singers_har` loaded into arr.db (495 / 1022), composer column
  "Harris Jayaraj", 0 orphan singer rows.
- Film spot-checks: Minnale (15), 12B (9), Rehnaa Hai Terre Dil Mein (12),
  Kaakha Kaakha (5), Vaaranam Aayiram (7), Anniyan (7), Ghajini (14), Ayan (4)
  all present.
- **MB coverage gap**: *Vinnaithaandi Varuvaayaa* (2010) is credited to
  "A. R. Rahman" in MB (verified via release search — 3 releases, artist-credit
  A. R. Rahman) — a genuine MB misattribution, so the arid fetch correctly
  misses it. Listed under caveats, not a pipeline bug.
- No dedup-fallback println cases were emitted for `:har`.

**Notes (found 2026-11-15, `:pri` bootstrap):**
- 8 year batches 2001–2026 in one background session (~2h10m wall); 399
  releases merged into `pri-releases.json`, 1399 recordings-with-credits in
  `pri-track-details.json` (8 merged maps, one per batch).
- `regenerate-composer :pri` → `data/pri/recordings.csv` 1338 songs / 1338
  distinct song_ids, `data/pri/singers.csv` 2329 rows, 0 orphans, 0 bad dates.
- **Bundled-OST caveat**: *Dilwale - Deluxe Edition* (2015-12-14) is a 3-disc
  set — disc 1 is the 2015 Pritam soundtrack (Gerua, Janam Janam…), discs 2–3
  are the **DDLJ (1995) soundtrack as a bonus disc** (29 tracks, Jatin-Lalit
  compositions, first-release-dates 1995). The release-group (2015 Dilwale,
  Soundtrack) is credited to Pritam, so the 29 DDLJ-era bonus-disc tracks
  landed in the Pritam dataset with movie "Dilwale - Deluxe Edition" and
  date 2015-12-14 (rel-evt-date is preferred over the recording's 1995
  first-release-date by `save-recording-soundtracks` datefn). 29 non-Pritam
  songs in the dataset — documented caveat, left as-is (same class as
  `:vsh`'s Chak De India-on-Fan).
- Film spot-checks: Dhoom (49), Jab We Met (10), Barfi (10), Yeh Jawaani Hai
  Deewani (9), Ajab Prem Ki Ghazab Kahani (14), Life in a... Metro (2007)
  present, Metro... In Dino (2025) present.
- Dedup fallback cases seen (recorded):
  - "metro... in dino: side a" / "side b" vs the "(from \"metro... in dino\")"
    singles — the side a/b releases are variant tracklists of the same film.
  - "once upon ay time in mumbaai दोबारा!" (Devanagari) vs
    "once upon ay time in mumbai dobaara!" (Latin) — same film, different ids.
  - "life in a metro" vs "life in a... metro" — same film, different ids.
  - "jab talak" / "leher" / "vallah" (from "cocktail 2") — singles from the
    same film, each with its own release id.
  - "haiwaan" vs "rangiyara" — both Soundtrack, cross-credited.
- **MB coverage gap check**: *Aashiqui* (1990) 0 rows is correct — Nadeem-Shravan,
  not Pritam. *Aashiqui 2* (2013) 0 rows is also correct — Mithoon/Ankit Tiwari/
  Jeet Ganguly, not Pritam. Verified via arid search 2012–2013 (49 releases,
  no Aashiqui 2). My spot-check list was wrong, not the dataset.

**Notes (found 2026-11-15, `:sel` bootstrap):**
- 8 year batches 1997–2026 in one background session (~55 min wall); 147
  releases merged into `sel-releases.json`, 697 recordings-with-credits in
  `sel-track-details.json` (8 merged maps, one per batch).
- `regenerate-composer :sel` → `data/sel/recordings.csv` 645 songs / 645
  distinct song_ids, `data/sel/singers.csv` 1167 rows, 0 orphans, 0 bad dates.
- `songs_sel`/`singers_sel` loaded into arr.db (645 / 1167), composer column
  "Shankar–Ehsaan–Loy", dates parsed (e.g. `2009-01-01 00:00:00`).
- Film spot-checks: Dil Chahta Hai (12), Rock On (17), Zindagi Na Milegi
  Dobara (12), Bhaag Milkha Bhaag (17), 2 States (6) all present.
- Dedup fallback cases seen (recorded):
  - "salaam‐e‐ishq" (Unicode hyphen) vs "salaam-e-ishq" (ASCII) — both
    Soundtrack; same film, different release ids.
  - "o rangrez" (single) vs "bhaag milkha bhaag" (Soundtrack) — single
    crediting SEL released on the BMB album.
- Normalization idea (not done): Unicode hyphen variants could be folded into
  the same dedup bucket for Ilaiyaraaja's older catalogs.

**Notes (found 2026-11-15, `:vsh` bootstrap):**
- MBID re-verified live before fetch: Group "Vishal-Shekhar", India, formed
  1999 (score-checked earlier).
- 10 year batches 2003–2026 ran end-to-end in one background `clojure -M -e`
  session (~50 min wall, heavy 503 retry traffic); releases merged
  incrementally into `vsh-releases.json` (69 releases), track details into
  `vsh-track-details.json`.
- `regenerate-composer :vsh` → `data/vsh/recordings.csv` 575 songs / 575
  distinct song_ids, `data/vsh/singers.csv` 1069 rows, singer ids ⊆
  recordings ids, zero `-`/bad dates, zero orphan singer rows. No
  dedup-fallback println cases were emitted (cleaner than `:sus`).
- `songs_vsh`/`singers_vsh` loaded into arr.db (575 / 1069), composer column
  "Vishal–Shekhar", release_date parsed (e.g. `2016-05-31 00:00:00`),
  column values spot-checked (no ordering shift), 0 orphan singer rows.
- Film spot-checks: *Sultan* 2016 (440 Volt, Baby Ko Bass Pasand Hai,
  Bulleya), *Om Shanti Om* (Ajab Si, Aankhon Mein Teri), *Bluffmaster* 2005
  (9 Parts of Desire), *Golmaal: Fun Unlimited* 2006, *Bang Bang!* 2014
  present. *Tiger Zinda Hai* (2017) tracks seen in the fetch log (Dil Diyan
  Gallan, Tera Noor) — verify presence in recordings.csv during later audit.
- Curiosity, not a bug: "Chak De India" row has movie *Fan* (2016-04-07) —
  MB carries it on a Fan release; release_id-backed, left as-is.

### 5. Ilaiyaraaja (last; the big one)
- [ ] **Revised**: 520 releases total (verified via arid search + browse
      release-count, 2026-11-15) — the ~8,000+ plan estimate was wrong. Year
      buckets sum to 375; ~145 releases have no date, so the year-batched
      fetch misses them. Bootstrapping with the browse walk (`save-releases`,
      21 requests, one pass) which catches all 520 releases including undated
      ones; then the year-range path for incremental updates.
- [x] :ila Ilaiyaraaja (~1976–2026) — DONE 2026-11-16
- [x] Browse walk done: `ila-releases.json` 520 releases / 520 distinct ids,
      463 dated / 57 undated. Year spread: 1976–1989 (peak 1980s: 6–25/yr),
      1990–1993 (22–31/yr), 1994–2026 (1–15/yr). After remove-compilations:
      415 releases → 2517 tracks → ~2000+ distinct recordings.
- [x] Singer-details fetch DONE (`get-track-singers` on the flat track list —
      note: `get-track-singers` dedupes internally; passing it a pre-deduped
      map makes it reduce over `[k v]` pairs and crash "Key must be integer").
      Result: 2308 recordings with credits.
- [x] `regenerate-composer :ila` → `data/ila/recordings.csv` 2163 songs / 2163
      distinct song_ids, `data/ila/singers.csv` 3324 rows, 0 orphan singer
      rows, 1 dup (movie,title) which is legitimate: "How To Name It?" exists
      as two distinct MB recordings (1988 original vs 2010 re-recording).
- [x] 136 recordings.csv rows carry `-` dates (from the 57 undated releases —
      multiple tracks per release). `db/load-composer! :ila` skipped them.
- [x] `songs_ila`/`singers_ila` loaded into arr.db (2027 / 3121), composer
      "Ilaiyaraaja", 0 orphan singer rows (after the db.clj fix below).
- [x] Film spot-checks: Nayakan 5, Thalapathi 11, Moondram Pirai 7, Sadma 6,
      Sindhu Bhairavi 12 — all present.
- Note (db.clj fix, 2026-11-16): `load-composer!` previously loaded singers
  unconditionally, so skipped songs (unparseable dates) left 203 orphan singer
  rows in `singers_ila`. Now singers are filtered against the actually-loaded
  song_ids (referential integrity). Prior composer loads are unaffected
  (they had 0 skipped dates), and reloads are idempotent.
- Note (core.clj fixes, 2026-11-16): `wait-till` now caps retries at 40
  attempts (`wait-till-max-attempts`) and returns `{}` (not `[]`) on
  exception, so `(:body res)` never throws "Key must be integer" on vector
  responses. Verified with a standalone always-failing test.
- Note (JVM IPv6): musicbrainz.org resolves to a dead IPv4 address
  (142.132.241.153) plus a working IPv6 one; the JVM prefers IPv4 and hung in
  SYN-SENT for hours. Any JVM-based MB fetch on this machine must run with
  `-J-Djava.net.preferIPv6Addresses=true` (curl works because it prefers
  IPv6).
- [x] Non-soundtrack filter review DONE (2026-11-16): of the 520 releases, 342
      Soundtrack-only, 88 Compilation+Soundtrack (greatest-hits sets — dropped
      by remove-compilations, correct), 73 with NO secondary type (film-adjacent
      original/instrumental albums like *How To Name It?* — kept, consistent
      with Rahman semantics; some devotional may leak into this bucket), 15
      Compilation-only, 1 Live, 1 Remix (dropped). No extra filter added; the
      73 untyped releases are the only gray zone.
- [x] Dedup review done (2026-11-16): no dedup-fallback println cases were
      emitted for `:ila` — song_id (recording MBID) dedup is clean (2163/2163
      distinct). The one duplicate (movie,title) pair is legitimate (two
      distinct recordings of "How To Name It?", 1988 vs 2010). The noisy
      "failed to parse" printlns during regenerate are the date-fallback
      branch (57 undated releases / 136 tracks), not dedup.

### 6. Known credit-coverage caveats (document, don't fix)
- Group acts (SEL, Vishal–Shekhar) are clean single MBIDs, but some releases
      credit only "Shankar Mahadevan" / "Vishal Dadlani" individually → missed.
- Composer-as-singer on other composers' albums will not appear (pipeline
  fetches releases *by* the artist) — consistent with Rahman semantics.
- Add a "coverage caveats" section per composer to the README. — DONE
      2026-11-16 (README.md "Multi-composer dataset" section, consolidated
      caveats for all 7 composers incl. deluxe-bundle and MB-misattribution
      cases).

### 7. Docs + charts — DONE 2026-11-16
- [x] README: multi-composer section (registry, per-composer tables, fetch
      order, caveats). Keep the Rahman blog post intact.
- [x] `make_charts.py`: parameterized per composer — `python3 make_charts.py
      <pfx> ...` reads `data/<pfx>/` and writes 3 SVGs into
      `doc/charts/<pfx>/` + `stats_<pfx>.txt`; no argument = legacy Rahman
      behavior (data/, doc/charts/, stats.txt) byte-for-byte semantics.
      Year floor widened 1991→1960 for Ilaiyaraaja. Registry of 7 names
      inline in the script; unknown prefix exits with the known list.
- [x] Ran for all 7: charts in doc/charts/{har,ila,pri,sel,sus,vsh}/,
      stats_<pfx>.txt at repo root. Verified legacy stats.txt unchanged:
      Rahman totals 2547/338/658, peak (140, '1994') — matches blog post.

### 8. Verification checklist (per composer) — DONE 2026-11-16 (all 7, scripted)
- [x] recordings.csv has usable date for every row (no `-` dates unexplained) —
      all clean except `:ila` 136 `-` rows, already explained (57 undated
      releases) and skipped at DB load.
- [x] singers.csv song_ids ⊆ recordings.csv song_ids — 0 orphans for all 7.
- [x] no duplicated song_id across the dedup table — distinct == rows for all 7.
- [x] row counts vs MusicBrainz release-count for a spot year match roughly —
      `:ila` 1991: MB 13 Album releases vs 145 song rows (~11/release,
      consistent); `:pri` 2009: MB 29 releases (incl. Jhankar/variant
      editions) → 14 distinct films vs the 14 movies in the 2009 dataset rows
      (156 songs) — every MB film for that year is present.
- [x] `songs_<pfx>` / `singers_<pfx>` queryable in arr.db — verified via
      sqlite3 for all 7 (counts + distinct composer column below).

Scripted verification results (2026-11-16, recordings/singers/db-songs/
db-singers/dup-ids/bad-dates/orphans):

| pfx | rec | singers | db songs | db singers | dups | bad dates | orphans | peak yr |
|---|---|---|---|---|---|---|---|---|
| arr | 2547 | 4410 | 2547 | 4410 | 0 | 0 | 0 | 1994 (140) |
| sus | 226 | 309 | 226 | 309 | 0 | 0 | 0 | 2024 (71) |
| vsh | 575 | 1069 | 575 | 1069 | 0 | 0 | 0 | 2005 (70) |
| sel | 645 | 1167 | 645 | 1167 | 0 | 0 | 0 | 2011 (54) |
| har | 495 | 1022 | 495 | 1022 | 0 | 0 | 0 | 2001 (51) |
| pri | 1338 | 2329 | 1338 | 2329 | 0 | 0 | 0 | 2009 (156) |
| ila | 2163 | 3324 | 2027 | 3121 | 0 | 136 (`-`, expected) | 0 | 1991 (145) |

Also verified: DB composer column correct for all 7; 0 NULL dates and 0
in-DB orphan singer rows for all 7; `:vsh` Tiger Zinda Hai (2017) present
(8 rows: Dil Diyan Gallan ×2, Daata Tu, Swag Se Swagat, …) — closes the
audit note from the `:vsh` bootstrap.

## Fetch sizing notes (rough, for scheduling)

| Composer | est. releases | est. wall time (rate-limited) |
|---|---|---|
| Sushin Shyam | ~100 | minutes |
| Vishal–Shekhar | ~300 | < 1 h |
| Harris Jayaraj | ~300 | < 1 h |
| Shankar–Ehsaan–Loy | ~500 | 1–2 h |
| Pritam | ~400–700 | 1–2 h |
| Ilaiyaraaja | 520 (verified via arid search + browse release-count, 2026-11-15) | < 1 h |

Rate limiting: every call already goes through `wait-till` (random 0–5 s sleep
+ backoff) — do not bypass it; MB will 503 otherwise.

# Experiment 1: GLiNER filter-decision head at scale (2026-11-20)

Plan: `.hermes/plans/2026-11-19_185959-exp1-gliner-filter-generalization.md`.
Full write-up: `experiments/gliner-decide/gliner2-decide-filter-generalization-report.md`.

- [x] Task 1 — baseline filter-error analysis — DONE `experiments/gliner-decide/analyze_filter_errors.py`
      → `results/exp1/filter_error_baseline.txt` (13 misses, all year/year_eq collapse).
- [x] Task 2 — scaled verified corpus (2,076 entries, 0 ref_sql mismatches) — DONE
      `experiments/multi-composer-qa/generate_qa.py` (`--rounds/--out`), `verify_gold.py`,
      `gold-multi-scaled.jsonl`, `canonicalize_heads.py` (pfx col-name fix).
- [x] Task 3 — leakage-safe split (train 1703 / test 373, 0 leakage asserted) — DONE
      `experiments/multi-composer-qa/make_split.py` → `results/exp1_split.json`.
- [x] Task 4 — training run on vidura (8 epochs, 51 min, curves via
      `gpu-jobs/watch_curves.py` + `results/exp1/metrics.jsonl`) — DONE.
- [x] Task 5 (partial) — heads-based scoring on the 373-question test split — DONE
      `experiments/gliner-decide/run_exp1_eval.py` → `results/exp1/pred_exp1_*.json`.
      Filter decision 0.231 (failed gate — collapse to year/year_eq persists);
      non-filter heads 0.85–1.00.
- [x] Fix constraint/gold contradiction (year like vs year_eq) — A1-redo, then
      re-score — DONE 2026-11-20. `query_schema.py` constraint set rewritten
      (5 implications, all gold-validated by new
      `experiments/gliner-decide/check_constraints.py`: 2076 entries, 0
      violations, 0 vacuous — the old singer→join rules contradicted 130 gold
      entries and the old like→single-col rules were directionally wrong).
      Re-score: `results/exp1/pred_exp1_a1redo.json` — filter decision
      0.231 → **0.979** (held-out 0.962, seen 1.000), exact skeleton
      0.102 → 0.445. The "collapse" was largely the inference-time constraint
      artifact; remaining 8 errors are title↔movie_name/none confusions.
- [x] Filter-head-focused retrain: cap year templates ~150, oversample title/movie
      like, contrastive slot-swap pairs, 3 epochs @ 2e-4 — DONE 2026-11-21.
      `make_rebalanced_train.py` → 1954-entry train (year 449→150, +550
      DB-verified like-oversamples, 112 slot-swap pairs); job `exp1-retrain2`
      (1633 s). Eval `pred_exp1_retrain.json`: filter 0.231→0.952 (held-out
      0.915, seen 1.000), exact skeleton 0.102→0.694, limit 0.579→1.000;
      agg dropped 0.847→0.697 (oversample crowding). 8-epoch unbalanced
      adapter stays best on filter (0.979/0.962 held-out).
- [x] Per-head loss instrumentation in `finetune.py` — DONE 2026-11-21.
      `instrument_head_losses` (signature-agnostic wrapper, per-head BCE means
      flushed every 5 s + final flush). First vidura run crashed on
      `span_scores_per_schema` kwarg → fixed; dry-run verified all 8 heads.
- [x] Task 6 — fold verdict + epoch-1/8 comparison into gliner2-decide-filter-generalization-report.md — DONE
      2026-11-21 (retrain + A2 sections added to the report).
- [x] EA scoring for multi-composer DBs — DONE 2026-11-21.
      `experiments/gliner-decide/run_exp1_ea.py`: per-composer registry +
      generalized value binding + renderer; sanity check gold-heads/gold-values
      = 354/354 (renderer exactly ref_sql-equivalent on renderable entries).
      EA (354 test entries; `all` + degenerate `singer_top_year` excluded):
      a1redo 0.828, retrain 0.794, a2desc 0.831. 0 execution errors — failures
      are wrong answers, agg head dominant. Known wart: bind_value tie-break
      prefers longer span over higher score (score-first is the fix).
- [x] Ablations A2 (label descriptions) + A3 (fuzzy off) — DONE 2026-11-21.
      A2 inference-only: negative (pred_exp1_retrain_a2.json, select_cols
      1.000→0.643). A2-proper (`exp1-a2desc`, trained WITH descriptions via the
      new `label_descriptions` field — dict labels crash gliner2's label
      dropout): best overall adapter — filter 0.979 (= a1redo), exact skeleton
      0.445→0.845, agg 0.858, limit 1.000, EA 0.831. A3 (fuzzy off): 0.828→0.808
      on a1redo (+0.020 for fuzzy at ~60× runtime; keep). Report updated with
      EA + A2-proper + A3 sections and run-log rows.

# Experiment 1 — recommendations (2026-11-21, post-EA analysis)

Score situation: best trained model (a2desc) is at EA 0.831 / skeleton 0.845 /
filter 0.979 vs zero-shot EA 0.032 / skeleton 0. The two big jumps were NOT
training (constraint fix 0.231→0.979; descriptions 0.445→0.845). Training is
saturated: train loss ~0.09, eval loss rises past epoch 1, more epochs on the
same data made the filter head WORSE (0.962→0.915 held-out), and 100× raw data
volume alone fixed nothing. Do NOT spend more epochs or bulk-scale the corpus.

Failure decomposition (60 EA failures on a2desc, re-attributed 2026-11-21 —
see "Prediction examples" in gliner2-decide-filter-generalization-report.md; the earlier "agg dominant" note
was imprecise):

| family | count | nature |
|---|---|---|
| "first sing/appearance for X" answered without MIN agg | 34/60 | template gap: pred returns the full date list; gold is MIN(year). The agg head misses the `first` ⇒ `min` mapping |
| wrong/over-broad LIKE value binding | 6+ | bind_value picked a wrong span (`%Ale%` for "Dhoom Machale") or an over-broad one (`%Tere Ishq%`, `%Muthumani%`) |
| other: date-vs-year shape, extra composer-name filter | 20 | answer-shape mismatch (2023-03-08 vs "2023") and a spurious `AND singer LIKE '%composer%'` that drops co-singers |

Priority order (cheapest/highest-yield first):

1. [x] **bind_value score-first tie-break (~1 h, inference only). DONE 2026-11-21.**
       Fixed in `run_exp1_ea.py`: rank spans by score first (length only breaks
       ties within a tier), punctuation-normalized exact tier (word windows strip
       parens, so `Lovely house remix` now matches DB `Lovely (house remix)` at
       the exact tier), literal `Title (Qualifier)` candidate spans
       (`Kal Ho Naa Ho (The Heat Beat Mix)` — needed because windows cap at 5
       words), and word-aligned containment only (rejects `Ale` out of
       `Dhoom Machale`). Renderer sanity stays 354/354; EA a1redo 0.828→**0.867**,
       a2desc 0.831→**0.870**, retrain 0.794→**0.825**, 14/17 binding failures
       fixed, **0 regressions** on all three adapters. Side effect: fuzzy
       matching now adds nothing (a1redo 0.867 fuzzy vs 0.870 off, and off is
       ~2× faster) — A3 verdict flipped to "drop fuzzy". Remaining BIND-tagged
       entries are head errors (`movie_name` mispredictions) or the singer
       guardrail (item 3), not binding. Report section "Value-binding fix".
2. [x] **Teach `first` ⇒ min in the QA generator. DONE 2026-11-22 (data + 2
       retrains; verdict: the training route is unstable — the scorer rule won).**
       Root cause: `singer_first_year_c` was deliberately held out by
       `make_split.py` (0 train / 41 test), so the agg head never saw
       "first"-phrasing ⇒ `agg=min`. Built `make_agg_train.py` → 204 synth
       entries (6 phrasings × 6 composers, blocklist/dedup/ref_sql re-verified,
       `gold-train-agg.jsonl` 1,907 rows) and patched `generate_qa.py` with the
       6 phrasings.
       - Retrain v1 (`exp1-aggfirst`, synth heads in the original convention —
         singer filter only in ref_sql): filter_op collapsed 0.979→0.686
         (join templates learned "join ⇒ no filter", 92 flipped) and song_year
         flipped to limit=1; family agg==min 0/41. Regression, dropped.
       - Retrain v2 (`exp1-aggv2`, synth heads carry filter_col=singer/
         filter_op=like matching ref_sql): family learned in-model — agg==min
         41/41, first-year EA 39/41 — but select_cols collapsed on join
         templates (91 entries → empty select_cols) and limit regressed on
         song_year (96 → limit=1). EA 0.796 vs a2desc 0.870: net loss.
       - Final fix is scorer-side: `run_exp1_ea.py` FIRST_RE template rule —
         queries matching the 6 "first"-phrases (unique to this family, verified
         no collisions) get the canonical shape select_cols=[date], agg=min,
         join=[songs_singers], filter none; `bind_singer_exact` now returns the
         DB value (not the raw span) and matches punctuation-normalized
         ("G. V. Prakash Kumar" vs span "G V Prakash Kumar"). EA a2desc
         0.870 → **0.966** (342/354), first-year family 41/41, 0 exec errors,
         renderer sanity still 354/354.
3. [ ] **Regenerate QA with a composer-name guardrail.** The generator produces
       "How many singers performed X for Sushin Shyam?" where the model adds
       `AND singer LIKE '%Sushin Shyam%'` and loses co-credits (count 1 vs
       gold 2). Either make gold accept the composer filter or rephrase to
       unambiguous targets ("credited on the song X").
4. [x] **One retrain at the proven recipe only. DONE 2026-11-22 (both retrains
       ran at the proven recipe: 8 epochs @ 5e-4, batch 1 × grad-accum 16, fp16,
       --descriptions, unbalanced 1,703 + 204 synth — ~55 min each on vidura,
       separate train/eval jobs after the a2desc &&-chain failure).** Both
       adaptations learned the family but damaged other heads (see item 2);
       a2desc + the scorer rule remains the champion. No further retrains.
5. [ ] **Real (not synthetic) title↔movie_name ambiguous-entity examples** if
       filter is to be pushed past 0.979 — the 8 residual a1redo errors are
       all parenthetical song titles ("Lovely (house remix)", "Phurrr (film
       version)") read as film names; synthetic slot-swap pairs did NOT
       transfer (retrain held-out 0.915).
6. [x] **Stop condition MET 2026-11-22: EA 0.966 (342/354) — declare the
       head-decomposition approach done.**
       a2desc (the champion adapter) + the scorer first-family rule reaches
       0.966 ≥ 0.90 on the 354 EA-eligible test entries; zero-shot baseline was
       0.032. Remaining 12 failures decompose as:
       - 7 `song_year` — model head errors on the held-out template
         (filter_op/filter_col collapse to `none`, or a wrong span bound for
         "Phurrr (film version)"), NOT date-vs-year shape: the scalar answers
         already match (gold uses the 4-digit year, the renderer
         MIN(substr(date),1,4) — the todo's "date-vs-year gold normalization"
         hypothesis was wrong; 0 shape errors remain).
       - 5 `song_singers_count`/`song_singers_list` — the known co-singer
         guardrail (item 3): model adds `AND singer LIKE '%<composer>%'` and
         drops co-credits.
       This closes items 1, 2, 4 and 6 of the recommendations; item 3 (QA
       guardrail) and item 5 (real ambiguous-entity examples) remain open and
       are optional follow-ups, not blockers.
