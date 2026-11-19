(ns arr-song-dataset.db
  "Load per-composer CSVs (data/<pfx>/recordings.csv, singers.csv) into the
   on-disk arr.db as songs_<pfx> / singers_<pfx> tables.
   The legacy `songs`/`singers` tables in arr.db and the in-memory query.clj
   path are left untouched."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [arr-song-dataset.core :as core])
  (:import (java.text SimpleDateFormat ParseException)))

(def file-spec
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "arr.db"})

(defn- get-rows
  [ifile]
  (with-open [reader (io/reader ifile)]
    (doall (csv/read-csv reader))))

(defn- parse-date
  [s]
  (.parse (SimpleDateFormat. "yyyy-MM-dd") s))

(defn create-composer-tables!
  "create songs_<pfx> and singers_<pfx> if they don't exist"
  [db pfx]
  (jdbc/execute! db [(str "create table if not exists songs_" pfx
                          " (song_id uuid, song text, release_date date, movie text, release_id uuid, composer text)")])
  (jdbc/execute! db [(str "create table if not exists singers_" pfx
                          " (song_id uuid, singer text)")]))

(defn drop-composer-tables!
  [db pfx]
  (jdbc/execute! db [(str "drop table if exists songs_" pfx)])
  (jdbc/execute! db [(str "drop table if exists singers_" pfx)]))

(defn load-composer!
  "load data/<pfx>/recordings.csv and singers.csv into arr.db tables
   songs_<pfx>/singers_<pfx>. Rows with unparseable dates (e.g. `-`) are
   skipped with a println. Idempotent: replaces existing rows. Returns
   {:songs n :singers n :skipped m}."
  ([ckey] (load-composer! file-spec ckey))
  ([db ckey]
   (let [{:keys [prefix data-dir] :as paths} (core/paths-for ckey)
         _ (when-not paths
             (throw (ex-info (str "unknown composer " ckey) {:composer ckey})))
         composer-name (or (:name paths) (str prefix))
         rec-file (str data-dir "recordings.csv")
         singer-file (str data-dir "singers.csv")]
     (jdbc/with-db-transaction [tx db]
       (create-composer-tables! tx prefix)
       (jdbc/execute! tx [(str "delete from songs_" prefix)])
       (jdbc/execute! tx [(str "delete from singers_" prefix)])
       (let [rows->maps (fn [rows]
                          (let [headers (mapv keyword (first rows))]
                            (map #(zipmap headers %) (rest rows))))
             fmt-date (fn [m]
                        (try
                          (assoc m :release_date
                                 (.format (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")
                                          (parse-date (:date m))))
                          (catch ParseException e
                            (println " skipping row with unparseable date " (:song m) (:date m))
                            nil)))
             song-rows (->> (get-rows (io/file rec-file))
                            rows->maps
                            (map fmt-date)
                            (remove nil?)
                            (mapv #(-> %
                                       (assoc :composer composer-name)
                                       (dissoc :date))))
             loaded-song-ids (set (map :song_id song-rows))
             singer-rows (->> (get-rows (io/file singer-file))
                              rows->maps
                              (mapv #(select-keys % [:song_id :singer]))
                              ;; keep referential integrity: a skipped song
                              ;; (unparseable date) must not leave orphan
                              ;; singer rows behind
                              (filter #(contains? loaded-song-ids (:song_id %))))
             song-cols [:song_id :song :release_date :movie :release_id :composer]
             singer-cols [:song_id :singer]
             ;; java.jdbc 0.7 insert-multi! with map rows ignores the col-names
             ;; ordering (values follow map iteration order) — pass value vectors
             ;; in the exact column order instead.
             row-vals (fn [cols m] (mapv #(get m %) cols))
             song-rows-vals (mapv #(row-vals song-cols %) song-rows)
             singer-rows-vals (mapv #(row-vals singer-cols %) singer-rows)]
         (when (seq song-rows-vals)
           (jdbc/insert-multi! tx (keyword (str "songs_" prefix)) song-cols song-rows-vals))
         (when (seq singer-rows-vals)
           (jdbc/insert-multi! tx (keyword (str "singers_" prefix)) singer-cols singer-rows-vals))
         {:songs (count song-rows)
          :singers (count singer-rows)
          :table-suffix prefix})))))

(comment
  ;; load one composer (after regenerate built data/<pfx>/*.csv)
  (load-composer! :arr)
  (jdbc/query file-spec ["select count(*) from songs_arr"])
  (jdbc/query file-spec ["select count(*) from singers_arr"]))
