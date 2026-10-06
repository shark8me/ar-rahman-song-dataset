(ns arr-song-dataset.core
  (:require [clj-http.client :as client]
            [clojure.data.json :as json])
  #_(:gen-class))

(def headers {"User-Agent" "Application name/arr-song-dataset ( https://github.com/shark8me)" })
(def arr-artist-id "e0bba708-bdd3-478d-84ea-c706413bedab")

(defn wait-till
  [ifn]
  (let [sleep-for (rand-int 5)
        icall (fn[]@(future
                      (Thread/sleep (* sleep-for 1000))
                      (try
                        (ifn)
                        (catch Exception e
                          (do
                            (println " exception "e)
                            [])))))]
    (loop [sl sleep-for]
      (let [iresp (icall)]
        (if (:body iresp)
          iresp
          (recur (+ sl (rand-int 5))))))))

#_(wait-till #(client/get
             (str "https://musicbrainz.org/ws/2/release?artist="arr-artist-id "&offset=" 0 "&inc=recordings+release-groups+artist-rels+recording-rels")
             {:accept :json :headers headers :as :json}))

(defn get-all-releases
  [artist-id]
  (loop [acc []
         release-count -1
         offset 0]
    (if (= release-count offset)
      acc
      (let [rel (str "https://musicbrainz.org/ws/2/release?artist=" artist-id "&offset=" offset "&inc=recordings+release-groups+artist-rels+recording-rels")
            resp (:body (wait-till #(client/get rel {:accept :json :headers headers :as :json})))
            rel-count (:release-count resp)
            idiff (- rel-count (:release-offset resp))
            climb (if (> idiff 25) 25 idiff)]
        (println " rel-count " rel-count " rel-offset " (:release-offset resp))
        (recur (conj acc resp)
               rel-count
               (+ climb offset))))))

(defn save-releases
  "download all releases and save it"
  [artist-id releases-json]
  (->>
   (get-all-releases artist-id)
   (map :releases)
   (reduce into [])
   (json/write-str)
   (spit releases-json)))

(defn get-song-details
  [rid]
  (let [track-url (str "https://musicbrainz.org/ws/2/recording/" rid "?inc=artists+artist-rels+recording-rels+release-rels+release-group-rels")
        song (client/get track-url {:accept :json :headers headers :as :json})
        ikeys [:title :first-release-date :id :artist-credit]]
    song))

;; ---- incremental update by year ------------------------------------------
;; the browse endpoint used in get-all-releases has no date filter, but the
;; search endpoint does: arid:<artist> AND date:[<y1>-01-01 TO <y2>-12-31].
;; search results lack :media, so each new release id is re-fetched with the
;; full inc= payload before being merged into the existing dump.

(defn get-all-track-singers
  [track-map]
  (->> track-map
       (mapv (fn[[k v]]
               {k (assoc v :song-details
                         (let [res (wait-till (fn[](get-song-details k)))]
                           (println " " (-> res :body :title))
                           (:body res)))}))))

(defn get-releases-for-years
  "search releases of the artist released between [start-year end-year] (inclusive).
  returns the raw search-endpoint release maps (no :media, ids + release-group only)."
  [artist-id start-year end-year]
  (loop [acc []
         offset 0]
    (let [q (str "arid:" artist-id " AND date:[" start-year "-01-01 TO " end-year "-12-31]")
          url (str "https://musicbrainz.org/ws/2/release?query="
                   (java.net.URLEncoder/encode q "UTF-8")
                   "&fmt=json&limit=100&offset=" offset)
          resp (:body (wait-till #(client/get url {:accept :json :headers headers :as :json})))
          cnt (:count resp)
          rels (vec (:releases resp))]
      (println " search-count " cnt " offset " offset " got " (count rels))
      (if (>= (+ offset (count rels)) cnt)
        (into acc rels)
        (recur (into acc rels) (+ offset (count rels)))))))

(defn get-release-details
  "full release payload (with :media/:tracks) for one release id, in the same
  shape that get-all-releases/save-recordings-table expect"
  [rel-id]
  (:body (wait-till #(client/get
                      (str "https://musicbrainz.org/ws/2/release/" rel-id
                           "?inc=recordings+release-groups+artist-rels+recording-rels")
                      {:accept :json :headers headers :as :json}))))

(defn remove-compilations
  "remove albums that are marked as compilations"
  [rel-list]
  (let [filt-fn (fn[rel]
                  (let [sec-types  (map keyword (map #(.toLowerCase %) (-> rel :release-group :secondary-types)))
                        ret (some #{:compilation :remix :live} sec-types)]
                    ret))]
    (vec (remove filt-fn rel-list))))

(defn save-releases-for-years
  "incremental update: download releases dated [start-year end-year], merge them
  into the existing releases-json (deduped by release id), and fetch singer
  details for any recordings not already in track-details-json, merging those
  in too. does not run regenerate afterwards."
  [artist-id start-year end-year releases-json track-details-json]
  (let [existing (json/read-str (slurp releases-json) :key-fn keyword)
        existing-ids (into #{} (map :id) existing)
        new-releases (->> (get-releases-for-years artist-id start-year end-year)
                          (remove #(existing-ids (:id %)))
                          (map :id)
                          (mapv get-release-details))
        merged-releases (into (vec existing) new-releases)
        ;; singer details for new recordings
        track-details (vec (json/read-str (slurp track-details-json) :key-fn keyword))
        known-recs (->> track-details
                        (apply merge)
                        keys
                        (map name)
                        (into #{}))
        new-track-map (->> new-releases
                           remove-compilations
                           (map #(mapv :tracks (:media %)))
                           (reduce into [])
                           (reduce into [])
                           (reduce (fn[acc i] (assoc acc (-> i :recording :id) i)) {}))
        missing-recs (remove known-recs (keys new-track-map))
        new-singers (when (seq missing-recs)
                      (apply merge
                             (get-all-track-singers (select-keys new-track-map (vec missing-recs)))))
        merged-details (if new-singers
                         (conj track-details new-singers)
                         track-details)]
    (println " new releases " (count new-releases)
             " new recordings " (count missing-recs))
    (spit releases-json (json/write-str merged-releases))
    (spit track-details-json (json/write-str merged-details))
    [releases-json track-details-json]))

#_(defn get-tracks-in-media
  "get all the tracks in all :media entries"
  [rel-list]
  (->> rel-list
       remove-compilations
       (map #(mapv :tracks (:media %)))
       (reduce into [])
       (reduce into [])))

;;(def arr-rel-list (json/read-str (slurp "./arr-releases-5471.json") :key-fn keyword))
;;(def all-tracks (get-tracks-in-media arr-rel-list))

(defn get-track-singers
  [all-tracks]
  (-> (reduce (fn[acc i] (assoc acc (-> i :recording :id) i)) {} all-tracks)
       (get-all-track-singers)))


(defn save-track-singers
  [track-singers]
  (spit "./arr-track-details.json" (json/write-str track-singers)))

;;(def track-singers (get-track-singers all-tracks))
;;(save-track-singers track-singers)

#_(def track-singers-fin
  (json/read-str (slurp "./arr-track-details.json") :key-fn keyword))


(defn extract-artists
  [ival]
  (let [artist-credits (mapv #(select-keys (:artist %) [:id :name :disambiguation])
                             (-> ival :song-details :artist-credit ))]
    (-> (select-keys ival [:title ])
        (assoc :artists artist-credits)
        (assoc :first-release-date (-> ival :song-details :first-release-date)))))

;;spit songs out
(defn save-song-table
  [track-singers song-table-csv]
  (->> track-singers
       (apply merge)
       (map (fn[[rec-id v]]
              (let [ea (extract-artists v) ]
                {:song (-> (select-keys ea [:title :first-release-date])
                           (assoc :rec-id rec-id))
                 :artists (->> ea
                               :artists
                               (mapv #(assoc  % :rec-id rec-id)))})))
       (mapv :song)
       (mapv (fn[i] (clojure.string/join ","
                                         (mapv #(let  [s (i %)]
                                                  (if (keyword? s) (name s) s))
                                               [:title :first-release-date :rec-id]))))
       (clojure.string/join "\n")
       (spit song-table-csv )))

;;(save-song-table track-singers-fin "songtable.csv")

(defn save-recordings-table
  "remove duplicate recordings and songs"
  [arr-rel-list json-file]
  (->> arr-rel-list
       remove-compilations
       (map #(let [release-id (:id %)
                   rel-title (:title %)
                   stype (-> % :release-group :secondary-types)
                   release-group-first-release-date (-> % :release-group :first-release-date)
                   rel-area (-> % :release-events first :area :name)
                   rel-evt-date (-> % :release-events first :date)
                   sdf (new java.text.SimpleDateFormat "yyyy-MM-dd")
                   rel-date (try (.parse sdf rel-evt-date)
                                 (catch Exception e
                                   #_(println " failed to parse " rel-evt-date " : " release-id )))]
               (->> (mapv :tracks (:media %))
                    (reduce into [])
                    (map :recording)
                    (map (fn[i]
                           ;;sometimes the track->recordings doesn't have a first-release-date
                           ;;then add the first-release-date from the release-group.
                           (-> (if (:first-release-date i) i
                                   (assoc i :first-release-date release-group-first-release-date))
                               (assoc :rel-title rel-title :rel-id release-id
                                      :rel-area rel-area
                                      :rel-evt-date rel-evt-date
                                      :rel-type stype)))))))
       (reduce into [])
       (map #(dissoc % :video :length))
       (map #(assoc {} (:id %)  %))
       (reduce (fn[acc i]
                 (let [kys (first (keys i))
                       ival (i kys)
                       rel-title (.toLowerCase (:rel-title (i kys)))]
                   (if-not (acc kys)
                     (merge acc i)
                     (let [accrel-title (.toLowerCase (:rel-title (acc kys)))
                           rel-types (mapv #(:rel-type (% kys)) [i acc])
                           [irel-area accrel-area] (mapv #(:rel-area (% kys)) [i acc])
                           [irel-type accrel-type] rel-types
                           [frid srid] (mapv #(:rel-id (% kys)) [i acc])]
                       ;;if rel-type of both is "Soundtrack"
                       ;;prefer (= "India" (-> :release-events :area :sort-name))
                       ;;if there is a india & worldwide release e.g.
                       ;;repeat  Thiruda Thiruda: Original Motion Picture Soundtrack :  [Soundtrack] : b9f4f065-7b77-45ec-9cbe-aba5e38050da  second  Thiruda Thiruda :  [Soundtrack] : 4bb3dd57-beec-4bf4-a444-47fa7f2fc624
                       ;;sometimes none of the release events are in India e.g.

                       ;;repeat  Sapnay :  [Soundtrack] : 4cc8b732-03eb-48cb-8fe3-d91085372c8c  second  Sapnay (Original Motion Picture Soundtrack) :  [Soundtrack] : 8aa45018-fb87-453b-88a0-86d7322aa3cc

                       ;;some songs are only released in one album, even if other songs in the same album areduplicated.
                       (if (and (not= rel-title accrel-title)
                                (> (count accrel-type) 0)
                                (> (count irel-type) 0))
                         (cond (and (some #(= % "Soundtrack") accrel-type)
                                    (some #(= % "Compilation") irel-type))
                               acc
                               (and (some #(= % "Soundtrack") irel-type)
                                    (some #(= % "Compilation") accrel-type))
                               (assoc acc kys ival)
                               (= "India" irel-area)
                               (assoc acc kys ival)
                               (= "India" accrel-area)
                               acc
                               (.contains rel-title accrel-title)
                               acc
                               (.contains accrel-title rel-title )
                               (assoc acc kys ival)
                               :default
                               (do
                                 (println " " rel-title ": " irel-type ":" frid
                                          " second "accrel-title ": " accrel-type ":" srid)
                                 (assoc acc kys ival)))
                         acc))))) {})
       ;;(take 2)
       #_(map (fn[[k i]] (mapv #(i %)
                             [:id :title :first-release-date :disambiguation
                              :rel-evt-date :rel-title :rel-id :rel-type])))
       (map (fn[[k v]] v))
       (sort-by :title)
       (json/write-str)
       ;;(map #(clojure.string/join "," %))
       ;;(clojure.string/join "\n")
       (spit json-file)))

;;(save-recordings-table arr-rel-list "arrrecordingtable.json")
;;(-> arr-rel-list)
;;4696 recordings
;;3335 unique recordings
;;547 releases
;;sometimes there's a local release and worldwide release, each having the same recordings.
;;the release id is different, but the status id is same.
;;some releases (e.g. 127 hours) have different release events in UK and US

;;many songs in recordingtable.csv have duplicate names. Let's remove duplicates if the singers are different.

(defn parse-line-rtcsv
  [line]
  (->> (clojure.string/split line #",")
       (map #(assoc {} %1 %2)
            [:id :title :first-release-date :disambiguation :rel-title :rel-id :rel-type])
       (apply merge)))

(defn remove-songs-with-identical-singers
  "some recordings have identical names.
  Index by song name and remove songs that have identical titles and the same singers"
  [rtcsvmap recid-singers-map]
  (->> (reduce-kv (fn[acc k v]
                    (if (> (count v) 1)
                      (let [rids (map :id v)
                            singers (map #(set (recid-singers-map %)) rids)
                            ret-v (->> (mapv vector singers v)
                                       (reduce (fn[{:keys [singers act] :as iacc} [a b]]
                                                 (if (singers a)
                                                   iacc
                                                   (-> iacc
                                                       (update-in [:singers] conj a)
                                                       (update-in [:act] conj b))))
                                               {:singers #{} :act []})
                                       :act)]
                        ;;(println " ret-v " ret-v)
                        #_(when (> (count v) (count ret-v))
                            (println " filtered items " singers))
                        (assoc acc k ret-v))
                      (assoc acc k v)))
                  {} rtcsvmap)
       vals
       (map first)
       (sort-by :title)))

(defn save-recording-soundtracks
  "remove recodings with identical singers, and only those marked as soundtracks"
  [recordings-table-json track-singers rec-wo-duplicates-csv]
  (let [rtcsvmap
        (->> (json/read-str (slurp recordings-table-json) :key-fn keyword)
             (reduce (fn[acc i]
                       (if (acc (.toLowerCase (:title i)))
                         (update-in acc [(:title i)] conj i)
                         (assoc acc (:title i) [i]))) {}))
        recid-singers-maps
        (->> track-singers
             (apply merge)
             (map (fn[[rec-id v]]
                    (let [ea (extract-artists v) ]
                      {rec-id (->> ea :artists (mapv :name))})))
             (apply merge))
      sdf (new java.text.SimpleDateFormat "yyyy-MM-dd")
      sdf2 (new java.text.SimpleDateFormat "yyyy")
      datefn (fn[i]
               (let [dt (try (.parse sdf (:rel-evt-date i))
                             (catch Exception e
                               (try
                                 (.parse sdf2 (:rel-evt-date i))
                                 (catch Exception e
                                   (try
                                     (.parse sdf (:first-release-date i))
                                     (catch Exception e
                                       (try
                                         (.parse sdf2 (:first-release-date i))
                                         (catch Exception e
                                           (do
                                             (println " failed to parse " (:rel-evt-date i)
                                                      " : " (:first-release-date i)
                                                      i  ) nil)))))))))]
                 (assoc i :date (if dt (.format sdf dt) "-"))))
        cols [:id :title :date :rel-title :rel-id]
        table-data
        (->> (remove-songs-with-identical-singers rtcsvmap recid-singers-maps)
             (map datefn)
             (map #(assoc % :title
                          (if (.startsWith (:title %) "\"")
                            (:title %)
                            (str "\"" (.replace (:title %)"\"" "") "\""))))
             (map #(map (fn[i] (% i)) cols))
             ;;(take 5)
             (map #(clojure.string/join "," %))
             (clojure.string/join "\n"))]
    (spit rec-wo-duplicates-csv
          (str (clojure.string/join ","
                                    (map name [:song_id :song :date :movie :release_id])) "\n" table-data ))))

;;(save-recording-soundtracks "arrrecordingtable.json" track-singers-fin "recordingtable_wo_dupl_singers2.csv")

(defn save-singers-table
  [track-singers unique-recordings-csv singers-csv]
  (let [recid-singers
        (->> track-singers
             (apply merge)
             (map (fn[[rec-id v]]
                    (let [ea (extract-artists v) ]
                      {rec-id (->> ea :artists (map #(select-keys % [:id :name])))})))
             (apply merge))
        table-data
        (->> (clojure.string/split (slurp unique-recordings-csv ) #"\n")
             (map parse-line-rtcsv )
             (map #(let [rid (:id %)]
                     (->> (recid-singers (keyword rid))
                          (mapv :name )
                          (mapv (fn[i](conj [rid] i ))))))
             (reduce into [])
             (map #(clojure.string/join "," %))
             (clojure.string/join "\n"))]
    (spit singers-csv (str (clojure.string/join "," ["song_id" "singer"]) "\n" table-data ))))

(defn regenerate
  [arr-releases-json track-details-json]
  (let [arr-rel-list (json/read-str (slurp arr-releases-json) :key-fn keyword)
        track-singers-fin (json/read-str (slurp track-details-json) :key-fn keyword)]
    (save-song-table track-singers-fin "songtable.csv")
    (save-recordings-table arr-rel-list "arrrecordingtable.json")

    (save-recording-soundtracks "arrrecordingtable.json" track-singers-fin "data/recordings.csv")

    (save-singers-table track-singers-fin "data/recordings.csv" "data/singers.csv")))

(defn update-for-years
  "incremental update by release year, then rebuild all derived csv/json outputs"
  [artist-id start-year end-year releases-json track-details-json]
  (let [[rj tdj] (save-releases-for-years artist-id start-year end-year
                                          releases-json track-details-json)]
    (regenerate rj tdj)))

;;download and save all releases
;;(save-releases arr-artist-id "./arr-releases-5471.json")

;;(def arr-rel-list (json/read-str (slurp "./arr-releases-5471.json") :key-fn keyword))
;;(def all-tracks (get-tracks-in-media arr-rel-list))

;;(def track-singers (get-track-singers all-tracks))
;;(save-track-singers track-singers)

#_(def track-singers-fin
    (json/read-str (slurp "./arr-track-details.json") :key-fn keyword))


;;(save-song-table track-singers-fin "songtable.csv")
;;(save-recordings-table arr-rel-list "arrrecordingtable.csv")

;;(save-recording-soundtracks "arrrecordingtable.json" track-singers-fin "data/recordings.csv")

;;(save-singers-table track-singers-fin "data/recordings.csv" "data/singers.csv")

;;(regenerate "./arr-releases-5471.json" "./arr-track-details.json")

;;incremental update: fetch only releases dated in the given year range, merge
;;into the existing dumps, rebuild the derived outputs
;;(update-for-years arr-artist-id 2025 2026 "./arr-releases-5471.json" "./arr-track-details.json")
;;without rebuilding:
;;(save-releases-for-years arr-artist-id 2025 2026 "./arr-releases-5471.json" "./arr-track-details.json")
