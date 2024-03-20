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

(defn get-tracks-in-media
  "get all the tracks in all :media entries"
  [rel-list]
  (->> rel-list
       (map #(mapv :tracks (:media %)))
       (reduce into [])
       (reduce into [])))

;;(def arr-rel-list (json/read-str (slurp "./arr-releases-5471.json") :key-fn keyword))
;;(def all-tracks (get-tracks-in-media arr-rel-list))

(defn get-all-track-singers
  [track-map]
  (->> track-map
       (mapv (fn[[k v]]
               {k (assoc v :song-details
                         (let [res (wait-till (fn[](get-song-details k)))]
                           (println " " (-> res :body :title))
                           (:body res)))}))))

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
  [arr-rel-list csv-file]
  (->> arr-rel-list
       (map #(let [release-id (:id %)
                   rel-title (:title %)
                   stype (-> % :release-group :secondary-types)
                   rel-area (-> % :release-events first :area :name)
                   rel-evt-date (-> % :release-events first :date)]
               (->> (mapv :tracks (:media %))
                    (reduce into [])
                    (map :recording)
                    (map (fn[i] (assoc i :rel-title rel-title :rel-id release-id
                                       :rel-area rel-area
                                       :rel-evt-date rel-evt-date
                                       :rel-type stype))))))
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
       (map (fn[[k i]] (mapv #(i %)
                             [:id :title :first-release-date :disambiguation
                              :rel-title :rel-id :rel-type])))
       (sort-by second)
       (map #(clojure.string/join "," %))
       (clojure.string/join "\n")
       (spit csv-file)))

;;(save-recordings-table arr-rel-list "arrrecordingtable.csv")

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
  [recordings-table-csv track-singers rec-wo-duplicates-csv]
  (let [rtcsvmap
        (->> (clojure.string/split (slurp recordings-table-csv) #"\n")
             (map parse-line-rtcsv )
             (reduce (fn[acc i]
                       (if (acc (:title i))
                         (update-in acc [(:title i)] conj i)
                         (assoc acc (:title i) [i]))) {}))
        recid-singers-maps
        (->> track-singers
             (apply merge)
             (map (fn[[rec-id v]]
                    (let [ea (extract-artists v) ]
                      {rec-id (->> ea :artists (mapv :name))})))
             (apply merge))]
    (->> (remove-songs-with-identical-singers rtcsvmap recid-singers-maps)
         (filter #(= "[\"Soundtrack\"]" (:rel-type %)))
         (map #(map (fn[i] (% i))
                    [:id :title :first-release-date :disambiguation :rel-title :rel-id :rel-type]))
         ;;(take 5)
         (map #(clojure.string/join "," %))
         (clojure.string/join "\n")
         (spit rec-wo-duplicates-csv))))

;;(save-recording-soundtracks "arrrecordingtable.csv" track-singers-fin "recordingtable_wo_dupl_singers2.csv")

(defn save-singers-table
  [track-singers unique-recordings-csv singers-csv]
  (let [recid-singers
        (->> track-singers
             (apply merge)
             (map (fn[[rec-id v]]
                    (let [ea (extract-artists v) ]
                      {rec-id (->> ea :artists (map #(select-keys % [:id :name])))})))
             (apply merge))]
    (->> (clojure.string/split (slurp unique-recordings-csv ) #"\n")
         (map parse-line-rtcsv )
         (map #(let [rid (:id %)]
                   (->> (recid-singers (keyword rid))
                      (mapv :name )
                      (mapv (fn[i](conj [rid] i ))))))
         (reduce into [])
         (map #(clojure.string/join "," %))
         (clojure.string/join "\n")
         (spit singers-csv ))))

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

;;(save-recording-soundtracks "arrrecordingtable.csv" track-singers-fin "recordingtable_wo_dupl_singers2.csv")

#_(save-singers-table track-singers-fin
                    "recordingtable_wo_dupl_singers2.csv"
                    "recording_singers2.csv")
