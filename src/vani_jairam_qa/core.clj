(ns vani-jairam-qa.core
  (:require [clj-http.client :as client]
            [clojure.data.json :as json]
            )
  (:gen-class))

(defn till-end
  [ifn]
  @(future
     (Thread/sleep (* (rand-int 5) 1000))
     (try
       (ifn)
       (catch Exception e
         (do
           (println " exception "e)
           {:fail ifn})))))

(def headers {"User-Agent" "Application name/vani-qa ( https://github.com/shark8me)" })
(def arr-artist-id "e0bba708-bdd3-478d-84ea-c706413bedab")

(def releases (str "https://musicbrainz.org/ws/2/release?artist="arr-artist-id ))


(defn get-all-releases
  []
  (loop [acc []
         release-count -1
         offset 0]
    (if (= release-count offset)
      acc
      (let [rel (str "https://musicbrainz.org/ws/2/release?artist="arr-artist-id "&offset=" offset "&inc=recordings+release-groups+artist-rels+recording-rels")
            resp (:body (till-end #(client/get rel {:accept :json :headers headers :as :json})))
            rel-count (:release-count resp)
            idiff (- rel-count (:release-offset resp))
            climb (if (> idiff 25) 25 idiff)]
        (println " rel-count " rel-count " rel-offset " (:release-offset resp))
        (recur (conj acc resp)
               rel-count
               (+ climb offset))))))

(def arr-releases (get-all-releases))
(-> arr-releases last :releases count)
(-> arr-releases last :releases )
(-> arr-releases count)
(spit "./arr-releases.json" (json/write-str arr-releases))

(-> arr-releases second :releases first :media count)
(-> arr-releases second :releases count)
(def rec (client/get releases {:accept :json :headers headers :as :json}))



(-> rec :body :releases first :id)
(-> rec :body)

(def roja-rel (str "https://musicbrainz.org/ws/2/release/"(-> rec :body :releases first :id)"?inc=artists+recordings+collections+labels+release-groups+artist-rels+recording-rels"))

(def roja-rec (client/get roja-rel {:accept :json :headers h :as :json}))
(def chinna-id (-> roja-rec :body :media first :tracks first :recording :id))
(-> roja-rec :body )
(-> roja-rec :body)
(-> chinna-id)
(def chinna-track (str "https://musicbrainz.org/ws/2/recording/" chinna-id "?inc=artists+releases+release-groups+isrcs+url-rels"))
(-> chinna-track)
(def chinna-song (client/get chinna-track {:accept :json :headers h :as :json}))
(-> chinna-song :body )

(defn get-song-details
  [rid]
  (let [track-url (str "https://musicbrainz.org/ws/2/recording/" rid "?inc=artists+artist-rels+recording-rels+release-rels+release-group-rels")
        song (client/get track-url {:accept :json :headers headers :as :json})
        ikeys [:title :first-release-date :id :artist-credit]
        song-body (:body song)]
    song-body))

(def arr-rel-list
  (->> (map :releases arr-releases)
       (reduce into [])))

(first arr-rel-list)
(spit "./arr-releases-547.json" (json/write-str arr-rel-list))
(defn get-tracks-in-media
  "get all the tracks in all :media entries"
  [rel-list]
  (->> rel-list
       (map #(mapv :tracks (:media %)))
       (reduce into [])
       (reduce into [])
       ))

(def all-tracks (get-tracks-in-media arr-rel-list))
(-> all-tracks count)
(->> (map (comp count :media) arr-rel-list) frequencies)
;;{1 535, 2 8, 3 3, 5 1}
(->> (get-tracks-in-media arr-rel-list) count)
;;4696


(defn get-all-track-singers
  [track-map]
  (->> track-map
       (mapv (fn[[k v]]
               {k (assoc v :song-details
                         (let [res (till-end (fn[](get-song-details k)))]
                           (println " " (-> v :title))
                           res))}))))

(def track-map (->> all-tracks (reduce (fn[acc i] (assoc acc (-> i :recording :id) i)) {})))

(def track-singers (get-all-track-singers track-map))
(def failed-get-recordings (->> track-singers
                                (filter #(-> % vals first :song-details :fail))
                                (map keys)
                                (reduce into [])
                                ))

(def track-singers2 (get-all-track-singers (select-keys track-map failed-get-recordings)))
(def k3 (apply merge track-singers2))


(def k2 (apply merge track-singers))

(def track-singers-fin 
  (reduce-kv (fn[ m k v]
               (assoc m k
                      (if (-> v :song-details :fail) (k3 k) v))) {} k2))

(->> track-singers-fin
     (map (fn[[k v]] (-> v :song-details type)))
     frequencies)


(spit "./arr-track-details.json" (json/write-str track-singers-fin))

(defn extract-artists
  [ival]
  (let [artist-credits (mapv #(select-keys (:artist %) [:id :name :disambiguation])
                             (-> ival :song-details :artist-credit ))]
    (-> (select-keys ival [:title ])
        (assoc :artists artist-credits)
        (assoc :first-release-date (-> ival :song-details :first-release-date)))))

(def songs-artists 
  (->> track-singers-fin
       (map (fn[[rec-id v]]
              (let [ea (extract-artists v) ]
                {:song (-> (select-keys ea [:title :first-release-date])
                           (assoc :rec-id rec-id))
                 :artists (->> ea
                               :artists
                               (mapv #(assoc  % :rec-id rec-id)))})))))
;;spit songs out
(->> songs-artists (mapv :song)
     (mapv (fn[i] (clojure.string/join ","
                                       (mapv #(i %)
                                             [:title :first-release-date :rec-id]))))
     (clojure.string/join "\n")
     (spit "songtable.csv"))

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
     (spit "recordingtable.csv")
     )

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

(def rtcsvmap 
  (->> (clojure.string/split (slurp "recordingtable.csv") #"\n")
       (map parse-line-rtcsv )
       (reduce (fn[acc i]
                 (if (acc (:title i))
                   (update-in acc [(:title i)] conj i)
                   (assoc acc (:title i) [i]))) {})))

;;2841 unique song titles
(->> track-singers-fin
     (map (fn[[rec-id v]]
            (let [ea (extract-artists v) ]
              {rec-id (->> ea :artists (mapv :name))})))
     (apply merge)
     (def recid-singers-maps))

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
  [rtcsvmap recid-singers-maps]
  (->> (remove-songs-with-identical-singers rtcsvmap recid-singers-maps)
       (filter #(= "[\"Soundtrack\"]" (:rel-type %)))
       (map #(map (fn[i] (% i))
                  [:id :title :first-release-date :disambiguation :rel-title :rel-id :rel-type]))
       ;;(take 5)
       (map #(clojure.string/join "," %))
       (clojure.string/join "\n")
       (spit "recordingtable_wo_dupl_singers.csv")))

(save-recording-soundtracks rtcsvmap recid-singers-maps)
