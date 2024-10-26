(ns arr-song-dataset.query
  (:require
            [mount.core :as mount]
            [hugsql.core :as hugsql]
            [clojure.java.jdbc :as jdbc]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-http.client :as client])
  (:import java.text.SimpleDateFormat))

(def spec
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     ":memory:"})

(def db-uri "jdbc:sqlite::memory:")

(declare db)

(defn on-start []
  (let [spec {:connection-uri db-uri}
        conn (jdbc/get-connection spec)]
    (assoc spec :connection conn)))

(defn on-stop []
  (-> db :connection .close)
  nil)

(mount/defstate
  ^{:on-reload :noop}
  db
  :start (on-start)
  :stop (on-stop))

(defn get-rows
  [ifile]
  (with-open [reader (io/reader ifile)]
    (doall
     (csv/read-csv reader))))

(defn parse-date [date-string ]
  (let []
    (.parse (SimpleDateFormat. "yyyy-MM-dd") date-string)))

;;start and connect to the DB
(def m1 (mount/start #'db))

(defn create-tables
  []
  (jdbc/execute! db "create table songs (song_id uuid, song text,release_date date, movie text,release_id uuid, composer text)")
  (jdbc/execute! db "create table singers (song_id uuid, singer text)"))

(defn drop-tables
  []
  (jdbc/execute! db "drop table songs")
  (jdbc/execute! db "drop table singers"))

(comment
  (jdbc/query db ["select * from songs limit 2"])
  (jdbc/query db ["select * from singers"]))
;;(jdbc/execute! db "delete from songs where id > 1 ")
;;(jdbc/execute! db "drop table songs")
;;(jdbc/execute! db "drop table singers")

(def rahman "A. R. Rahman")
(defn insert-songs
  "insert songs and singers into the DB"
  []
  (let [recordings  (get-rows "data/recordings.csv")
        singers  (get-rows "data/singers.csv")
        songs (->>
               (let [headers (mapv keyword (first recordings))]
                 (mapv #(zipmap headers %) (rest recordings)))
               ;;(map #(clojure.set/rename-keys % {:title :song}))
               (map #(-> (assoc % :release_date (try
                                                  (.format (SimpleDateFormat. "YYYY-MM-dd hh:mm:ss"  )
                                                           (parse-date (:date %)))
                                                  (catch Exception e
                                                    (println " caught exception parsing date " % ))))
                         (dissoc :date)))
               ;;(map #(assoc % :release_date (str (:release_date %) " 00:00:00")))
               (remove #(nil? (:release_date %)))
               (mapv #(jdbc/insert! db :songs (assoc % :composer rahman))))
        singers (->> (let [headers (mapv keyword (first singers))]
               (mapv #(zipmap headers %) (rest singers)))
             #_(map #(clojure.set/rename-keys % {:song-id :song_id}))
             (mapv #(jdbc/insert! db :singers %)))]
    [songs singers]))

(comment
  (do ;;(drop-tables)
      (create-tables)
      (insert-songs)))

;;llama payload
(def payload {:model "llama3.2"
              :stream false
              :format :json
              :options {:seed 101, :temperature 0}
              :messages [{:role :system
                          :content "Generate a JSON representation of SQL for the user query.
The table schemas are:
```
create table songs (id uuid, song text,release_date date, movie text, release_id uuid, composer text);
create table singers (song_id uuid, singer text);
```
Replace Rahman, AR Rahman and all similar references with `A. R. Rahman`.
Only use the included table schema to answer the question."}
                         {:role :user
                          :content ""}]})
(defn get-payload
  [query]
  (update-in payload [:messages 1 :content] (constantly query)))

(defn get-raw-response
  [query]
  (let [pay (get-payload query)]
    (let [resp (client/post "http://localhost:11434/api/chat"
                            {:body (json/write-str pay)
                             :content-type :json})
          sql (-> resp :body)]
      sql)))

(defn get-n-responses
  "get 10 responses to the same query"
  [query]
  (mapv (fn[_] (get-raw-response query)) (range 10)))

(defn get-query
  "get the query content from the LLM response"
  [k]
  (-> k (json/read-str :key-fn keyword) :message :content (json/read-str :key-fn keyword) :query))

(defn most-common-type
  "return the most common type among the 10 responses to the same query"
  [iseq]
  (let [f (frequencies (map #(cond  (map? %) :map
                                    (string? %) :string
                                    (nil? %) nil)
                            iseq))]
    (-> (sort-by second f) last first)))

(comment
  ;;test an LLM query
  (def firstmovie (vec (get-n-responses "which is Rahman's first movie?")))
  (-> firstmovie)

  ;;run sql queries
  (jdbc/query db [(-> resp :body (json/read-str :key-fn keyword) :message :content (json/read-str :key-fn keyword) :query)])

  (last (jdbc/query db ["select * from songs order by release_date limit 10"]))


  )

(defn get-query-response
  "given a list of LLM responses and the list of queries, parse the response
  and return a list where each element is a 2-tuple [query, 10-ary-responses]"
  [queries resp]
  (->> resp
       (map #(map get-query %))
       (map #(vector (first %1) %2) queries )))

(defn get-query-response-type
  "return the mosst common type of response for each query
  as a csv. Each line has the query and type"
  [queries resp]
  (->> resp
       (map #(map get-query %))
       (map most-common-type)
       (map #(vector (first %1) %2) queries )
       (filterv #(nil? (second %)))
       (map #(clojure.string/join "," %))
       (clojure.string/join "\n")))

;(def queries  (get-rows "data/queriesv2.csv"))
;(def k1 (->> queries (map first) (mapv get-n-responses)))
;(def k2 (->> queries (map first) (mapv get-n-responses) vec))
;(-> k2)

;;(spit "query_response.txt" (get-query-responses queries k1))

;;(spit "query_type_response.csv" (get-query-response-type queries k1))
;;(spit "queryoutputs.txt" (get-query-response queries k1))
(defn getf
  [i]
  (-> i first vals first))

(defn checkeq
  ([eq-fn [text que]]
   (let [ires (jdbc/query db que)]
     (println " query resp " ires)
     (if (and (= 1 (count ires)) (eq-fn ires))
       true
       (if (and (= 1 (count ires)) (= 1 (count (first ires)))) (getf ires) ires)))))

(checkeq
 #(= 2358 (getf %))
 ["how many songs has Rahman composed?" ["select count(*) from songs where composer like ?" rahman] ])

(checkeq
 #(= 91 (getf %))
 ["how many songs has Rahman composed in 2005?"
  ["select count(distinct song) from songs where composer like ? and strftime('%Y',release_date)= ? " rahman "2005"]])

(checkeq (fn[j]
           (let [i (first j)]
             (and (= 140 (:number_of_songs i)) (= (:year i) "1994"))))
          ["which year did Rahman compose the most songs"
           [ "select count(distinct song) as number_of_songs,strftime('%Y',release_date) as year from songs  where composer like ? group by strftime('%Y',release_date) order by count(strftime('%Y',release_date)) desc limit 1" rahman]])

(checkeq
 (fn[j]
   (let [i (first j)]
     (and (= 7 (:number_of_songs i)) (= (:year i) "1991"))))
 ["which year did Rahman compose the least songs"
           ["select count(distinct song) as number_of_songs ,strftime('%Y',release_date) as year from songs  where composer like ? group by strftime('%Y',release_date) order by count(strftime('%Y',release_date)) asc limit 1" rahman]])

(checkeq (fn[j]
           (let [i (first j)]
             (and (= 19 (:number_of_movies i)) (= (:year i) "1994"))))
         ["which year did Rahman compose the most movies"
          ["select count(distinct movie) as number_of_movies,strftime('%Y',release_date) as year from songs  where composer like ? group by strftime('%Y',release_date) order by count(strftime('%Y',release_date)) desc limit 1" rahman]])

(checkeq
 (fn[j]
   (let [i (first j)]
     (and (= 1 (:number_of_movies i)) (= (:year i) "1991"))))
 ["which year did Rahman compose the least movies"
  ["select count(distinct movie) as number_of_movies,strftime('%Y',release_date) as year from songs  where composer like ? group by strftime('%Y',release_date) order by count(strftime('%Y',release_date)) asc limit 1" rahman]])

(checkeq
 (fn[j]
   (let [i (first j)]
     (and (= 29 (:number_of_songs i)) (= (:movie i) "Roja"))))
 ["which movie by Rahman had the most songs?"
  ["select count(distinct song) as number_of_songs,movie from songs  where composer like ? group by movie order by count(distinct song) desc limit 1" rahman]])

(checkeq
 (fn[j]
   (let [i (first j)]
     (and (= 1 (:number_of_songs i)) (= (:movie i) "Aalaporaan Thamizhan (From \"Mersal\")"))))
 ["which movie had the least songs?"
  ["select count(distinct song) as number_of_songs,movie from songs  where composer like ? group by movie order by count(distinct song) asc limit 1" rahman]])

(checkeq
 (fn[j]
   (println " j " j)
   (let [i (first j)]
     (= 8 (:number_of_movies i))))
 ["how many movies has Rahman composed in 2005?"
  ["select count(distinct movie) as number_of_movies from songs where composer like ? and strftime('%Y',release_date)=?" rahman "2005"]])

;;doesn't work
(checkeq
 (fn[j]
   (println " j " j)
   (let [i (first j)]
     (= (set '({:movie "Netaji Subhas Chandra Bose: The Forgotten Hero"}
              {:movie "Water"}
              {:movie "Mangal Pandey: The Rising"}
              {:movie "Anbe Aaruyire"}
              {:movie "Kisna"}
              {:movie "Rang De Basanti"}
              {:movie "Kisna - The Warrior Poet"}
               {:movie "Ah Aah"}))
        (set i))))
 ["which movies did Rahman composed in 2005?"
  ["select distinct movie from songs where composer like ? and strftime('%Y',release_date)=? " rahman "2005"]])

;;
(jdbc/query db [(str )])
;;({:movie "Netaji Subhas Chandra Bose: The Forgotten Hero"} {:movie "Water"} {:movie "Mangal Pandey: The Rising"} {:movie "Anbe Aaruyire"} {:movie "Kisna"} {:movie "Rang De Basanti"} {:movie "Kisna - The Warrior Poet"} {:movie "Ah Aah"})

;;which year was Swades released?
(jdbc/query db [(str "select strftime('%Y',release_date) from songs where composer like '" rahman "' and movie like 'Swades' limit 1")])
;;({:strftime('%y',release_date) "2004"})

;;list all the songs in the movie Swades 
(jdbc/query db [(str "select song from songs where composer like '" rahman "' and movie like 'Swades'")])
;;({:song "Aahista Aahista"} {:song "Dekho Na"} {:song "Pal Pal Hai Bhaari"} {:song "Pal Pal Hai Bhaari (instrumental)"} {:song "Saanwariya Saanwariya"} {:song "Yeh Jo Des Hai Tera (instrumental)"} {:song "Yeh Tara Woh Tara"})

;;list 3 songs in the movie Swades
(jdbc/query db [(str "select song from songs where composer like '" rahman "' and movie like 'Swades' limit 3")])
;;({:song "Aahista Aahista"} {:song "Dekho Na"} {:song "Pal Pal Hai Bhaari"})

;;when the movie was Swades released?
(jdbc/query db [(str "select release_date from songs where composer like '" rahman "' and movie like 'Swades' limit 1")])
;;({:release_date "2004-09-24 12:00:00"})

;;which movie is the song "Dheemi Dheemi" from?
(jdbc/query db [(str "select movie from songs where composer like '" rahman "' and song like 'Dheemi Dheemi'")])
;;({:movie "1947 Earth"})

;;when was the the song "Dheemi Dheemi" released?
(jdbc/query db [(str "select release_date from songs where composer like '" rahman "' and song like 'Dheemi Dheemi'")])
;;({:release_date "1998-01-01 12:00:00"})

;;who sang the song Aaj ki raat
(= '({:singer "Alisha Chinai"} {:singer "Mahalakshmi Iyer"} {:singer "Sonu Nigam"})
   (jdbc/query db [(str "select si.singer from songs s inner join singers si on s.song_id = si.song_id where s.composer like '" rahman "' and s.song like 'Aaj ki raat'")]))

;;how many singers sang in 'Aaj ki raat
(jdbc/query db [(str "select count(si.singer) from songs s inner join singers si on s.song_id = si.song_id where s.composer like '" rahman "' and s.song like 'Aaj ki raat'")])
;;({:count(si.singer) 3})

;;how many song did Sonu nigam sing for rahman
(jdbc/query db [(str "select count(distinct s.song) from songs s inner join singers si on s.song_id = si.song_id where s.composer like '" rahman "' and si.singer like 'Sonu Nigam'")])
;;({:count(distinct s.song) 21})

;;which songs did Sonu nigam sing for rahman?
;;({:song "Aaj ki raat"} {:song "Desh Ki Mitti"} {:song "Ekla Chalo"} {:song "Gaya Gaya Dil"} {:song "Gaya Gaya Dil - Remix"} {:song "Gulfisha"} {:song "Gum Sum"} {:song "Guzarish"} {:song "Hawa Sun Hawa"} {:song "In Lamhon Ke Daaman Mein"} {:song "Main Badhiya Tu Bhi Badhiya"} {:song "Matam Mastam"} {:song "Offho Jalta Hai"} {:song "Raave Naa Chaliyaa"} {:song "Satrangi re"} {:song "Shabba Shabba"} {:song "Shanno"} {:song "Tu Fiza Hai"} {:song "Tu Fiza Hai - Remix"} {:song "Varayo Thozhi"} {:song "Yeh Dil To Mila Hai"})
(jdbc/query db [(str "select distinct s.song from songs s inner join singers si on s.song_id = si.song_id where s.composer like '" rahman "' and si.singer like 'Sonu Nigam'")])

;;which was the last song that Sonu nigam sang for Rahman
;;({:song "Main Badhiya Tu Bhi Badhiya", :release_date "2018-06-29 12:00:00"})
(jdbc/query db [(str "select distinct s.song,s.release_date from songs s inner join singers si on s.song_id = si.song_id where s.composer like '" rahman "' and si.singer like 'Sonu Nigam' order by s.release_date desc limit 1")])

;;which was the first song that Sonu nigam sang for Rahman
;;({:singer "A. R. Rahman", :count(si.singer) 700} {:singer "S. P. Balasubrahmanyam", :count(si.singer) 134} {:singer "K. S. Chithra", :count(si.singer) 99} {:singer "Sujatha", :count(si.singer) 69} {:singer "Hariharan", :count(si.singer) 63} {:singer "Srinivas", :count(si.singer) 57} {:singer "Swarnalatha", :count(si.singer) 54} {:singer "Mano", :count(si.singer) 54} {:singer "Shreya Ghoshal", :count(si.singer) 49} {:singer "Karthik", :count(si.singer) 48})
(jdbc/query db [(str "select si.singer,count(si.singer) from songs s inner join singers si on s.song_id = si.song_id where s.composer like '" rahman "' group by si.singer order by count(si.singer) desc limit 10")])

;;how many singers has rahman  used so far?
;;({:count(distinct si.singer) 527})
(jdbc/query db [(str "select count(distinct si.singer) from songs s inner join singers si on s.song_id = si.song_id where s.composer like '" rahman "' ")])

