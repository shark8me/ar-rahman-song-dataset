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
  (jdbc/execute! db "create table songs (id uuid, song text,release_date date, movie text,release_id uuid, composer text)")
  (jdbc/execute! db "create table singers (song_id uuid, singer text)"))

(comment
  (jdbc/query db ["select * from songs"])
  (jdbc/query db ["select * from singers"]))
;;(jdbc/execute! db "delete from songs where id > 1 ")
;;(jdbc/execute! db "delete from singers where song_id > 1 ")

(def rahman "A. R. Rahman")
(defn insert-songs
  "insert songs and singers into the DB"
  []
  (let [recordings  (get-rows "data/recordings.csv")
        singers  (get-rows "data/singers.csv")
        songs (->>
               (let [headers (mapv keyword (first recordings))]
                 (mapv #(zipmap headers %) (rest recordings)))
               (map #(clojure.set/rename-keys % {:song-id :id :release-id :release_id :release-date :release_date}))
               (map #(assoc % :release_date (try
                                              (.format (SimpleDateFormat. "YYYY-MM-dd hh:mm:ss"  )
                                                       (parse-date (:release_date %)))
                                              (catch Exception e
                                                (println " caught exception parsing date " % )))))
               ;;(map #(assoc % :release_date (str (:release_date %) " 00:00:00")))
               (remove #(nil? (:release_date %)))
               (mapv #(jdbc/insert! db :songs (assoc % :composer rahman))))
        singers (->> (let [headers (mapv keyword (first singers))]
               (mapv #(zipmap headers %) (rest singers)))
             (map #(clojure.set/rename-keys % {:song-id :song_id}))
             (mapv #(jdbc/insert! db :singers %)))]
    [songs singers]))

(comment
  (do (create-tables)
      (insert-songs))
  )
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

(def queries  (get-rows "data/queriesv2.csv"))
(def k1 (->> queries (map first) (mapv get-n-responses)))
(def k2 (->> queries (map first) (mapv get-n-responses) vec))
(-> k2)

;;(spit "query_response.txt" (get-query-responses queries k1))

;;(spit "query_type_response.csv" (get-query-response-type queries k1))
;;(spit "queryoutputs.txt" (get-query-response queries k1))

;;how many songs has Rahman composed?
(jdbc/query db [(str "select count(*) from songs where composer like '" rahman "'")])
;;({:count(*) 2278})

;;how many songs has Rahman composed in 2005?
(jdbc/query db [(str "select count(distinct song) from songs where composer like '" rahman "' and strftime('%Y',release_date)='2005' ")])
;;({:count(distinct song) 87})

;;which year did Rahman compose the most songs
(jdbc/query db [(str "select count(distinct song),strftime('%Y',release_date) from songs  where composer like '" rahman "' group by strftime('%Y',release_date) order by count(strftime('%Y',release_date)) desc limit 1")])
;;({:count(distinct song) 129, :strftime('%y',release_date) "1999"})

;;which year did Rahman compose the least songs
(jdbc/query db [(str "select count(distinct song),strftime('%Y',release_date) from songs  where composer like '" rahman "' group by strftime('%Y',release_date) order by count(strftime('%Y',release_date)) asc limit 1")])
;;({:count(distinct song) 11, :strftime('%y',release_date) "2024"})

;;which year did Rahman compose the most movies
(jdbc/query db [(str "select count(distinct movie),strftime('%Y',release_date) from songs  where composer like '" rahman "' group by strftime('%Y',release_date) order by count(strftime('%Y',release_date)) desc limit 1")])
;;({:count(distinct movie) 16, :strftime('%y',release_date) "1999"})

;;which year did Rahman compose the least movies
(jdbc/query db [(str "select count(distinct movie),strftime('%Y',release_date) from songs  where composer like '" rahman "' group by strftime('%Y',release_date) order by count(strftime('%Y',release_date)) asc limit 1")])
;;({:count(distinct movie) 3, :strftime('%y',release_date) "2024"})

;;which movie had the most songs?
(jdbc/query db [(str "select count(distinct song),movie from songs  where composer like '" rahman "' group by movie order by count(distinct song) desc limit 1")])
;;({:count(distinct song) 63, :movie "Best of A.R. Rahman: Melody King"})

;;which movie had the least songs?
(jdbc/query db [(str "select count(distinct song),movie from songs  where composer like '" rahman "' group by movie order by count(distinct song) asc limit 1")])
;;({:count(distinct song) 1, :movie "Aalaporaan Thamizhan (From \"Mersal\")"})

;;how many movies has Rahman composed for?
(jdbc/query db [(str "select count(distinct movie) from songs where composer like '" rahman "'")])
;;({:count(distinct movie) 297})

;;how many movies has Rahman composed in 2005?
(jdbc/query db [(str "select count(distinct movie) from songs where composer like '" rahman "' and strftime('%Y',release_date)='2005' ")])
;;({:count(distinct movie) 8})

;;which movies did Rahman composed in 2005?
(jdbc/query db [(str "select distinct movie from songs where composer like '" rahman "' and strftime('%Y',release_date)='2005' ")])
;;({:movie "Netaji Subhas Chandra Bose: The Forgotten Hero"} {:movie "Water"} {:movie "Mangal Pandey: The Rising"} {:movie "Anbe Aaruyire"} {:movie "Kisna"} {:movie "Rang De Basanti"} {:movie "Kisna - The Warrior Poet"} {:movie "Ah Aah"})

;;which year was Swades released?
(jdbc/query db [(str "select strftime('%Y',release_date) from songs where composer like '" rahman "' and movie like 'Swades' limit 1")])
;;({:strftime('%y',release_date) "2004"})

;;list all the songs in the movie Swades 
(jdbc/query db [(str "select song from songs where composer like '" rahman "' and movie like 'Swades'")])
;;({:song "Aahista Aahista"} {:song "Dekho Na"} {:song "Pal Pal Hai Bhaari"} {:song "Pal Pal Hai Bhaari (instrumental)"} {:song "Saanwariya Saanwariya"} {:song "Yeh Jo Des Hai Tera (instrumental)"} {:song "Yeh Tara Woh Tara"})

;;when the movie was Swades released?
(jdbc/query db [(str "select release_date from songs where composer like '" rahman "' and movie like 'Swades' limit 1")])
;;({:release_date "2004-09-24 12:00:00"})

;;which movie is the song "Dheemi Dheemi" from?
(jdbc/query db [(str "select movie from songs where composer like '" rahman "' and song like 'Dheemi Dheemi'")])
;;({:movie "1947 Earth"})

;;when was the the song "Dheemi Dheemi" released?
(jdbc/query db [(str "select release_date from songs where composer like '" rahman "' and song like 'Dheemi Dheemi'")])
;;({:release_date "1998-01-01 12:00:00"})


