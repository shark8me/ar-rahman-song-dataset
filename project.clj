(defproject ar-rahman-songs-dataset "0.1.0-SNAPSHOT"
  :description "Create a dataset of ARR songs"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/data.json "2.5.0"]
                 [org.xerial/sqlite-jdbc "3.43.0.0"]
                 [clj-http "3.12.3"]
                 [org.clojure/java.jdbc "0.7.0"]
                 [com.layerware/hugsql "0.4.7"]
                 [mount "0.1.11"]
                 [org.clojure/data.csv "1.1.0"]
                 ]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
