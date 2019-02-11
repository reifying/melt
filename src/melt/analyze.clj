(ns melt.analyze
  (:require [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [melt.channel :as ch]
            [melt.config :refer [db]]
            [melt.jdbc :as mdb]
            [melt.util :refer [mkdirs conform-or-throw]])
  (:import [java.io File]))

(defn sample-file-name [dir-name table]
  (str dir-name File/separator (::ch/schema table) "." (::ch/name table) ".txt"))

(defn sample-writer [dir-name table]
  (io/writer (sample-file-name dir-name table)))

(defn sample-db [schema dir-name]
  (println "Writing sample to" dir-name)
  (mkdirs dir-name)
  (doseq [table schema]
    (with-open [wr (sample-writer dir-name table)]
      (let [name       (mdb/qualified-table-name table)
            sample-sql (str "Select TOP 10 * From " name)
            count-sql  (str "Select count(*) c From " name)]
        (println "Sampling " name)
        (binding [*out* wr]
          (println "Count:" (:c (first (jdbc/query db [count-sql]))))
          (pprint (jdbc/query db [sample-sql])))))))

(defn write-sample
  ([channels] (write-sample channels "target/data-samples"))
  ([channels dir-name] (sample-db channels dir-name)))

(defn -main []
  (write-sample (mdb/schema)))
