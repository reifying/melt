(ns melt.analyze
  (:require [clojure.data :as data]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [melt.config :refer [db ignorable-schemas schema-file-path]]
            [melt.util :refer [mkdirs]])
  (:import [java.io File]))

(defn user-schema? [{:keys [table_schem]}]
  (not (contains? ignorable-schemas table_schem)))

(defn table [{:keys [table_schem table_cat table_name]}]
  {:name   table_name
   :cat    table_cat
   :schema table_schem})

(defn group-by-table
  [table-map column-map]
  (update table-map
          (table column-map)
          (fn [columns] (conj columns (:column_name column-map)))))

(defn table-set []
  (set (jdbc/with-db-metadata [md db]
    (->> (.getTables md nil nil nil (into-array String ["TABLE"]))
         (jdbc/metadata-query)
         (filter user-schema?)
         (map table)))))

(defn contains-table? [table-set column-map]
  (contains? table-set (table column-map)))

(defn schema []
  (let [table-set (table-set)]
  (jdbc/with-db-metadata [md db]
    (->> (.getColumns md nil nil "%" nil)
         (jdbc/metadata-query)
         (filter (partial contains-table? table-set))
         (reduce group-by-table {})))))

(defn cached-schema-file []
  (let [f (io/as-file schema-file-path)]
    (mkdirs (.getParent f))
    f))

(defn cached-schema []
  (let [f (cached-schema-file)]
    (if (.exists f)
      (read-string (slurp f)))))

(defn save-schema
  ([] (save-schema (schema)))
  ([coll] (spit (cached-schema-file) (pr-str coll))))

(defn schema-diff []
  (let [cached (cached-schema)
        latest (schema)
        diff   (data/diff cached latest)]
    {:only-old   (first diff)
     :only-new   (second diff)
     :new-schema latest}))

(defn schema-changed? [diff]
  (some some? (vals (select-keys diff [:only-new :only-old]))))

(defn schema-check []
  (let [diff (schema-diff)]
    (if (and (schema-changed? diff)
             (= "TRUE" (System/getenv "ABORT_ON_SCHEMA_CHANGE")))
      false
      diff)))

(defn sample-file-name [dir-name table]
  (str dir-name File/separator (:schema table) "." (:name table) ".txt"))

(defn sample-writer [dir-name table]
  (io/writer (sample-file-name dir-name table)))

(defn sample-db [schema dir-name]
  (println "Writing sample to" dir-name)
  (mkdirs dir-name)
  (doseq [table (keys schema)]
    (with-open [wr (sample-writer dir-name table)]
      (let [name       (str "[" (:schema table) "].[" (:name table) "]")
            sample-sql (str "Select TOP 10 * From " name)
            count-sql  (str "Select count(*) c From " name)]
        (println "Sampling " name)
        (binding [*out* wr]
          (println "Count: "
                   (:c (first (jdbc/query db [count-sql]))))
          (pprint (jdbc/query db [sample-sql])))))))

(defn write-sample
  ([] (write-sample "target/data-samples"))
  ([dir-name] (sample-db (schema) dir-name)))

(defn -main []
  (write-sample))