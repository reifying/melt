(ns melt.analyze
  (:require [clojure.data :as data]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [melt.config :refer [db ignorable-schemas schema-file-path
                                 abort-on-schema-change table->topic-name]]
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
  (update-in table-map
             [(table column-map) :columns]
             (fn [columns] (conj columns (:column_name column-map)))))

(defn primary-keys [table]
  (jdbc/with-db-metadata [md db]
    (->> (.getPrimaryKeys md (:cat table) (:schema table) (:name table))
         jdbc/metadata-query
         (map (comp keyword clojure.string/lower-case :column_name)))))

(defn table-set []
  (set (jdbc/with-db-metadata [md db]
         (->> (.getTables md nil nil nil (into-array String ["TABLE"]))
              jdbc/metadata-query
              (filter user-schema?)
              (map table)))))

(defn contains-table? [table-set column-map]
  (contains? table-set (table column-map)))

(defn schema []
  (let [table-set (table-set)]
    (jdbc/with-db-metadata [md db]
      (->> (.getColumns md nil nil "%" nil)
           jdbc/metadata-query
           (filter (partial contains-table? table-set))
           (reduce group-by-table {})
           (reduce-kv #(assoc %1 %2 (assoc %3 :keys (primary-keys %2))) {})))))

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
             (= "TRUE" abort-on-schema-change))
      false
      diff)))

(defn sample-file-name [dir-name table]
  (str dir-name File/separator (:schema table) "." (:name table) ".txt"))

(defn sample-writer [dir-name table]
  (io/writer (sample-file-name dir-name table)))

(defn qualified-table-name [{:keys [schema name]}]
  (str "[" schema "].[" name "]"))

(defn sample-db [schema dir-name]
  (println "Writing sample to" dir-name)
  (mkdirs dir-name)
  (doseq [table (keys schema)]
    (with-open [wr (sample-writer dir-name table)]
      (let [name       (qualified-table-name table)
            sample-sql (str "Select TOP 10 * From " name)
            count-sql  (str "Select count(*) c From " name)]
        (println "Sampling " name)
        (binding [*out* wr]
          (println "Count: "
                   (:c (first (jdbc/query db [count-sql]))))
          (pprint (jdbc/query db [sample-sql])))))))

(defn entry->table [table-entry]
  (apply merge table-entry))

(defn select-all-sql [table-entry]
  (let [sql (str "Select * From "
                 (qualified-table-name (entry->table table-entry)))]
    (println sql)
    sql))

(defn select-row-keys [table row]
  (select-keys row (:keys (second table))))

(defn read-table [table]
  (letfn [(merge-by-key [m row] (assoc m (select-row-keys table row) row))]
    (reduce merge-by-key {} (jdbc/query db [(select-all-sql table)]))))

(defn write-sample
  ([] (write-sample "target/data-samples"))
  ([dir-name] (sample-db (schema) dir-name)))

(defn schema->topic-names [schema table-topic-fn]
  (map table-topic-fn (keys schema)))

(defn topic-names []
  (schema->topic-names (cached-schema) table->topic-name))

(defn -main []
  (write-sample))