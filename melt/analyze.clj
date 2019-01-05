(ns melt.analyze
  (:require [clojure.data :as data]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]))

(def mssql-host   (System/getenv "TEST_MSSQL_HOST"))
(def mssql-port   (or (System/getenv "TEST_MSSQL_PORT") "1433"))
(def mssql-user   (System/getenv "TEST_MSSQL_USER"))
(def mssql-pass   (System/getenv "TEST_MSSQL_PASS"))
(def mssql-dbname (System/getenv "TEST_MSSQL_NAME"))
(def jtds-host    (or (System/getenv "TEST_JTDS_HOST") mssql-host))
(def jtds-port    (or (System/getenv "TEST_JTDS_PORT") mssql-port))
(def jtds-user    (or (System/getenv "TEST_JTDS_USER") mssql-user))
(def jtds-pass    (or (System/getenv "TEST_JTDS_PASS") mssql-pass))
(def jtds-dbname  (or (System/getenv "TEST_JTDS_NAME") mssql-dbname))

(def db {:dbtype   "jtds"
         :dbname   jtds-dbname
         :host     jtds-host
         :port     jtds-port
         :user     jtds-user
         :password jtds-pass})

(def ignorable-schemas #{"sys" "INFORMATION_SCHEMA"})

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

(def cached-schema-file (io/as-file "schema.edn"))

(defn cached-schema []
  (if (.exists cached-schema-file)
    (read-string (slurp cached-schema-file))))

(defn save-schema
  ([] (save-schema (schema)))
  ([coll] (spit cached-schema-file (pr-str coll))))

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
  (str dir-name
       java.io.File/separator
       (:schema table) "." (:name table) ".txt"))

(defn sample-writer [dir-name table]
  (io/writer (sample-file-name dir-name table)))

(defn sample-db [schema dir-name]
  (println "Writing sample to" dir-name)
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
  ([] (let [dir-name "target/data-samples"
            dir      (java.io.File. dir-name)]
        (.mkdirs dir)
        (write-sample dir-name)))
  ([dir-name] (sample-db (schema) dir-name)))

(write-sample)