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

(defn user-schema? [{:keys [table_schem]}]
  (not (= table_schem "sys"))) ; TODO support more than just SQL Server

(defn tables []
  (jdbc/with-db-metadata [md db]
    (filter user-schema?
            (jdbc/metadata-query
             (.getTables md nil nil nil (into-array String ["TABLE"]))
             {:row-fn (fn [m] (select-keys m [:table_schem
                                              :table_name
                                              :table_cat]))}))))

(defn columns [catalogue schema table]
  (jdbc/with-db-metadata [md db]
    (->> (.getColumns md catalogue schema table nil)
         (jdbc/metadata-query)
         (map :column_name))))

(map (juxt :table_cat :table_schem :table_name) (tables))

(defn assoc-columns [table]
  (assoc table :columns (apply columns ((juxt :table_cat
                                              :table_schem
                                              :table_name) table))))

(defn schema []
  "List the tables from non-system schemas. Example result:
  ({:table_schem ...
    :table_name  ...
    :table_cat   ...
    :columns     (col1 col2 col3)} ...)"
  (map assoc-columns (tables)))

(def cached-schema-file (io/as-file "schema.edn"))

(defn cached-schema []
  (if (.exists cached-schema-file)
    (read-string (slurp cached-schema-file))))

(defn save-schema [coll]
  (spit cached-schema-file (pr-str coll)))

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

(defn sample-db [schema file-name]
  (with-open [wr (io/writer file-name)]
    (binding [*out* wr]
      (doseq [table schema]
        (let [name       (str "[" (:table_schem table) "].["
                              (:table_name table) "]")
              sample-sql (str "Select TOP 10 * From " name)
              count-sql  (str "Select count(*) c From " name)]
          (println "Sampling " name ", count: "
                   (:c (first (jdbc/query db [count-sql]))))
          (pprint (jdbc/query db [sample-sql]))
          (println ""))))))

(defn write-sample [file-name]
  (sample-db (schema) file-name))
