(ns melt.analyze
  (:require [clojure.java.jdbc :as jdbc]))

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
  (map assoc-columns (tables)))
