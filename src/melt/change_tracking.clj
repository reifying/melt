(ns melt.change-tracking
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :refer [difference]]
            [melt.channel :as ch]
            [melt.config :refer [db]]
            [melt.jdbc :as mdb]))

(defn- enable-change-tracking-sql [db-name]
  (str "ALTER DATABASE " db-name "
        SET CHANGE_TRACKING = ON
        (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)"))

(defn enable-change-tracking [db-name]
  (jdbc/execute! db [(enable-change-tracking-sql db-name)]))

(defn qualified-table-name [table]
  (str (::ch/schema table) "." (::ch/name table)))

(defn track-table-sql [table]
  (str "ALTER TABLE " (qualified-table-name table) "
        ENABLE CHANGE_TRACKING
        WITH (TRACK_COLUMNS_UPDATED = OFF)"))

(defn untrack-table-sql [table]
  (str "ALTER TABLE " (qualified-table-name table) "
        DISABLE CHANGE_TRACKING"))

(defn track-table [table]
  (let [sql (track-table-sql table)]
    (jdbc/execute! db [sql])))

(defn untrack-table [table]
  (let [sql (untrack-table-sql table)]
    (jdbc/execute! db [sql])))

(defn trackable-tables
  ([] (trackable-tables (mdb/cached-schema)))
  ([schema] (filter #(seq (::ch/keys %)) schema)))

(defn list-tracked []
  (map (juxt :schema_name :table_name)
       (jdbc/query db
                   ["Select object_schema_name(object_id) schema_name, 
                            object_name(object_id) table_name 
                     From sys.change_tracking_tables"])))

(defn tracked [schema]
  (let [m (reduce #(assoc %1 ((juxt ::ch/schema ::ch/name) %2) %2)
                  {}
                  schema)]
    (vals (select-keys m (list-tracked)))))

(defn trackable-untracked [schema]
  (let [trackable (trackable-tables schema)
        tracked   (tracked schema)]
    (difference (set trackable) (set tracked))))

(defn track-all [schema]
  (doall (map track-table (trackable-untracked schema))))

(defn print-track-all [schema]
  (doall (map #(do (println (track-table-sql %)) (println "GO")) (trackable-untracked schema))))

(defn untrack-all [schema]
  (doseq [table (tracked schema)] (untrack-table table)))

(defn change-sql [table]
  (String/join " "
               ["Select ct.*"
                "From CHANGETABLE(CHANGES "
                (qualified-table-name table)
                ", ?) As ct Order By ct.sys_change_version"]))

(defn changes [table change-version]
  (jdbc/query db [(change-sql table) change-version]))

(defn min-change-version [table]
  (-> (jdbc/query db ["Select change_tracking_min_valid_version(OBJECT_ID(?)) min_ver"
                      (qualified-table-name table)])
      first
      :min_ver))

