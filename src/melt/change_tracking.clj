(ns melt.change-tracking
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :refer [difference]]
            [clojure.spec.alpha :as spec]
            [clojure.string :refer [join]]
            [melt.config :refer [db]]
            [melt.jdbc :as mdb]
            [melt.kafka :as k]
            [melt.source :as source]
            [melt.sync :as s])
  (:import [org.apache.kafka.clients.producer ProducerRecord])
  (:refer-clojure :exclude [sync]))

(defn- enable-change-tracking-sql [db-name]
  (str "ALTER DATABASE " db-name "
        SET CHANGE_TRACKING = ON
        (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)"))

(defn enable-change-tracking [db-name]
  (jdbc/execute! db [(enable-change-tracking-sql db-name)]))

(defn qualified-table-name [table]
  (str (::source/schema table) "." (::source/name table)))

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

(defn trackable? [table]
  (seq (::source/keys table)))

(defn trackable-tables
  "List tables that are eligible for change tracking. (Those without
   primary keys are not.)"
  ([] (trackable-tables (mdb/cached-schema)))
  ([schema] (filter trackable? schema)))

(defn list-tracked []
  (map (juxt :schema_name :table_name)
       (jdbc/query db
                   ["Select object_schema_name(object_id) schema_name, 
                            object_name(object_id) table_name 
                     From sys.change_tracking_tables"])))

(defn tracked [schema]
  (let [m (reduce #(assoc %1 ((juxt ::source/schema ::source/name) %2) %2)
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

(defn change-entity-sql [table]
  (let [table-name (qualified-table-name table)]
    (join " "
          ["Select ct.sys_change_operation, ct.sys_change_version,"
           "ct.sys_change_creation_version, ct.sys_change_columns,"
           "ct.sys_change_context, t.*"
           "From CHANGETABLE(CHANGES" table-name ", ?) As ct"
           "Left Outer Join " table-name "t On "
           (join " And "
                 (map #(str "ct.[" (name %) "] = t.[" (name %) "]")
                      (::source/keys table)))
           "Order By ct.sys_change_version"])))

(defn changes [db table change-version]
  (jdbc/query db [(change-sql table) change-version]))

(defn min-change-version [db table]
  (-> (jdbc/query db ["Select change_tracking_min_valid_version(OBJECT_ID(?)) min_ver"
                      (qualified-table-name table)])
      first
      :min_ver))

(defn current-version [db]
  (-> (jdbc/query db ["Select change_tracking_current_version() cur_ver"])
      first
      :cur_ver))

(defn- send-message [producer message]
  (let [#::source{:keys [topic key value]}
        (spec/assert ::source/message message)]
    (.send producer (ProducerRecord. topic key value))
    message))

(def tracking-fields [:sys_change_operation
                      :sys_change_version
                      :sys_change_creation_version
                      :sys_change_columns
                      :sys_change_context])

(defn- relocate-tracking-fields [message]
  (merge (apply update message ::source/value dissoc tracking-fields)
         (select-keys (get message ::source/value) tracking-fields)))

(defn- reduce-change-version [_ message]
  (get message :sys_change_version))

(defn send-changes
  "Query change tracking, starting at change version `ver`, and send to Kafka.
   Returns new version"
  [p-spec db source ver]
  (k/with-producer [p-spec p-spec]
    (let [p (k/producer p-spec)
          s (assoc source ::source/sql-params [(change-entity-sql source) ver])]
      (transduce (comp (map (partial source/message s))
                       (map relocate-tracking-fields)
                       (source/xform s)
                       (map (partial send-message p)))
                 (completing reduce-change-version)
                 ver
                 (mdb/reducible-source db s))
      (.flush p))))

(defn sync
  "Perform full sync and return latest change version"
  [c-spec p-spec db source]
  (k/with-producer [p-spec p-spec]
    (k/with-consumer [c-spec c-spec]
      (let [ver (current-version db)]
        (s/sync db c-spec p-spec source)
        (send-changes p-spec db source ver)))))
