(ns jdbc.melt.sql-server
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :refer [difference union]]
            [clojure.spec.alpha :as spec]
            [clojure.string :refer [join]]
            [jdbc.melt :as melt])
  (:import [org.apache.kafka.clients.producer ProducerRecord]))

(defn- enable-change-tracking-sql [db-name]
  (str "ALTER DATABASE " db-name " SET CHANGE_TRACKING = ON
        (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)"))

(defn enable-change-tracking [db db-name]
  (jdbc/execute! db [(enable-change-tracking-sql db-name)]))

(defn qualified-table-name [table]
  (str (::melt/schema table) "." (::melt/name table)))

(defn track-table-sql [table]
  (str "ALTER TABLE " (qualified-table-name table) " ENABLE CHANGE_TRACKING
        WITH (TRACK_COLUMNS_UPDATED = OFF)"))

(defn untrack-table-sql [table]
  (str "ALTER TABLE " (qualified-table-name table) " DISABLE CHANGE_TRACKING"))

(defn track-table [db table]
  (jdbc/execute! db [(track-table-sql table)]))

(defn untrack-table [db table]
  (jdbc/execute! db [(untrack-table-sql table)]))

(defn trackable? [table]
  (boolean (seq (::melt/keys table))))

(defn list-tracked [db]
  (map (juxt :schema_name :table_name)
       (jdbc/query db
                   ["Select object_schema_name(object_id) schema_name, 
                            object_name(object_id) table_name 
                     From sys.change_tracking_tables"])))

(defn tracked [db schema]
  (let [m (reduce #(assoc %1 ((juxt ::melt/schema ::melt/name) %2) %2)
                  {}
                  schema)]
    (vals (select-keys m (list-tracked db)))))

(defn trackable-untracked [db schema]
  (let [trackable (filter trackable? schema)
        tracked   (tracked db schema)]
    (difference (set trackable) (set tracked))))

(defn track-all [db schema]
  (doall (map track-table (trackable-untracked db schema))))

(defn print-track-all [db schema]
  (doall (map #(do (println (track-table-sql %)) (println "GO"))
              (trackable-untracked db schema))))

(defn untrack-all [db schema]
  (doseq [table (tracked db schema)] (untrack-table db table)))

(defn change-sql [table]
  (String/join " "
               ["Select ct.*"
                "From CHANGETABLE(CHANGES "
                (qualified-table-name table)
                ", ?) As ct Order By ct.sys_change_version"]))

(def tracking-fields #{:sys_change_operation
                       :sys_change_version
                       :sys_change_creation_version
                       :sys_change_columns
                       :sys_change_context})

(defn- select-fields [table]
  (let [ks (set (::melt/keys table))]
    (join ", "
          (flatten
           [(map #(str "ct." (name %))
                 (union tracking-fields ks))
            (map #(str "t." (name %))
                 (difference (set (::melt/columns table)) ks))]))))

(defn change-entity-sql [table]
  (let [table-name (qualified-table-name table)]
    (join " "
          ["Select"
           (select-fields table)
           "From CHANGETABLE(CHANGES" table-name ", ?) As ct"
           "Left Outer Join " table-name "t On "
           (join " And "
                 (map #(str "ct.[" (name %) "] = t.[" (name %) "]")
                      (::melt/keys table)))
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
  (let [#::melt{:keys [topic key value]}
        (spec/assert ::melt/message message)]
    (.send producer (ProducerRecord. topic key value))
    message))

(defn- relocate-tracking-fields [message]
  (merge (apply update message ::melt/value dissoc tracking-fields)
         (select-keys (get message ::melt/value) tracking-fields)))

(defn- tombstone [message]
  (if (= "D" (:sys_change_operation message))
    (assoc message ::melt/value nil)
    message))

(defn send-changes
  "Query change tracking, starting at change version `ver`, and send to Kafka.
   Returns new version"
  [p-spec db source ver]
  (melt/with-producer [p-spec p-spec]
    (let [p (melt/producer p-spec)
          s (assoc source ::melt/sql-params [(change-entity-sql source) ver])
          v (get (last (eduction (map (partial melt/message s))
                                 (map relocate-tracking-fields)
                                 (map tombstone)
                                 (melt/xform s)
                                 (map (partial send-message p))
                                 (melt/query-source db s)))
                 :sys_change_version
                 ver)]
      (.flush p)
      v)))

(defn sync-kafka
  "Perform full sync and return latest change version"
  [c-spec p-spec db source]
  (melt/with-producer [p-spec p-spec]
    (melt/with-consumer [c-spec c-spec]
      (let [ver (current-version db)]
        (melt/sync-kafka db c-spec p-spec source)
        (send-changes p-spec db source ver)))))
