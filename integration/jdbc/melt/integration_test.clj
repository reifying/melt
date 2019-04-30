(ns jdbc.melt.integration-test
  (:require [clojure.java.jdbc :as jdbc]
            [jdbc.melt :as melt]
            [jdbc.melt.sql-server :as s]
            [midje.sweet :refer [facts fact => contains]]))

;; Tests are dependent on each other. order matters and predecessor tests failing
;; will likely cause successors to fail.

(def host   (System/getenv "MELT_DB_HOST"))
(def port   (or (System/getenv "MELT_DB_PORT") "1433"))
(def user   (System/getenv "MELT_DB_USER"))
(def pass   (System/getenv "MELT_DB_PASS"))
(def dbname (System/getenv "MELT_DB_NAME"))

(def db {:dbtype   "jtds"
         :dbname   dbname
         :host     host
         :port     port
         :user     user
         :password pass})

(def bootstrap-servers
  (str (or (System/getenv "MELT_KAFKA_HOST") "localhost") ":9092"))

(def serializer "jdbc.melt.serdes.Serializer")

(def producer-props
  (doto (java.util.Properties.)
    (.put "bootstrap.servers" bootstrap-servers)
    (.put "acks" "all")
    (.put "key.serializer" serializer)
    (.put "value.serializer" serializer)))

(def deserializer "jdbc.melt.serdes.Deserializer")

(def consumer-props
  (doto (java.util.Properties.)
    (.put "bootstrap.servers" bootstrap-servers)
    (.put "key.deserializer" deserializer)
    (.put "value.deserializer" deserializer)
    (.put "group.id" "melt.integration-test")))

(def sync-consumer-props
  (doto consumer-props
    (.put "group.id" "melt.integration-test.sync")))

(try
  (jdbc/execute! db ["Create table t_empty (id integer)"])
  (catch Exception e)) ; ignore

(def schema (melt/schema db))

(defn topic [source]
  (str "melt." (::melt/schema source) "." (::melt/name source)))

(defn assoc-topic-xform [source]
  (map #(assoc % ::melt/topic (topic source))))

(def schema-sources
  (map #(assoc % ::melt/xform (assoc-topic-xform %)) schema))

(def table (first (filter #(= (::melt/name %) "Address") schema-sources)))

(def empty-table (first (filter #(= (::melt/name %) "t_empty") schema-sources)))

(fact "an empty topic results in an empty seq"
  (melt/count-topic consumer-props "melty.empty") => 0)

(fact "a table written to a topic may be read from a topic"
      (melt/load-with-producer db schema-sources producer-props)
      (let [topic-content (melt/read-topics consumer-props ["melt.SalesLT.Address"])]
        (get-in topic-content ["melt.SalesLT.Address" {:addressid 603}])
        =>
        {:city          "Killeen"
         :addressline2  nil
         :modifieddate  "2007-08-01T00:00:00Z"
         :rowguid       "0E6E9E86-A637-4FD5-A945-AC342BFD715B"
         :postalcode    "76541"
         :addressline1  "9500b E. Central Texas Expressway"
         :countryregion "United States"
         :stateprovince "Texas"
         :addressid     603}))

(fact "`diff` finds no differences between a source table and target topic after initial load"
      (melt/diff db consumer-props table)
      =>
      {:table-only {}
       :topic-only {}})

(fact "`diff` handles empty table correctly"
      (melt/diff db consumer-props empty-table) => {:topic-only {}
                                                    :table-only {}})

(jdbc/with-db-transaction [t-con db]
  (jdbc/db-set-rollback-only! t-con) 

  (fact "`diff` finds differences when the table has changed"
        (jdbc/update! t-con "saleslt.address" {:postalcode "99995"} ["addressid = ?" 888]) => [1]

        (let [diff (melt/diff t-con consumer-props table)]
          (get-in diff [:table-only ["melt.SalesLT.Address" {:addressid 888}]]) => (contains {:postalcode "99995"})
          (get-in diff [:topic-only ["melt.SalesLT.Address" {:addressid 888}]]) => (contains {:postalcode "98626"})))

  (fact "`sync-kafka` publishes differences in a table to the topic to bring them back in sync"
        (melt/sync-kafka t-con consumer-props producer-props table) => 1
        (melt/diff t-con consumer-props table)
        =>
        {:table-only {}
         :topic-only {}}))

(jdbc/with-db-transaction [t-con db]
  (jdbc/db-set-rollback-only! t-con)

  (fact "`verify` returns truthy value of whether topic contents match table"
        (melt/verify t-con consumer-props table 0 1) => false)

  (fact "`verify` can retry to reduce false-positives for active sources"
        (future (Thread/sleep 5000)
                (melt/sync-kafka t-con sync-consumer-props producer-props table))
        (melt/verify t-con consumer-props table 20 1) => true))

(fact "Deleted table entries will result in tombstone on topic"
      (jdbc/with-db-transaction [t-con db]
        (jdbc/db-set-rollback-only! t-con)
        (jdbc/delete! t-con "SalesLT.CustomerAddress" ["addressid = ?" 888]) => [1]
        (jdbc/delete! t-con "saleslt.address" ["addressid = ?" 888]) => [1]
        (melt/sync-kafka t-con sync-consumer-props producer-props table) => 1
        (let [topic-content (melt/read-topics consumer-props ["melt.SalesLT.Address"])]
          (find (get topic-content "melt.SalesLT.Address") {:addressid 888})
          => nil)))

(fact "`verify-sync` combines functionality of verify and sync-kafka to bring a
       topic with drift back in sync after X retries fail"
      (jdbc/with-db-transaction [t-con db]
        (melt/verify t-con consumer-props table 0 1) => false ;; already out of sync from last test
        (melt/verify-sync t-con consumer-props producer-props table 0 1) = true
        (jdbc/update! t-con "saleslt.address" {:postalcode "99995"} ["addressid = ?" 888]) => [1]
        (melt/verify-sync t-con consumer-props producer-props table 0 1) = true))

(fact "`verify-sync` sends tombstones for table deletes"
      (jdbc/with-db-transaction [t-con db]
        (jdbc/db-set-rollback-only! t-con)
        (jdbc/delete! t-con "SalesLT.CustomerAddress" ["addressid = ?" 888]) => [1]
        (jdbc/delete! t-con "saleslt.address" ["addressid = ?" 888]) => [1]
        (melt/verify t-con consumer-props table 0 1) => false
        (melt/verify-sync t-con consumer-props producer-props table 0 1) => (contains {:matches    true
                                                                                       :sync       true
                                                                                       :sync-count 1})))

(facts "values can be used as keys for tables lacking primary keys"
       (let [topic-fn     (fn [#::melt{:keys [schema name]}]
                            (str "melt.altkey." schema "." name))
             xform-fn     (fn [table] (comp (map #(assoc % ::melt/key (get % ::melt/value)))
                                            (map #(assoc % ::melt/topic (topic-fn table)))))
             sources      (map #(assoc % ::melt/xform (xform-fn %)) schema)
             sample-value {:city          "Killeen"
                           :addressline2  nil
                           :modifieddate  "2007-08-01T00:00:00Z"
                           :rowguid       "0E6E9E86-A637-4FD5-A945-AC342BFD715B"
                           :postalcode    "76541"
                           :addressline1  "9500b E. Central Texas Expressway"
                           :countryregion "United States"
                           :stateprovince "Texas"
                           :addressid     603}]
         (melt/load-with-producer db sources producer-props)
         (fact "can read message"
               (let [topic-content (melt/read-topics consumer-props ["melt.altkey.SalesLT.Address"])]
                 (get-in topic-content ["melt.altkey.SalesLT.Address" sample-value])
                 => sample-value))

         (fact "can verify-sync to re-sync when changed"
               (jdbc/with-db-transaction [t-con db]
                 (jdbc/update! t-con "saleslt.address" {:postalcode "99996"} ["addressid = ?" 888]) => [1]
                 (let [table (first (filter #(= (::melt/name %) "Address") sources))]
                   (melt/verify-sync t-con consumer-props producer-props table 0 1) => (contains {:matches true
                                                                                                  :sync    true}))))))

(defn update-table [postalcode]
  (jdbc/update! db "saleslt.address"
                {:postalcode postalcode}
                ["addressid = ?" 888]))

(defn new-zip []
  (str (+ 10000 (rand-int 90000))))

(defn update-table-with-random []
  (update-table (new-zip)))

(try
  (s/enable-change-tracking db (get table ::melt/cat))
  (s/track-table db table)
  (catch Exception e (println (.getMessage e))))

(try
  (fact "`sql-server/sync-kafka` performs full sync and `send-changes` performs incremental sync"
        (update-table-with-random)
        (let [sync1 (s/sync-kafka consumer-props producer-props db table)]
          (melt/verify db consumer-props table 0 1) => true
          (update-table-with-random)
          (let [sync2 (s/send-changes producer-props db table (:version sync1))]
            (:sent-count sync2) => 1
            (melt/verify db consumer-props table 0 1) => true
            (let [inserted (:id (first (jdbc/insert! db
                                                     "saleslt.address"
                                                     {:countryregion "United States"
                                                      :city          "Melt"
                                                      :addressline1  "Three Rivers Mall"
                                                      :addressline2  nil
                                                      :postalcode    "98626"
                                                      :stateprovince "Washington"})))
                  sync3    (s/send-changes producer-props db table (:version sync2))]
              (:sent-count sync3) => 1
              (try
                (melt/verify db consumer-props table 0 1) => true
                (jdbc/delete! db "saleslt.address" ["addressid = ?" inserted])
                (:sent-count (s/send-changes producer-props db table (:version sync3))) => 1
                (melt/verify db consumer-props table 0 1) => true
                (finally (jdbc/delete! db "saleslt.address" ["addressid = ?" inserted])))))))

  (finally
    (update-table "98626") ; restore original value
    (shutdown-agents)))
