(ns melt.integration-test
  (:require [clojure.java.jdbc :as jdbc]
            [melt.source :as source]
            [melt.config :refer [db]]
            [melt.diff :as d]
            [melt.jdbc :as mdb]
            [melt.load-kafka :as lk]
            [melt.read-topic :as rt]
            [melt.sync :as s]
            [melt.verify :as v]
            [midje.sweet :refer [facts fact => contains]]))

;; Tests are dependent on each other. order matters and predecessor tests failing
;; will likely cause successors to fail.

(def bootstrap-servers
  (str (or (System/getenv "MELT_KAFKA_HOST") "localhost") ":9092"))

(def serializer "melt.Serializer")

(def producer-props
  (doto (java.util.Properties.)
    (.put "bootstrap.servers" bootstrap-servers)
    (.put "acks" "all")
    (.put "key.serializer" serializer)
    (.put "value.serializer" serializer)))

(def deserializer "melt.Deserializer")

(def consumer-props
  (doto (java.util.Properties.)
    (.put "bootstrap.servers" bootstrap-servers)
    (.put "key.deserializer" deserializer)
    (.put "value.deserializer" deserializer)
    (.put "group.id" "melt.integration-test")))

(def sync-consumer-props
  (doto consumer-props
    (.put "group.id" "melt.integration-test.sync")))

(def schema (mdb/schema))

(defn topic [source]
  (str "melt." (::source/schema source) "." (::source/name source)))

(defn assoc-topic-xform [source]
  (map #(assoc % ::source/topic (topic source))))

(def schema-sources
  (map #(assoc % ::source/xform (assoc-topic-xform %)) schema))

(def table (first (filter #(= (::source/name %) "Address") schema-sources)))

(fact "a table written to a topic may be read from a topic"
      (lk/load-with-producer db schema-sources producer-props)
      (let [topic-content (rt/read-topics consumer-props ["melt.SalesLT.Address"])]
        (get-in topic-content ["melt.SalesLT.Address" {:addressid 603}])
        =>
        {:city          "Killeen"
         :addressline2  nil
         :modifieddate  "2007-08-01"
         :rowguid       "0E6E9E86-A637-4FD5-A945-AC342BFD715B"
         :postalcode    "76541"
         :addressline1  "9500b E. Central Texas Expressway"
         :countryregion "United States"
         :stateprovince "Texas"
         :addressid     603}))

(fact "`diff` finds no differences between a source table and target topic after initial load"
      (d/diff db consumer-props table)
      =>
      {:table-only {}
       :topic-only {}})

(jdbc/with-db-transaction [t-con db]
  (jdbc/db-set-rollback-only! t-con)

  (fact "`diff` finds differences when the table has changed"
        (jdbc/update! t-con "saleslt.address" {:postalcode "99995"} ["addressid = ?" 888]) => [1]

        (let [diff (d/diff t-con consumer-props table)]
          (get-in diff [:table-only ["melt.SalesLT.Address" {:addressid 888}]]) => (contains {:postalcode "99995"})
          (get-in diff [:topic-only ["melt.SalesLT.Address" {:addressid 888}]]) => (contains {:postalcode "98626"})))

  (fact "`sync` publishes differences in a table to the topic to bring them back in sync"
        (s/sync t-con consumer-props producer-props table)
        (d/diff t-con consumer-props table)
        =>
        {:table-only {}
         :topic-only {}}))

(jdbc/with-db-transaction [t-con db]
  (jdbc/db-set-rollback-only! t-con)

  (fact "`verify` returns truthy value of whether topic contents match table"
        (v/verify t-con consumer-props table 0 1) => false)

  (fact "`verify` can retry to reduce false-positives for active sources"
        (future (Thread/sleep 5000)
                (s/sync t-con sync-consumer-props producer-props table))
        (v/verify t-con consumer-props table 20 1) => true))

(fact "Deleted table entries will result in tombstone on topic"
      (jdbc/with-db-transaction [t-con db]
        (jdbc/db-set-rollback-only! t-con)
        (jdbc/delete! t-con "SalesLT.CustomerAddress" ["addressid = ?" 888]) => [1]
        (jdbc/delete! t-con "saleslt.address" ["addressid = ?" 888]) => [1]
        (s/sync t-con sync-consumer-props producer-props table)
        (let [topic-content (rt/read-topics consumer-props ["melt.SalesLT.Address"])]
          (find (get topic-content "melt.SalesLT.Address") {:addressid 888})
          => nil)))

(fact "`identity` can be used as keys for tables lacking primary keys"
      (let [topic-fn     (fn [#:melt.source{:keys [schema name]}]
                           (str "melt.keyfn." schema "." name))
            xform-fn     (fn [table] (comp (map #(assoc % ::source/key (get % ::source/value)))
                                           (map #(assoc % ::source/topic (topic-fn table)))))
            sources      (map #(assoc % ::source/xform (xform-fn %)) schema)
            sample-value {:city          "Killeen"
                          :addressline2  nil
                          :modifieddate  "2007-08-01"
                          :rowguid       "0E6E9E86-A637-4FD5-A945-AC342BFD715B"
                          :postalcode    "76541"
                          :addressline1  "9500b E. Central Texas Expressway"
                          :countryregion "United States"
                          :stateprovince "Texas"
                          :addressid     603}]
        (lk/load-with-producer db sources producer-props)
        (let [topic-content (rt/read-topics consumer-props ["melt.keyfn.SalesLT.Address"])]
          (get-in topic-content ["melt.keyfn.SalesLT.Address" sample-value])
          => sample-value)))

(shutdown-agents)
