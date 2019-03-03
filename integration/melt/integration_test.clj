(ns melt.integration-test
  (:require [clojure.java.jdbc :as jdbc]
            [melt.channel :as ch]
            [melt.config :refer [db table->topic-name]]
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

(def producer-props
  (doto (java.util.Properties.)
    (.put "bootstrap.servers" bootstrap-servers)
    (.put "acks" "all")
    (.put "key.serializer" "org.apache.kafka.common.serialization.StringSerializer")
    (.put "value.serializer" "org.apache.kafka.common.serialization.StringSerializer")))

(def consumer-props
  (doto (java.util.Properties.)
    (.put "bootstrap.servers" bootstrap-servers)
    (.put "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
    (.put "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
    (.put "group.id" "melt.integration-test")))

(def sync-consumer-props
  (doto consumer-props
    (.put "group.id" "melt.integration-test.sync")))

(if (= "TRUE" (System/getenv "RESET_INTEGRATION"))
  (jdbc/update! db "saleslt.address" {:postalcode "98626"} ["addressid = ?" 888]))

(def schema-channels (map #(assoc % ::ch/topic-fn table->topic-name) (mdb/schema)))

(def table (first (filter #(= (::ch/name %) "Address") schema-channels)))

(fact "a table written to a topic may be read from a topic"
      (lk/load-with-producer schema-channels
                             {:producer-properties producer-props})
      (let [topic-content (rt/read-topics consumer-props ["melt.SalesLT.Address"])]
        (get-in topic-content ["melt.SalesLT.Address" {:addressid 603}])
        =>
        {:addressid     603
         :addressline1  "9500b E. Central Texas Expressway"
         :addressline2  nil
         :city          "Killeen"
         :countryregion "United States"
         :modifieddate  "2007-08-01"
         :postalcode    "76541"
         :rowguid       "0E6E9E86-A637-4FD5-A945-AC342BFD715B"
         :stateprovince "Texas"}))

(fact "`diff` finds no differences between a source table and target topic after initial load"
      (d/diff consumer-props table)
      =>
      {:table-only {}
       :topic-only {}})

(fact "`diff` finds differences when the table has changed"
      (jdbc/update! db "saleslt.address" {:postalcode "99995"} ["addressid = ?" 888]) => [1]

      (let [diff (d/diff consumer-props table)]
        (get-in diff [:table-only ["melt.SalesLT.Address" {:addressid 888}]]) => (contains {:postalcode "99995"})
        (get-in diff [:topic-only ["melt.SalesLT.Address" {:addressid 888}]]) => (contains {:postalcode "98626"})))

(fact "`sync` publishes differences in a table to the topic to bring them back in sync"
      (s/sync consumer-props producer-props table)
      (d/diff consumer-props table)
      =>
      {:table-only {}
       :topic-only {}})

(fact "`verify` returns truthy value of whether topic contents match table"
      (v/verify consumer-props table 0 1) => true
      (jdbc/update! db "saleslt.address" {:postalcode "99994"} ["addressid = ?" 888]) => [1]
      (v/verify consumer-props table 0 1) => false)

(fact "`verify` can retry to reduce false-positives for active channels"
      (.start
       (Thread.
        (fn [] (do (Thread/sleep 5000)
                   (s/sync sync-consumer-props producer-props table)))))
      (v/verify consumer-props table 20 1) => true)
