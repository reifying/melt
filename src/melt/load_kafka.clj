(ns melt.load-kafka
  (:require [clojure.data.json :as json]
            [melt.analyze :as a]
            [melt.config :refer [table->topic-name]]
            [melt.util :refer [has-time? format-date-time]])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))

(defn ->json [x]
  (letfn [(json-value-fn [k v]
            (cond (instance? java.sql.Timestamp v) (format-date-time k v)
                  (instance? java.sql.Blob v) (.getBytes v 1 (.length v))
                  (instance? java.sql.Clob v) (.getSubString v 1 (.length v))
                  :else v))]
    (json/write-str x :value-fn json-value-fn)))

(defn sender-fn [producer topic]
  (fn [k v] (.send producer (ProducerRecord. topic (->json k) (->json v)))))

(defn load-topic [send-fn table]
  (doseq [[k v] (a/read-table table)] (send-fn k v)))

;; TODO support transformations and custom SQL queries
(defn load-topics-from-tables [producer-properties]
  (with-open [producer (KafkaProducer. producer-properties)]
    (doseq [table (a/schema)]
      (let [topic  (table->topic-name table)
            sender (sender-fn producer topic)]
        (load-topic sender table)))
    (.flush producer)))
