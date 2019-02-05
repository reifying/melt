(ns melt.load-kafka
  (:require [melt.analyze :as a]
            [melt.config :refer [table->topic-name]]
            [melt.serial :as serial])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))

(defn sender-fn [producer topic]
  (fn [k v] (.send producer
                   (ProducerRecord. topic
                                    (serial/write-str k)
                                    (serial/write-str v)))))

(defn load-topic [send-fn record-seq]
  (doseq [[k v] record-seq] (send-fn k v)))

(defn read-schema []
  (map (fn [table] [(table->topic-name table) (a/read-table table)])
       (a/schema)))

;; TODO support transformations and custom SQL queries
(defn load-topics
  [{:keys [producer producer-properties sender-fn records-fn]
    :or   {sender-fn  sender-fn
           records-fn read-schema}}]
  {:pre [(some some? [producer producer-properties])]}
  (with-open [p (or producer (KafkaProducer. producer-properties))]
    (doseq [[topic record-seq] (records-fn)]
      (load-topic (sender-fn p topic) record-seq))
    (.flush p)))
