(ns melt.load-kafka
  (:require [melt.channel :as ch]
            [melt.jdbc :as mdb]
            [melt.serial :as serial])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))

(defn with-producer [callback {:keys [producer producer-properties]}]
  {:pre [(some some? [producer producer-properties])]}
  (with-open [p (or producer (KafkaProducer. producer-properties))]
    (callback p)
    (.flush p)))

(defn default-send-fn [producer]
  (fn [topic k v] (.send producer (ProducerRecord. topic k v))))

(defn load-with-sender [channels send-fn]
  (doseq [#::ch{:keys [channel records]} (mdb/channel-content channels)]
    (let [topic-fn (::ch/topic-fn channel)]
      (doseq [[k v] records]
        (send-fn (topic-fn channel v)
                 (serial/write-str k)
                 (serial/write-str v))))))

(defn load-with-producer [channels producer-options]
  (with-producer
    (fn [p] (load-with-sender channels (default-send-fn p)))
    producer-options))
