(ns melt.load-kafka
  (:require [melt.channel :as ch]
            [melt.config :as c]
            [melt.jdbc :as mdb]
            [melt.kafka :as k])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))

(defn default-send-fn [producer]
  (fn [topic k v] (.send producer (ProducerRecord. topic k v))))

(defn load-with-sender [channels send-fn]
  (doseq [#::ch{:keys [channel records]} (mdb/channel-content c/db channels)]
    (let [name     (or (::ch/name channel) (::ch/sql channel))
          topic-fn (::ch/topic-fn channel)]
      (println "Starting to load" name)
      (doseq [[k v] records]
        (send-fn (topic-fn channel v) k v))
      (println "Completed loading" name))))

(defn load-with-producer [channels p-spec]
  (k/with-producer [p-spec p-spec]
    (let [p (k/producer p-spec)]
      (load-with-sender channels (default-send-fn p))
      (.flush p))))
