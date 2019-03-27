(ns melt.load-kafka
  (:require [clojure.spec.alpha :as spec]
            [melt.jdbc :as mdb]
            [melt.kafka :as k]
            [melt.source :as source])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))

(defn default-send-fn [producer]
  (fn [topic k v] (.send producer (ProducerRecord. topic k v))))

(defn- source-desc [source]
  (or (::source/name source) (::source/sql source)))

(defn- do-load [db sources send-fn]
  (doseq [source sources]
    (let [desc (source-desc source)]
      (println "Starting to load" desc)
      (doall (eduction (map (partial source/message source))
                       (source/xform source)
                       (map send-fn)
                       (mdb/query-source db source)))
      (println "Completed loading" desc))))

(defn load-with-sender [db sources send-fn]
  (do-load db sources #(apply send-fn ((juxt ::source/topic
                                             ::source/key
                                             ::source/value)
                                       (spec/assert ::source/message %)))))

(defn load-with-producer [db sources p-spec]
  (k/with-producer [p-spec p-spec]
    (let [p (k/producer p-spec)]
      (load-with-sender db sources (default-send-fn p))
      (.flush p))))
