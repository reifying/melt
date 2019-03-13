(ns melt.verify
  (:require [melt.diff :as diff]
            [melt.serial :as serial]
            [melt.read-topic :as rt])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer]))

(defn- consumer-topics [c] (.subscription c))

(defn- topics-match [consumer coll]
  (let [consumer-topics (consumer-topics consumer)
        new-topics      (set coll)]
    (= consumer-topics new-topics)))

(defn- refresh [consumer topic-data topics]
  (if (topics-match consumer topics)
    (rt/reduce-consumer-seq consumer topic-data)
    (rt/reduce-consumer-seq (rt/reset-consumer consumer topics) rt/empty-data)))

(defn- sleep [secs]
  (Thread/sleep (* 1000 secs)))

(defn- matches [channel-data topic-data]
  (= (serial/fuzz channel-data)
     (diff/merge-topic-key (:data topic-data))))

(defn verify [db consumer-props channel retries retry-delay-sec]
  (with-open [c (KafkaConsumer. consumer-props)]
    (loop [prev-topic-data rt/empty-data
           retries         retries]
      (let [channel-data (diff/by-topic-key db channel)
            topic-data   (refresh c prev-topic-data (diff/topics channel-data))
            matches      (matches channel-data topic-data)]
        (if (or matches (<= retries 0))
          matches
          (do (sleep retry-delay-sec)
              (recur topic-data (dec retries))))))))
