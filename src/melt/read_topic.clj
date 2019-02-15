(ns melt.read-topic
  (:require [melt.serial :as serial])
  (:import [org.apache.kafka.clients.consumer Consumer KafkaConsumer ConsumerRecord]
           [org.apache.kafka.common TopicPartition]))

(defn consumer-at-beginning [consumer-props topics]
  (let [c (KafkaConsumer. consumer-props)]
    (doto c
      (.subscribe topics)
      (.poll 1)
      (.seekToBeginning (.assignment c)))))

(defn record [^ConsumerRecord cr]
  {:value     (serial/read-str (.value cr))
   :key       (serial/read-str (.key cr) :key-fn keyword)
   :offset    (.offset cr)
   :partition (.partition cr)
   :timestamp (.timestamp cr)
   :topic     (.topic cr)})

(defn at-end? [consumed-offsets [^TopicPartition p end-offset]]
  (if-let [committed-offset (get consumed-offsets (.partition p))]
    (<= end-offset (inc committed-offset))
    false))

(defn fully-consumed? [^Consumer c consumed-offsets end-offsets]
  (every? (partial at-end? consumed-offsets) end-offsets))

(defn track-offset [consumed-offsets-atom
                    {:keys [partition offset]
                     :as   r}]
  (swap! consumed-offsets-atom
         (fn [m] (assoc m partition (max offset (or (m partition) 0)))))
  r)

(defn poll [track-offset-fn c]
  (let [crs (.poll c 1000)]
    (if (.isEmpty crs) nil (map (comp track-offset-fn record) crs))))

(defn not-fully-consumed-fn [^Consumer c consumed-offsets-atom]
  (let [end-offsets (.endOffsets c (.assignment c))]
    (fn [_] (not (fully-consumed? c @consumed-offsets-atom end-offsets)))))

(defn consumer-seq
  ([^Consumer c] (consumer-seq c (atom {})))
  ([^Consumer c consumed-offsets-atom]
   (let [track-offset-fn (partial track-offset consumed-offsets-atom)
         take-while-fn   (not-fully-consumed-fn c consumed-offsets-atom)]
     (filter some?
             (flatten
              (take-while take-while-fn
                          (repeatedly #(poll track-offset-fn c))))))))

(defn count-topic [consumer-props topic]
  (with-open [c (consumer-at-beginning consumer-props [topic])]
    (count (consumer-seq c))))

(defn- reduce-topics [consumer-seq topic-map]
  (reduce (fn [m {:keys [topic key value]}] (assoc-in m [topic key] value))
          topic-map
          consumer-seq))

(defn read-topics-loop [consumer-props topics end-fn retries]
  (with-open [c (consumer-at-beginning consumer-props topics)]
    (loop [c        c
           end-fn   end-fn
           consumed (atom {})
           reduced  {}
           retries  retries]
      (let [reduced (reduce-topics (consumer-seq c consumed) reduced)]
        (if (or (<= retries 0) (end-fn reduced))
          reduced
          (recur c end-fn consumed reduced (dec retries)))))))

(defn read-topics
  "Read topics twice since reading a large topic could take minutes by which
   time the original end-offsets may no longer be the true end-offsets"
  [consumer-props topics]
  (read-topics-loop consumer-props topics (fn [_] false) 1))
