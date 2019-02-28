(ns melt.read-topic
  (:require [melt.serial :as serial])
  (:import [org.apache.kafka.clients.consumer Consumer KafkaConsumer ConsumerRecord]
           [org.apache.kafka.common TopicPartition])
  (:refer-clojure :exclude [poll]))

(def empty-data {:offsets {}})

(defn- partition-infos [c topics]
  (let [topics (set topics)]
    (filter #(topics (first %)) (into {} (.listTopics c)))))

(defn- topic-partition [partition-info]
  (TopicPartition. (.topic partition-info) (.partition partition-info)))

(defn- topic-partitions [c topics]
  (map topic-partition (flatten (map seq (vals (partition-infos c topics))))))

(defn reset-consumer [consumer topics]
  (let [tps (topic-partitions consumer topics)]
    (.assign consumer tps)
    (doseq [[partition offset] (.beginningOffsets consumer tps)]
      (.seek consumer partition offset)))
  consumer)

(defn consumer-at-beginning [consumer-props topics]
  (reset-consumer (KafkaConsumer. consumer-props) topics))

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

(defn end-offsets [^Consumer c]
  (.endOffsets c (.assignment c)))

(defn fully-consumed-fn [^Consumer c]
  (let [end-offsets (end-offsets c)]
    (fn [consumed-offsets]
      (every? (partial at-end? consumed-offsets) end-offsets))))

(defn assoc-offset [offsets message]
  (assoc offsets (:partition message) (:offset message)))

(defn- poll [c]
  (map record (.poll c 1000)))

(defn- seq-entry [record offsets]
  {:consumer-record record
   :offsets         offsets})

(defn consumer-seq
  ([^Consumer c current-offsets]
   (consumer-seq c current-offsets (fully-consumed-fn c) []))
  ([^Consumer c current-offsets stop-consuming-fn messages]
   (lazy-seq
    (if (seq messages)
      (let [record      (first messages)
            cur-offsets (assoc-offset current-offsets record)]
        (cons (seq-entry record cur-offsets)
              (consumer-seq c cur-offsets stop-consuming-fn (rest messages))))
      (if-not (stop-consuming-fn current-offsets)
        (consumer-seq c current-offsets stop-consuming-fn (poll c)))))))

(defn count-topic [consumer-props topic]
  (with-open [c (consumer-at-beginning consumer-props [topic])]
    (count (consumer-seq c {}))))

(defn- merge-seq-entry [topic-data seq-entry]
  (let [{:keys [topic key value]} (:consumer-record seq-entry)]
    (-> topic-data
        (assoc-in [:data topic key] value)
        (assoc :offsets (:offsets seq-entry)))))

(defn reduce-consumer-seq [c topic-data]
  (reduce merge-seq-entry topic-data (consumer-seq c (:offsets topic-data))))

(defn- background-consume-fn [consumer-props topics reduced-atom shutdown-atom]
  (fn []
    (with-open [c (consumer-at-beginning consumer-props topics)]
      (doseq [entry (consumer-seq c {} (fn [_] @shutdown-atom) [])]
        (swap! reduced-atom (fn [topic-data]
                              (merge-seq-entry topic-data entry)))))))

(defn background-consume [consumer-props topics reduced-atom]
  (let [shutdown (atom false)]
    (.start
     (Thread.
      (background-consume-fn consumer-props topics reduced-atom shutdown)))
    (reify java.lang.AutoCloseable
      (close [this] (reset! shutdown true)))))

(defn read-topics-loop [consumer-props topics retries]
  (with-open [c (consumer-at-beginning consumer-props topics)]
    (loop [topic-data empty-data
           retries    retries]
      (if (<= retries 0)
        topic-data
        (recur (reduce-consumer-seq c topic-data)
               (dec retries))))))

(defn read-topics
  "Read topics twice since reading a large topic could take minutes by which
   time the original end-offsets may no longer be the true end-offsets"
  [consumer-props topics]
  (:data (read-topics-loop consumer-props topics 1)))
