(ns melt.read-topic
  (:require [melt.kafka :as k])
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

(defn record [^ConsumerRecord cr]
  {:value     (.value cr)
   :key       (.key cr)
   :offset    (.offset cr)
   :partition (.partition cr)
   :timestamp (.timestamp cr)
   :topic     (.topic cr)})

(defn at-end? [consumed-offsets [^TopicPartition p end-offset]]
  (or (zero? end-offset)
      (if-let [committed-offset (get-in consumed-offsets [(.topic p) (.partition p)])]
        (<= end-offset (inc committed-offset))
        false)))

(defn end-offsets [^Consumer c]
  (.endOffsets c (.assignment c)))

(defn fully-consumed-fn [^Consumer c]
  (let [end-offsets (end-offsets c)]
    (fn [consumed-offsets]
      (every? (partial at-end? consumed-offsets) end-offsets))))

(defn assoc-offset [offsets message]
  (assoc-in offsets [(:topic message) (:partition message)] (:offset message)))

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

(defn count-topic [c-spec topic]
  (k/with-consumer [c-spec c-spec]
    (let [c (k/consumer c-spec)]
      (reset-consumer c [topic])
      (count (consumer-seq c {})))))

(defn- merge-seq-entry [topic-data seq-entry]
  (let [{:keys [topic key value]} (:consumer-record seq-entry)
        data                      (update topic-data :offsets
                                          merge (:offsets seq-entry))]
    (if (some? value)
      (assoc-in data [:data topic key] value)
      (update-in data [:data topic] dissoc key))))

(defn reduce-consumer-seq [c topic-data]
  (reduce merge-seq-entry topic-data (consumer-seq c (:offsets topic-data))))

(defn- background-consume-fn [c-spec topics reduced-atom shutdown-atom]
  (fn []
    (k/with-consumer [c-spec c-spec]
      (let [c (k/consumer c-spec)]
        (reset-consumer c topics)
        (doseq [entry (consumer-seq c {} (fn [_] @shutdown-atom) [])]
          (swap! reduced-atom (fn [topic-data]
                                (merge-seq-entry topic-data entry))))))))

(defn background-consume [c-spec topics reduced-atom]
  (let [shutdown (atom false)]
    (.start
     (Thread.
      (background-consume-fn c-spec topics reduced-atom shutdown)))
    (reify java.lang.AutoCloseable
      (close [this] (reset! shutdown true)))))

(defn read-topics-loop [c-spec topics retries]
  (k/with-consumer [c-spec c-spec]
    (let [c (k/consumer c-spec)]
      (reset-consumer c topics)
      (loop [topic-data empty-data
             retries    retries]
        (if (<= retries 0)
          topic-data
          (recur (reduce-consumer-seq c topic-data)
                 (dec retries)))))))

(defn read-topics
  "Read topics twice since reading a large topic could take minutes by which
   time the original end-offsets may no longer be the true end-offsets"
  [c-spec topics]
  (:data (read-topics-loop c-spec topics 1)))
