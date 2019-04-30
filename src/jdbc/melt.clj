(ns jdbc.melt
  (:require [clojure.data :as data]
            [cheshire.core :as json]
            [cheshire.generate :as gen]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as spec]
            [clojure.string :refer [lower-case]]
            [jdbc.melt.util :refer [mkdirs conform-or-throw format-date-time]])
  (:import java.io.File
           [org.apache.kafka.clients.consumer Consumer KafkaConsumer ConsumerRecord]
           [org.apache.kafka.clients.producer Producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization
            Deserializer Serializer StringDeserializer StringSerializer]))

(def ignorable-schemas #{"sys" "INFORMATION_SCHEMA"})

(def schema-file-path (or (System/getenv "SCHEMA_PATH") "target/schema.edn"))

; Set to TRUE to enable
(def abort-on-schema-change (System/getenv "ABORT_ON_SCHEMA_CHANGE"))

(spec/def ::keyed (spec/keys :req [::keys]))

(spec/def ::message (spec/keys :req [::topic ::key ::value]))

(defn xform [source]
  (get source ::xform (map identity)))

(defn throw-err [e]
  (when (instance? Throwable e) (throw e))
  e)

(defmacro <?? [ch]
  `(throw-err (async/<!! ~ch)))

(defn backpressure-channel [coll]
  (let [c (async/chan 1000)]
    (async/go
      (try
        (doseq [x coll] (async/>! c x))
        (catch Throwable e (async/>! c e))
        (finally (async/close! c))))
    c))

(defn message [source row]
  (if-let [keys (get source ::keys)]
    {::value row
     ::key    (select-keys row keys)}
    {::value row}))

(defn- user-schema? [{:keys [table_schem]}]
  (not (contains? ignorable-schemas table_schem)))

(defn- table [{:keys [table_schem table_cat table_name]}]
  {::name   table_name
   ::cat    table_cat
   ::schema table_schem})

(def column->keyword (comp keyword lower-case :column_name))

(defn- group-by-table
  [table-map column-map]
  (update-in table-map
             [(table column-map) ::columns]
             (fn [columns] 
               (conj (or columns #{}) (column->keyword column-map)))))

(defn- primary-keys [db table]
  (jdbc/with-db-metadata [md db]
    (->> (.getPrimaryKeys md (::cat table) (::schema table) (::name table))
         jdbc/metadata-query
         (map column->keyword)
         set)))

(defn- table-set [db]
  (set (jdbc/with-db-metadata [md db]
         (->> (.getTables md nil nil nil (into-array String ["TABLE"]))
              jdbc/metadata-query
              (filter user-schema?)
              (map table)))))

(defn- contains-table? [table-set column-map]
  (contains? table-set (table column-map)))

(defn schema [db]
  (let [table-set (table-set db)
        id        (fn [t] (clojure.string/join
                           "." ((juxt ::cat ::schema ::name) t)))]
    (jdbc/with-db-metadata [md db]
      (->> (.getColumns md nil nil "%" nil)
           jdbc/metadata-query
           (filter (partial contains-table? table-set))
           (reduce group-by-table {})
           (map #(apply merge %))
           (map #(assoc % ::keys (primary-keys db %)))
           (apply sorted-set-by #(compare (id %1) (id %2)))))))

(defn cached-schema-file []
  (let [f (io/as-file schema-file-path)]
    (mkdirs (.getParent f))
    f))

(defn file-schema []
  (let [f (cached-schema-file)]
    (if (.exists f)
      (read-string (slurp f)))))

(def cached-schema (memoize schema))

(defn save-schema
  ([coll] (spit (cached-schema-file) (with-out-str (pprint coll)))))

(defn schema-diff [db]
  (let [cached (cached-schema)
        latest (schema db)
        diff   (data/diff cached latest)]
    {:only-old   (first diff)
     :only-new   (second diff)
     :new-schema latest}))

(defn schema-changed? [diff]
  (some some? (vals (select-keys diff [:only-new :only-old]))))

(defn schema-check [db]
  (let [diff (schema-diff db)]
    (if (and (schema-changed? diff)
             (= "TRUE" abort-on-schema-change))
      false
      diff)))

(defn qualified-table-name [#:jdbc.melt{:keys [schema name]}]
  (str "[" schema "].[" name "]"))

(defn select-all-sql [table]
  (str "Select * From " (qualified-table-name table)))

(defn query-source [db source]
  (let [sql-params (get source
                        ::sql-params
                        [(get source ::sql (select-all-sql source))])]
    (jdbc/query db sql-params)))

(defn reducible-source [db source]
  (let [sql-params (get source
                        ::sql-params
                        [(get source ::sql (select-all-sql source))])]
    (jdbc/reducible-query db sql-params)))

(defn sample-file-name [dir-name table]
  (str dir-name File/separator (::schema table) "." (::name table) ".txt"))

(defn sample-writer [dir-name table]
  (io/writer (sample-file-name dir-name table)))

(defn sample-db [db schema dir-name]
  (println "Writing sample to" dir-name)
  (mkdirs dir-name)
  (doseq [table schema]
    (with-open [wr (sample-writer dir-name table)]
      (let [name       (qualified-table-name table)
            sample-sql (str "Select TOP 10 * From " name)
            count-sql  (str "Select count(*) c From " name)]
        (println "Sampling " name)
        (binding [*out* wr]
          (println "Count:" (:c (first (jdbc/query db [count-sql]))))
          (pprint (jdbc/query db [sample-sql])))))))

(defn write-sample
  ([db sources] (write-sample db sources "target/data-samples"))
  ([db sources dir-name] (sample-db db sources dir-name)))

(defn- ensure-map [m]
  (cond (map? m) m
        (instance? java.util.Map m) (into {} m)
        :else (throw (Exception. "Expected map, Properties, or java.util.Map"))))

(defn add-producer [m producer]
  (assoc (ensure-map m) ::producer producer))

(defn add-consumer [m consumer]
  (assoc (ensure-map m) ::consumer consumer))

(defn find-producer
  "Returns the current producer (or nil if there is none)"
  ^org.apache.kafka.clients.producer.Producer [producer-spec]
  (and (map? producer-spec)
       (::producer producer-spec)))

(defn producer
  "Returns the current producer (or throws if there is none)"
  ^org.apache.kafka.clients.producer.Producer [producer-spec]
  (or (find-producer producer-spec)
      (throw (Exception. "no current producer"))))

(defn find-consumer
  "Returns the current consumer (or nil if there is none)"
  ^org.apache.kafka.clients.consumer.Consumer [consumer-spec]
  (and (map? consumer-spec)
       (::consumer consumer-spec)))

(defn consumer
  "Returns the current consumer (or throws if there is none)"
  ^org.apache.kafka.clients.consumer.Consumer [consumer-spec]
  (or (find-consumer consumer-spec)
      (throw (Exception. "no current consumer"))))

(defmacro with-consumer
  "Evaluates body in the context of an active connection to the Kafka.
  (with-consumer [consumer consumer-spec]
    ... consumer ...)"
  [binding & body]
  `(let [c-spec# ~(second binding)]
     (if (find-consumer c-spec#)
       (let [~(first binding) c-spec#]
         ~@body)
       (with-open [c# (KafkaConsumer. c-spec#)]
         (let [~(first binding) (add-consumer c-spec# c#)]
           ~@body)))))

(defmacro with-producer
  "Evaluates body in the context of an active connection to the Kafka.
  (with-producer [producer producer-spec]
    ... producer ...)"
  [binding & body]
  `(let [p-spec# ~(second binding)]
     (if (find-producer p-spec#)
       (let [~(first binding) p-spec#]
         ~@body)
       (with-open [p# (KafkaProducer. p-spec#)]
         (let [~(first binding) (add-producer p-spec# p#)]
           ~@body)))))

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

(defn- poll-consumer [c]
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
        (consumer-seq c current-offsets stop-consuming-fn (poll-consumer c)))))))

(defn count-topic [c-spec topic]
  (with-consumer [c-spec c-spec]
    (let [c (consumer c-spec)]
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
    (with-consumer [c-spec c-spec]
      (let [c (consumer c-spec)]
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
  (if (seq topics)
    (with-consumer [c-spec c-spec]
      (let [c (consumer c-spec)]
        (reset-consumer c topics)
        (loop [topic-data empty-data
               retries    retries]
          (if (<= retries 0)
            topic-data
            (recur (reduce-consumer-seq c topic-data)
                   (dec retries)))))))
  empty-data)

(defn read-topics
  "Read topics twice since reading a large topic could take minutes by which
   time the original end-offsets may no longer be the true end-offsets"
  [c-spec topics]
  (:data (read-topics-loop c-spec topics 1)))

(defn ensure-sorted [x]
  (if (and (map? x) (not (sorted? x)))
    (into (sorted-map) x)
    x))

(gen/add-encoder java.sql.Blob
                 (fn [v jsonGenerator]
                   (.writeBinary jsonGenerator (.getBytes v 1 (.length v)))))

(gen/add-encoder java.sql.Clob
                 (fn [v jsonGenerator]
                   (.writeString jsonGenerator (.getSubString v 1 (.length v)))))

(defn write-str [x]
    (json/generate-string x))

(def write-key (comp write-str ensure-sorted))

(defn read-str [s]
  (json/parse-string s true))

(def lossy-identity (comp read-str write-str))

(defn fuzz [table-map]
  (reduce-kv (fn [m k v] (assoc m (lossy-identity k) (lossy-identity v)))
             {} table-map))

(defn merge-by-key [acc message]
  (let [m (spec/assert ::message message)]
    (assoc acc ((juxt ::topic ::key) m) (get m ::value))))

(defn by-topic-key [db source]
  (transduce (comp (map (partial message source))
                   (xform source))
             (completing merge-by-key)
             {}
             (reducible-source db source)))

(defn merge-topic-key [topic-map]
  (reduce-kv (fn [topic-key-m topic records]
               (merge topic-key-m
                      (reduce-kv (fn [m k v] (assoc m [topic k] v)) {} records)))
             {} topic-map))

(defn- topic-map [c-spec topics]
  (merge-topic-key (read-topics c-spec topics)))

(defn topics [m]
  (distinct (map first (keys m))))

(defn diff
  ([db c-spec source]
   (let [source-map (by-topic-key db source)
         topic-map  (topic-map c-spec (topics source-map))]
     (diff source-map topic-map)))
  ([source-map topic-map]
   (let [diff       (data/diff (fuzz source-map) topic-map)]
     {:table-only (select-keys source-map (map key (first diff)))
      :topic-only (select-keys topic-map (map key (second diff)))})))

(defn default-send-fn [producer]
  (fn [topic k v]
    (let [record (ProducerRecord. topic k v)]
      {:message record
       :future  (.send producer record)})))

(defn flush-message [m]
  (.get (:future m))
  (:message m))

(defn flush-messages [channel]
  (map flush-message
       (take-while some? (repeatedly #(<?? channel)))))

(defn- source-desc [source]
  (or (::name source) (::sql source)))

(defn- log-load-start [source]
  (println "Starting to load" (source-desc source))
  source)

(defn- log-load-finish [source total]
  (println "Completed loading" (source-desc source) "with count" total)
  total)

(defn- do-load [db sources send-fn] ; TODO return total
  (doseq [source sources]
    (->> source
         log-load-start
         (query-source db)
         (eduction (map (partial message source)) (xform source) (map send-fn))
         backpressure-channel
         flush-messages
         count
         (log-load-finish source))))

(defn load-with-sender [db sources send-fn]
  (do-load db sources #(apply send-fn ((juxt ::topic ::key ::value)
                                       (spec/assert ::message %)))))

(defn load-with-producer [db sources p-spec]
  (with-producer [p-spec p-spec]
    (->> p-spec
         producer
         default-send-fn
         (load-with-sender db sources))))

(defn sync-with-sender [db-only send-fn]
  (-> (for [[[topic k] v] db-only] (send-fn topic k v))
      backpressure-channel
      flush-messages
      count))

(defn deleted [diff]
  (apply dissoc
         (:topic-only diff)
         (keys (:table-only diff))))

(defn send-tombstones [deleted send-fn]
  (-> (for [[[topic k] _] deleted] (send-fn topic k nil))
      backpressure-channel
      flush-messages
      count))

(defn sync-kafka
  ([db c-spec p-spec source]
   (sync-kafka p-spec (diff db c-spec source)))
  ([p-spec diff]
   (let [table-only (seq (:table-only diff))
         deleted    (seq (deleted diff))]
     (if (or deleted table-only)
       (with-producer [p-spec p-spec]
         (let [p (producer p-spec)]
           (apply
            + (filter some?
                      [(if table-only
                         (sync-with-sender table-only (default-send-fn p)))
                       (if deleted
                         (send-tombstones deleted (default-send-fn p)))]))))))))

(defn- consumer-topics [c] (.subscription c))

(defn- topics-match [consumer coll]
  (let [consumer-topics (consumer-topics consumer)
        new-topics      (set coll)]
    (= consumer-topics new-topics)))

(defn- refresh [consumer topic-data topics]
  (if (seq topics)
    (if (topics-match consumer topics)
      (reduce-consumer-seq consumer topic-data)
      (reduce-consumer-seq (reset-consumer consumer topics) empty-data))
    topic-data))

(defn- sleep [secs]
  (Thread/sleep (* 1000 secs)))

(defn- matches [source-data topic-data]
  (= (fuzz source-data)
     (merge-topic-key (:data topic-data))))

(defn- diff-matches? [diff]
  (every? empty? (vals diff)))

(defn verify [db c-spec source retries retry-delay-sec]
  (with-consumer [c-spec c-spec]
    (let [c (consumer c-spec)]
      (loop [prev-topic-data empty-data
             retries         retries]
        (let [source-data (by-topic-key db source)
              topic-data  (refresh c prev-topic-data (topics source-data))
              matches     (matches source-data topic-data)]
          (if (or matches (<= retries 0))
            matches
            (do (sleep retry-delay-sec)
                (recur topic-data (dec retries)))))))))

(defn verify-sync
  "Verify up to `retries` with delay. If verify fails, perform sync and final 
   verify."
  [db c-spec p-spec source retries retry-delay-sec]
  (with-consumer [c-spec c-spec]
    (let [c (consumer c-spec)]
      (loop [prev-topic-data empty-data
             attempt         1
             retries         retries
             post-sync-try   false
             sync-count      0]
        (let [source-map (by-topic-key db source)
              topic-data (->> source-map
                              topics
                              (refresh c prev-topic-data))
              topic-map  (merge-topic-key (:data topic-data))
              diff       (diff source-map topic-map)
              matches    (diff-matches? diff)]
          (cond (or matches post-sync-try) {:matches    matches
                                            :attempts   attempt
                                            :sync       post-sync-try
                                            :sync-count sync-count}
                (<= retries 0) (recur topic-data attempt 0 true (sync-kafka p-spec diff))
                :default (recur topic-data (inc attempt) (dec retries) false sync-count)))))))
