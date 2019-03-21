(ns melt.kafka
  (:import [org.apache.kafka.clients.consumer Consumer KafkaConsumer]
           [org.apache.kafka.clients.producer Producer KafkaProducer]))

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
