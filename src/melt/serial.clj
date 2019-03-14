(ns melt.serial
  (:require [clojure.data.json :as json]
            [melt.util :refer [format-date-time]])
  (:import [org.apache.kafka.common.serialization
            Deserializer Serializer StringDeserializer StringSerializer]))

;; TODO Implement a key serializer that can be used by the DefaultPartitioner.
;;      Do not depend on json or clojure structures since partition calculation 
;;      could vary by implementation

(defn ensure-sorted [x]
  (if (and (map? x) (not (sorted? x)))
    (into (sorted-map) x)
    x))

(defn write-str [x]
  (letfn [(json-value-fn [k v]
            (cond (instance? java.sql.Timestamp v) (format-date-time k v)
                  (instance? java.sql.Blob v) (.getBytes v 1 (.length v))
                  (instance? java.sql.Clob v) (.getSubString v 1 (.length v))
                  :else v))]
    (json/write-str x :value-fn json-value-fn)))

(def write-key (comp write-str ensure-sorted))

(defn read-str [s]
  (json/read-str s :key-fn keyword))

(gen-class
 :name "melt.Serializer"
 :state "state"
 :init "init-ser"
 :prefix "-"
 :implements [org.apache.kafka.common.serialization.Serializer])

(gen-class
 :name "melt.Deserializer"
 :state "state"
 :init "init-des"
 :prefix "-"
 :implements [org.apache.kafka.common.serialization.Deserializer])

(defn delegate [this]
  (:delegate (deref (.state this))))

(defn is-key? [this]
  (:is-key (deref (.state this))))

(defn -init-des []
  [[] (atom {:delegate (StringDeserializer.)
             :is-key   false})])

(defn -init-ser []
  [[] (atom {:delegate (StringSerializer.)
             :is-key   false})])

(defn -configure [this configs is-key]
  (swap! (.state this) #(assoc % :is-key is-key))
  (.configure (delegate this) configs is-key))

(defn write [this data]
  (if (is-key? this)
    (write-key data)
    (write-str data)))

(defn -serialize
  ([this topic data]
   (.serialize (delegate this) topic (write this data)))
  ([this topic headers data]
   (.serialize (delegate this) topic headers (write this data))))

(defn -deserialize
  ([this topic data]
   (if (some? data)
     (read-str (.deserialize (delegate this) topic data))))
  ([this topic headers data]
   (if (some? data)
     (read-str (.deserialize (delegate this) topic headers data)))))

(defn -close [this]
  (.close (delegate this)))

(def lossy-identity (comp read-str write-str))

(defn fuzz [table-map]
  (reduce-kv (fn [m k v] (assoc m k (lossy-identity v))) {} table-map))
