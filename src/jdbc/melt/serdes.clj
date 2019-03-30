(ns jdbc.melt.serdes
  (:require [jdbc.melt :as melt])
  (:import [org.apache.kafka.common.serialization
            StringDeserializer StringSerializer]))

(gen-class
 :name "jdbc.melt.serdes.Serializer"
 :state "state"
 :init "init-ser"
 :prefix "-"
 :implements [org.apache.kafka.common.serialization.Serializer])

(gen-class
 :name "jdbc.melt.serdes.Deserializer"
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
    (melt/write-key data)
    (melt/write-str data)))

(defn -serialize
  ([this topic data]
   (.serialize (delegate this) topic (write this data)))
  ([this topic headers data]
   (.serialize (delegate this) topic headers (write this data))))

(defn -deserialize
  ([this topic data]
   (if (some? data)
     (melt/read-str (.deserialize (delegate this) topic data))))
  ([this topic headers data]
   (if (some? data)
     (melt/read-str (.deserialize (delegate this) topic headers data)))))

(defn -close [this]
  (.close (delegate this)))
