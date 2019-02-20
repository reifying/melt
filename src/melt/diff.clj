(ns melt.diff
  (:require [melt.channel :as ch]
            [melt.read-topic :as rt]
            [melt.serial :as serial]))

(defn by-topic-key [channel]
  (let [content  (ch/read-channel channel)
        topic-fn (::ch/topic-fn channel)]
    (reduce-kv (fn [m k v]
                 (let [topic (topic-fn channel v)]
                   (assoc m [topic k] v))) {} content)))

(defn merge-topic-key [topic-map]
  (reduce-kv (fn [topic-key-m topic records]
               (merge topic-key-m
                      (reduce-kv (fn [m k v] (assoc m [topic k] v)) {} records)))
             {} topic-map))

(defn- topic-map [consumer-props topics]
  (merge-topic-key (rt/read-topics consumer-props topics)))

(defn topics [m]
  (distinct (map first (keys m))))

(defn diff [consumer-props channel]
  (let [channel-map (by-topic-key channel)
        topic-map   (topic-map consumer-props (topics channel-map))
        diff        (clojure.data/diff (serial/fuzz channel-map) topic-map)]
    {:table-only (select-keys channel-map (map key (first diff)))
     :topic-only (select-keys topic-map (map key (second diff)))}))
