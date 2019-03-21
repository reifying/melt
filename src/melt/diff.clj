(ns melt.diff
  (:require [clojure.data :as data]
            [melt.channel :as ch]
            [melt.read-topic :as rt]
            [melt.serial :as serial]))

(defn by-topic-key [db channel]
  (let [content  (ch/read-channel db channel)
        topic-fn (::ch/topic-fn channel)]
    (reduce-kv (fn [m k v]
                 (let [topic (topic-fn channel v)]
                   (assoc m [topic k] v))) {} content)))

(defn merge-topic-key [topic-map]
  (reduce-kv (fn [topic-key-m topic records]
               (merge topic-key-m
                      (reduce-kv (fn [m k v] (assoc m [topic k] v)) {} records)))
             {} topic-map))

(defn- topic-map [c-spec topics]
  (merge-topic-key (rt/read-topics c-spec topics)))

(defn topics [m]
  (distinct (map first (keys m))))

(defn diff [db c-spec channel]
  (let [channel-map (by-topic-key db channel)
        topic-map   (topic-map c-spec (topics channel-map))
        diff        (data/diff (serial/fuzz channel-map) topic-map)]
    {:table-only (select-keys channel-map (map key (first diff)))
     :topic-only (select-keys topic-map (map key (second diff)))}))
