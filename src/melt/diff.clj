(ns melt.diff
  (:require [clojure.data :as data]
            [clojure.spec.alpha :as spec]
            [melt.jdbc :as mdb]
            [melt.source :as source]
            [melt.read-topic :as rt]
            [melt.serial :as serial]))

(defn merge-by-key [acc message]
  (let [m (spec/assert ::source/message message)]
    (assoc acc ((juxt ::source/topic ::source/key) m) (get m ::source/value))))

(defn by-topic-key [db source]
  (transduce (comp (map (partial source/message source))
                   (source/xform source))
             (completing merge-by-key)
             {}
             (mdb/reducible-source db source)))

(defn merge-topic-key [topic-map]
  (reduce-kv (fn [topic-key-m topic records]
               (merge topic-key-m
                      (reduce-kv (fn [m k v] (assoc m [topic k] v)) {} records)))
             {} topic-map))

(defn- topic-map [c-spec topics]
  (merge-topic-key (rt/read-topics c-spec topics)))

(defn topics [m]
  (distinct (map first (keys m))))

(defn diff [db c-spec source]
  (let [source-map (by-topic-key db source)
        topic-map  (topic-map c-spec (topics source-map))
        diff       (data/diff (serial/fuzz source-map) topic-map)]
    {:table-only (select-keys source-map (map key (first diff)))
     :topic-only (select-keys topic-map (map key (second diff)))}))
