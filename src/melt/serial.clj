(ns melt.serial
  (:require [clojure.data.json :as json]
            [melt.util :refer [format-date-time]]))

;; TODO Implement a key serializer that can be used by the DefaultPartitioner.
;;      Do not depend on json or clojure structures since partition calculation 
;;      could vary by implementation

(defn write-str [x]
  (letfn [(json-value-fn [k v]
            (cond (instance? java.sql.Timestamp v) (format-date-time k v)
                  (instance? java.sql.Blob v) (.getBytes v 1 (.length v))
                  (instance? java.sql.Clob v) (.getSubString v 1 (.length v))
                  :else v))]
    (json/write-str x :value-fn json-value-fn)))

(def read-str json/read-str)

(def lossy-identity (comp read-str write-str))

(defn fuzz [table-map]
  (reduce-kv (fn [m k v] (assoc m k (lossy-identity v))) {} table-map))
