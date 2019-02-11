(ns melt.sync
  (:require [melt.diff :refer [diff]]
            [melt.load-kafka :as lk]
            [melt.serial :as serial])
  (:refer-clojure :exclude [sync]))

(defn sync-with-sender [db-only send-fn]
  (doseq [[[topic k] v] db-only]
    (send-fn topic
             (serial/write-str k)
             (serial/write-str v))))

(defn sync-with-producer [db-only producer-options]
  (lk/with-producer
    (fn [p] (sync-with-sender db-only (lk/default-send-fn p)))
    producer-options))

(defn sync [consumer-props producer-props channel]
  (let [diff (diff consumer-props channel)]
    (if (seq (:table-only diff))
      (sync-with-producer (:table-only diff)
                          {:producer-properties producer-props}))))
