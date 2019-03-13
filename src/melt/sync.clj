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

(defn deleted [diff]
  (apply dissoc
         (:topic-only diff)
         (keys (:table-only diff))))

(defn send-tombstones [deleted send-fn]
  (doseq [[[topic k] _] deleted]
    (send-fn topic
             (serial/write-str k)
             nil)))

(defn sync [db consumer-props producer-props channel]
  (let [diff       (diff db consumer-props channel)
        table-only (seq (:table-only diff))
        deleted    (seq (deleted diff))]
    (if (or deleted table-only)
      (lk/with-producer
        (fn [p]
          (if table-only
            (sync-with-sender table-only (lk/default-send-fn p)))
          (if deleted
            (send-tombstones deleted (lk/default-send-fn p))))
        {:producer-properties producer-props}))))
