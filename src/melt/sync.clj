(ns melt.sync
  (:require [melt.config :refer [table->topic-name]]
            [melt.diff :refer [diff]]
            [melt.load-kafka :as lk])
  (:refer-clojure :exclude [sync]))

(defn sync [consumer-props producer-props table]
  (let [diff (diff consumer-props table)]
    (if (seq (:table-only diff))
      (lk/load-topics
       {:producer-properties producer-props
        :records-fn          (fn [] [[(table->topic-name table) (:table-only diff)]])}))))
