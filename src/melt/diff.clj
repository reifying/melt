(ns melt.diff
  (:require [melt.analyze :as a]
            [melt.config :refer [table->topic-name]]
            [melt.read-topic :as rt]
            [melt.serial :as serial]))

(defn diff [consumer-props table]
  (let [topic     (table->topic-name table)
        table-map (a/read-table table)
        topic-map (rt/read-topic consumer-props topic)
        diff      (serial/lossy-diff table-map topic-map)]
    {:table-only (select-keys table-map (map key (first diff)))
     :topic-only (select-keys topic-map (map key (second diff)))}))
