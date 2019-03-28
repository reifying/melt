(ns melt.source
  (:require [clojure.spec.alpha :as s]))

(s/def ::keyed (s/keys :req [::keys]))

(s/def ::message (s/keys :req [::topic ::key ::value]))

(defn xform [source]
  (get source ::xform (map identity)))

(defn message [source row]
  (if-let [keys (get source ::keys)]
    {::value row
     ::key    (select-keys row keys)}
    {::value row}))
