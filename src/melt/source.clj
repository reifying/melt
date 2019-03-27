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

; (defn transduce-source [f xform val db source]
;   (transduce (comp xform (map (partial message source)))
;              f
;              val
;              (mdb/reducible-source db source)))

; (defn reduce-source [f val db source]
;   (transduce-source (completing f) (map (xform source)) val db source))

; (defn- merge-by-key [acc row]
;   (assoc acc (get row ::key) (get (s/assert ::keyed row) ::message)))

; (defn source-by-key [db source]
;   (reduce-source merge-by-key {} db source))
