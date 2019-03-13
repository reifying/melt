(ns melt.channel
  (:require [clojure.spec.alpha :as s]
            [melt.util :refer [conform-or-throw]]))

;; retain order of key-fn before key-list so that `s/conform` short cirtuits to
;; key-fn since ::keys may also be present on channel (due to reading schema)
(s/def ::keyed
  (s/or ::key-fn   (s/keys :req [::key-fn])
        ::key-list (s/keys :req [::keys])))

(s/def ::channel
  (s/or ::table (s/keys :req [::schema ::name]
                        :opt [::transform-fn ::topic-fn])
        ::query (s/keys :req [::sql ::topic-fn]
                        :opt [::transform-fn])))

(s/def ::content (s/keys :req [::channel ::records]))

(defmulti read-channel (fn [_ ch] (key (conform-or-throw ::channel ch))))
