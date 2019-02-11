(ns melt.channel
  (:require [clojure.spec.alpha :as s]
            [melt.util :refer [conform-or-throw]]))

(s/def ::channel (s/or ::table (s/keys :req [::schema ::name ::keys]
                                       :opt [::transform-fn ::topic-fn])
                       ::query (s/keys :req [::sql ::keys ::topic-fn]
                                       :opt [::transform-fn])))

(s/def ::content (s/keys :req [::channel ::records]))

(defmulti read-channel #(key (conform-or-throw ::channel %)))
