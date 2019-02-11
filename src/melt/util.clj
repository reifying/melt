(ns melt.util
  (:require [clj-time.coerce :as c]
            [clj-time.format :as f]
            [clojure.spec.alpha :as s])
  (:import [java.util Date Calendar]
           [org.joda.time DateTimeZone]))

(defn mkdirs
  "Create the directories if they don't exist. Returns whether the
   directory exists after this call."
  [path]
  (let [file (java.io.File. path)]
    (if (not (.exists file))
      (.mkdirs file)
      true)))

(defn has-time? [^Date date]
  (let [time-parts [Calendar/MINUTE Calendar/MILLISECOND Calendar/SECOND Calendar/HOUR]
        cal        (doto (Calendar/getInstance) (.setTime date))]
    (some #(< 0 (.get cal %)) time-parts)))

(defn- format-jdate [formatter jd]
  (f/unparse (f/formatters formatter) (c/from-date jd)))

(def format-date (partial format-jdate :date))

(def format-time (partial format-jdate :date-time))

(defn format-date-time [k v]
  (if (and (re-find #"date$" (name k))
           (not (has-time? v)))
    (format-date v)
    (format-time v)))

(defn conform-or-throw [spec x]
  (let [parsed (s/conform spec x)]
    (if (= parsed ::s/invalid)
      (throw (ex-info "Invalid" (s/explain-data spec x)))
      parsed)))
