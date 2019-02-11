(ns melt.analyze-test
  (:require [clj-time.coerce :refer [to-sql-time]]
            [clj-time.local :as l]
            [midje.sweet :refer [fact =>]]
            [melt.analyze :as a]
            [melt.channel :as ch]))

