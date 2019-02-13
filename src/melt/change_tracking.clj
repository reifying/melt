(ns melt.change-tracking
  (:require [clojure.java.jdbc :as jdbc]
            [melt.channel :as ch]
            [melt.config :refer [db]]))

(defn qualified-table-name [table]
  (str (::ch/schema table) "." (::ch/name table)))

(defn change-sql [table]
  (String/join " "
               ["Select ct.*"
                "From CHANGETABLE(CHANGES "
                (qualified-table-name table)
                ", ?) As ct Order By ct.sys_change_version"]))

(defn changes [table change-version]
  (jdbc/query db [(change-sql table) change-version]))
