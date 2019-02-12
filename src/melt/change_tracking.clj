(ns melt.change-tracking
  (:require [clojure.java.jdbc :as jdbc]
            [melt.channel :as ch]
            [melt.config :refer [db]]))

(defn- select-columns [table]
  (->> (::ch/keys table)
       (map name)
       (cons "sys_change_operation")
       (map #(str "ct." %))
       (String/join ", ")))

(defn- qualified-table-name [table]
  (str (::ch/schema table) "." (::ch/name table)))

(defn- change-sql [table]
  (String/join " "
               ["Select"
                (select-columns table)
                "From CHANGETABLE(CHANGES "
                (qualified-table-name table)
                ", ?) As ct"]))

(defn changes [table change-version]
  (jdbc/query db [(change-sql table) change-version]))
