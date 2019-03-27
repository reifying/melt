(ns melt.jdbc
  (:require [clojure.data :as data]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [melt.config :refer [db ignorable-schemas schema-file-path
                                 abort-on-schema-change]]
            [melt.source :as source]
            [melt.util :refer [mkdirs]]))

(defn- user-schema? [{:keys [table_schem]}]
  (not (contains? ignorable-schemas table_schem)))

(defn- table [{:keys [table_schem table_cat table_name]}]
  #::source{:name   table_name
        :cat    table_cat
        :schema table_schem})

(defn- group-by-table
  [table-map column-map]
  (update-in table-map
             [(table column-map) ::source/columns]
             (fn [columns] (conj columns (:column_name column-map)))))

(defn- primary-keys [table]
  (jdbc/with-db-metadata [md db]
    (->> (.getPrimaryKeys md (::source/cat table) (::source/schema table) (::source/name table))
         jdbc/metadata-query
         (map (comp keyword clojure.string/lower-case :column_name)))))

(defn- table-set []
  (set (jdbc/with-db-metadata [md db]
         (->> (.getTables md nil nil nil (into-array String ["TABLE"]))
              jdbc/metadata-query
              (filter user-schema?)
              (map table)))))

(defn- contains-table? [table-set column-map]
  (contains? table-set (table column-map)))

(defn schema []
  (let [table-set (table-set)
        id        (fn [t] (clojure.string/join
                           "." ((juxt ::source/cat ::source/schema ::source/name) t)))]
    (jdbc/with-db-metadata [md db]
      (->> (.getColumns md nil nil "%" nil)
           jdbc/metadata-query
           (filter (partial contains-table? table-set))
           (reduce group-by-table {})
           (map #(apply merge %))
           (map #(assoc % ::source/keys (primary-keys %)))
           (apply sorted-set-by #(compare (id %1) (id %2)))))))

(defn cached-schema-file []
  (let [f (io/as-file schema-file-path)]
    (mkdirs (.getParent f))
    f))

(defn file-schema []
  (let [f (cached-schema-file)]
    (if (.exists f)
      (read-string (slurp f)))))

(def cached-schema (memoize schema))

(defn save-schema
  ([] (save-schema (schema)))
  ([coll] (spit (cached-schema-file) (with-out-str (pprint coll)))))

(defn schema-diff []
  (let [cached (cached-schema)
        latest (schema)
        diff   (data/diff cached latest)]
    {:only-old   (first diff)
     :only-new   (second diff)
     :new-schema latest}))

(defn schema-changed? [diff]
  (some some? (vals (select-keys diff [:only-new :only-old]))))

(defn schema-check []
  (let [diff (schema-diff)]
    (if (and (schema-changed? diff)
             (= "TRUE" abort-on-schema-change))
      false
      diff)))

(defn qualified-table-name [#::source{:keys [schema name]}]
  (str "[" schema "].[" name "]"))

(defn select-all-sql [table]
  (str "Select * From " (qualified-table-name table)))

(defn query-source [db source]
  (let [sql-params (get source
                        ::source/sql-params
                        [(get source ::source/sql (select-all-sql source))])]
    (jdbc/query db sql-params)))

(defn reducible-source [db source]
  (let [sql-params (get source
                        ::source/sql-params
                        [(get source ::source/sql (select-all-sql source))])]
    (jdbc/reducible-query db sql-params)))
