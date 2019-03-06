(ns melt.jdbc
  (:require [clojure.data :as data]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [melt.channel :as ch]
            [melt.config :refer [db ignorable-schemas schema-file-path
                                 abort-on-schema-change]]
            [melt.util :refer [mkdirs]]))

(defn- user-schema? [{:keys [table_schem]}]
  (not (contains? ignorable-schemas table_schem)))

(defn- table [{:keys [table_schem table_cat table_name]}]
  #::ch{:name   table_name
        :cat    table_cat
        :schema table_schem})

(defn- group-by-table
  [table-map column-map]
  (update-in table-map
             [(table column-map) ::ch/columns]
             (fn [columns] (conj columns (:column_name column-map)))))

(defn- primary-keys [table]
  (jdbc/with-db-metadata [md db]
    (->> (.getPrimaryKeys md (::ch/cat table) (::ch/schema table) (::ch/name table))
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
  (let [table-set (table-set)]
    (jdbc/with-db-metadata [md db]
      (->> (.getColumns md nil nil "%" nil)
           jdbc/metadata-query
           (filter (partial contains-table? table-set))
           (reduce group-by-table {})
           (map #(apply merge %))
           (map #(assoc % ::ch/keys (primary-keys %)))
           set))))

(defn schema->channels [topic-fn]
  (map #(assoc % ::ch/topic-fn topic-fn) (schema)))

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

(defn qualified-table-name [#::ch{:keys [schema name]}]
  (str "[" schema "].[" name "]"))

(defn select-all-sql [table]
  (str "Select * From " (qualified-table-name table)))

(defn- merge-query [channel sql]
  (letfn [(merge-by-key [m row]
            (assoc m (select-keys row (::ch/keys channel)) row))
          (apply-transform [rows]
                           (let [xfn (::ch/transform-fn channel)]
                             (if xfn (map xfn rows) rows)))]
    (reduce merge-by-key {} (apply-transform (jdbc/query db [sql])))))

(defmethod ch/read-channel ::ch/query [query]
  (merge-query query (::ch/sql query)))

(defmethod ch/read-channel ::ch/table [table]
  (merge-query table (select-all-sql table)))

(defn channel-content [channels]
  (map (fn [c] {::ch/channel c
                ::ch/records (ch/read-channel c)})
       channels))

(s/fdef channel-content
  :ret (s/* ::ch/content))
