(ns melt.config)

(def host   (System/getenv "MELT_DB_HOST"))
(def port   (or (System/getenv "MELT_DB_PORT") "1433"))
(def user   (System/getenv "MELT_DB_USER"))
(def pass   (System/getenv "MELT_DB_PASS"))
(def dbname (System/getenv "MELT_DB_NAME"))

(def db {:dbtype   "jtds"
         :dbname   dbname
         :host     host
         :port     port
         :user     user
         :password pass})

(def ignorable-schemas #{"sys" "INFORMATION_SCHEMA"})

(def schema-file-path (or (System/getenv "SCHEMA_PATH") "target/schema.edn"))

; Set to TRUE to enable
(def abort-on-schema-change (System/getenv "ABORT_ON_SCHEMA_CHANGE"))

(defn table->topic-name [#:melt.channel{:keys [schema name]} _]
  (str "melt." schema "." name))
