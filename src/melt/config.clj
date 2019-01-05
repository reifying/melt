(ns melt.config)

(def mssql-host   (System/getenv "TEST_MSSQL_HOST"))
(def mssql-port   (or (System/getenv "TEST_MSSQL_PORT") "1433"))
(def mssql-user   (System/getenv "TEST_MSSQL_USER"))
(def mssql-pass   (System/getenv "TEST_MSSQL_PASS"))
(def mssql-dbname (System/getenv "TEST_MSSQL_NAME"))
(def jtds-host    (or (System/getenv "TEST_JTDS_HOST") mssql-host))
(def jtds-port    (or (System/getenv "TEST_JTDS_PORT") mssql-port))
(def jtds-user    (or (System/getenv "TEST_JTDS_USER") mssql-user))
(def jtds-pass    (or (System/getenv "TEST_JTDS_PASS") mssql-pass))
(def jtds-dbname  (or (System/getenv "TEST_JTDS_NAME") mssql-dbname))

(def db {:dbtype   "jtds"
         :dbname   jtds-dbname
         :host     jtds-host
         :port     jtds-port
         :user     jtds-user
         :password jtds-pass})

(def ignorable-schemas #{"sys" "INFORMATION_SCHEMA"})

(def schema-file-path (or (System/getenv "SCHEMA_PATH") "target/schema.edn"))

; Set to TRUE to enable
(def abort-on-schema-change (System/getenv "ABORT_ON_SCHEMA_CHANGE"))