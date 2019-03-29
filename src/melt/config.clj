(ns melt.config)

(def ignorable-schemas #{"sys" "INFORMATION_SCHEMA"})

(def schema-file-path (or (System/getenv "SCHEMA_PATH") "target/schema.edn"))

; Set to TRUE to enable
(def abort-on-schema-change (System/getenv "ABORT_ON_SCHEMA_CHANGE"))
