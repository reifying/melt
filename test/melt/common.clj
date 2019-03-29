(ns melt.common)

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
