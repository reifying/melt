(ns melt.repl
  (:require [melt.config :as c]
            [clojure.java.jdbc :as jdbc]))

(jdbc/delete! c/db "SalesLT.CustomerAddress" ["addressid = ?" 888])

(jdbc/delete! c/db "saleslt.address" ["addressid = ?" 888])

(jdbc/query c/db ["Select * From saleslt.address Where addressid = 888"])

(jdbc/insert! c/db "saleslt.address"
              {;  :addressid     888
;  :modifieddate  (java.util.Date.)
               :countryregion "United States"
               :city          "Kelso"
               :rowguid       "834AE73A-01A9-4850-9F8C-6DB5D1344AAD"
               :addressline1  "Three Rivers Mall"
               :addressline2  nil
               :postalcode    "98626"
               :stateprovince "Washington"})

(jdbc/execute! c/db ["Update saleslt.address Set addressid=888 Where addressid=11383"])

(jdbc/query c/db ["Select * From SalesLT.CustomerAddress Where addressid = 888"])

{:customerid   30051
 :addressid    888
 :addresstype  "Main Office"
 :rowguid      "A303B277-ECC4-49D1-AA81-C39D5193D035"
 :modifieddate #inst "2007-07-01T05:00:00.000000000-00:00"}

(jdbc/with-db-transaction [t-con c/db]
  (jdbc/db-set-rollback-only! t-con)
  (jdbc/delete! t-con "SalesLT.CustomerAddress" ["addressid = ?" 888])
  (jdbc/delete! t-con "saleslt.address" ["addressid = ?" 888])
  (println "Exists?" (jdbc/query t-con ["Select * From saleslt.address Where addressid = 888"])))
