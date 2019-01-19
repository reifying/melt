(ns melt.analyze-test
  (:require [clojure.test :as t]
            [melt.analyze :as a]))

(t/deftest reading-schema
  (t/testing "it reads a minimal amount of info describing the DB schema"
    (t/is (= (get (a/schema) {:name   "CustomerAddress"
                              :cat    "AdventureWorks"
                              :schema "SalesLT"})
             {:columns ["ModifiedDate" "rowguid" "AddressType" "AddressID" "CustomerID"]
              :keys    ["CustomerID" "AddressID"]}))))
