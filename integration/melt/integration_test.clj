(ns melt.integration-test
  (:require [melt.load-kafka :as lk]
            [melt.read-topic :as rt]
            [midje.sweet :refer [fact =>]]))

(def bootstrap-servers "localhost:9092")

(def producer-props
  (doto (java.util.Properties.)
    (.put "bootstrap.servers" bootstrap-servers)
    (.put "acks" "all")
    (.put "key.serializer" "org.apache.kafka.common.serialization.StringSerializer")
    (.put "value.serializer" "org.apache.kafka.common.serialization.StringSerializer")))

(def consumer-props
  (doto (java.util.Properties.)
    (.put "bootstrap.servers" bootstrap-servers)
    (.put "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
    (.put "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
    (.put "group.id" "melt.integration-test")))

(fact "a table written to a topic may be read from a topic"
      (lk/load-topics-from-tables producer-props)
      ((rt/read-topic consumer-props "melt.SalesLT.Address") {:addressid 603})
      =>
      {"city"          "Killeen"
       "addressline2"  nil
       "modifieddate"  "2007-08-01"
       "rowguid"       "0E6E9E86-A637-4FD5-A945-AC342BFD715B"
       "postalcode"    "76541"
       "addressline1"  "9500b E. Central Texas Expressway"
       "countryregion" "United States"
       "stateprovince" "Texas"
       "addressid"     603})
