(ns melt.load-kafka-test
  (:require [melt.load-kafka :as lk]
            [midje.sweet :refer [fact =>]])
  (:import [org.apache.kafka.clients.producer MockProducer]))

(fact "`load-topics-from-tables` reads tables and sends records to Kafka"
      (let [topic-counts (atom (sorted-map))]
        (lk/load-topics-from-tables
         (MockProducer.)
         (fn [_ topic]
           (fn [k v]
             (swap! topic-counts
                    (fn [tc] (update tc topic #(inc (or % 0))))))))
        @topic-counts
        =>
        {"melt.SalesLT.Address"                        450
         "melt.SalesLT.Customer"                       847
         "melt.SalesLT.CustomerAddress"                417
         "melt.SalesLT.Product"                        295
         "melt.SalesLT.ProductCategory"                41
         "melt.SalesLT.ProductDescription"             762
         "melt.SalesLT.ProductModel"                   128
         "melt.SalesLT.ProductModelProductDescription" 762
         "melt.SalesLT.SalesOrderDetail"               542
         "melt.SalesLT.SalesOrderHeader"               32
         "melt.dbo.BuildVersion"                       1}))
