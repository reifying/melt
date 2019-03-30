(ns jdbc.melt-test
  (:require [midje.sweet :refer [fact =>]]
            [jdbc.melt.common :refer [db]]
            [jdbc.melt :as melt])
  (:import [org.apache.kafka.clients.producer MockProducer]))

(fact "`schema` reads a minimal amount of info describing the DB schema"
      (first (filter #(= "SalesOrderHeader" (::melt/name %)) (melt/schema db))) =>
      #::melt{:name    "SalesOrderHeader"
              :cat     "AdventureWorks"
              :schema  "SalesLT"
              :columns ["ModifiedDate"
                        "rowguid"
                        "Comment"
                        "TotalDue"
                        "Freight"
                        "TaxAmt"
                        "SubTotal"
                        "CreditCardApprovalCode"
                        "ShipMethod"
                        "BillToAddressID"
                        "ShipToAddressID"
                        "CustomerID"
                        "AccountNumber"
                        "PurchaseOrderNumber"
                        "SalesOrderNumber"
                        "OnlineOrderFlag"
                        "Status"
                        "ShipDate"
                        "DueDate"
                        "OrderDate"
                        "RevisionNumber"
                        "SalesOrderID"]
              :keys    [:salesorderid]})

(defn count-send-fn [counts-atom]
  (fn [topic k v]
    (swap! counts-atom (fn [tc] (update tc topic #(inc (or % 0)))))))

(defn assoc-send-fn [records-atom]
  (fn [_ k v] (swap! records-atom
                     (fn [m] (assoc m
                                    (melt/lossy-identity k)
                                    (melt/lossy-identity v))))))

(defn topic [source]
  (str "melt." (::melt/schema source) "." (::melt/name source)))

(defn assoc-topic-xform [source]
  (map #(assoc % ::melt/topic (topic source))))

(defn sources []
  (map #(assoc % ::melt/xform (assoc-topic-xform %)) (melt/schema db)))

(fact "`load-with-sender` reads tables and sends records to Kafka"
      (let [topic-counts (atom (sorted-map))]
        (melt/load-with-sender db (sources) (count-send-fn topic-counts))
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

(fact "`load-with-sender` reads queries and sends records to Kafka"
      (let [records   (atom {})
            sender-fn (assoc-send-fn records)
            source    #::melt{:sql "Select * From SalesLT.Address Where addressid In (9, 11)"
                              :keys  [:addressid]
                              :xform (map #(assoc % ::melt/topic "melt.topic"))}]
        (melt/load-with-sender db [source] sender-fn)
        @records
        =>
        {{:addressid 9}  {:addressid     9
                          :addressline1  "8713 Yosemite Ct."
                          :addressline2  nil
                          :city          "Bothell"
                          :countryregion "United States"
                          :modifieddate  "2006-07-01"
                          :postalcode    "98011"
                          :rowguid       "268AF621-76D7-4C78-9441-144FD139821A"
                          :stateprovince "Washington"}
         {:addressid 11} {:addressid     11
                          :addressline1  "1318 Lasalle Street"
                          :addressline2  nil
                          :city          "Bothell"
                          :countryregion "United States"
                          :modifieddate  "2007-04-01"
                          :postalcode    "98011"
                          :rowguid       "981B3303-ACA2-49C7-9A96-FB670785B269"
                          :stateprovince "Washington"}}))

(fact "`load-with-sender` supports arbitrary transformations"
      (let [records   (atom {})
            sender-fn (assoc-send-fn records)
            xform-fn  (fn [m] (update m ::melt/value
                                      #(reduce-kv
                                        (fn [m k v]
                                          (assoc m k (if (= k :addressid) v 1)))
                                        {} %)))
            source   #::melt{:sql         "Select * From SalesLT.Address Where addressid In (9, 11)"
                             :keys        [:addressid]
                             :xform (comp (map xform-fn)
                                          (map #(assoc % ::melt/topic "melt.topic")))}]
        (melt/load-with-sender db [source] sender-fn)
        @records
        =>
        {{:addressid 9}  {:addressid     9
                          :addressline1  1
                          :addressline2  1
                          :city          1
                          :countryregion 1
                          :modifieddate  1
                          :postalcode    1
                          :rowguid       1
                          :stateprovince 1}
         {:addressid 11} {:addressid     11
                          :addressline1  1
                          :addressline2  1
                          :city          1
                          :countryregion 1
                          :modifieddate  1
                          :postalcode    1
                          :rowguid       1
                          :stateprovince 1}}))
