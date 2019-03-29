(ns melt.jdbc-test
  (:require [midje.sweet :refer [fact =>]]
            [melt.common :refer [db]]
            [melt.jdbc :as mdb]
            [melt.source :as source]))

(fact "`schema` reads a minimal amount of info describing the DB schema"
      (first (filter #(= "SalesOrderHeader" (::source/name %)) (mdb/schema db))) =>
      #:melt.source{:name    "SalesOrderHeader"
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
