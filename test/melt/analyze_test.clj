(ns melt.analyze-test
  (:require [clj-time.coerce :refer [to-sql-time]]
            [clj-time.local :as l]
            [midje.sweet :refer [fact =>]]
            [melt.analyze :as a]))

(fact "`schema` reads a minimal amount of info describing the DB schema"
      (first (a/schema)) =>
      {:name    "SalesOrderHeader"
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

(fact "`read-table` reads all the rows of a table and creates a map on primary keys"
      (get (a/read-table {:name   "CustomerAddress"
                          :cat    "AdventureWorks"
                          :schema "SalesLT"
                          :keys   [:customerid :addressid]})
           {:customerid 29926
            :addressid  638})
      =>
      {:customerid   29926
       :addressid    638
       :addresstype  "Main Office"
       :rowguid      "ACDA2178-9538-46D6-B023-01EB79BF7872"
       :modifieddate (to-sql-time (l/to-local-date-time "2007-08-01"))})
