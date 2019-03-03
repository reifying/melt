(ns melt.rest-api-test
  (:require [melt.rest-api :as r]
            [midje.sweet :refer [fact => contains]]))

(fact "`ok` returns a HTTP 200 response"
      (r/ok ...body...)
      => {:status  200
          :body    ...body...
          :headers nil})

(fact "`db-interceptor` associates the db with the context"
      ((:enter (r/db-interceptor (atom ...db...))) {})
      => {:request {:database ...db...}})

(fact "`db-interceptor` applies tx-data to the db"
      (:request ((:leave (r/db-interceptor (atom {})))
                 {:tx-data [assoc ...key... ...val...]}))
      => {:database {...key... ...val...}})

(fact "`entity-render` applies 200 response when result found"
      (:response ((:leave r/entity-render) {:result ...body...}))
      => (contains {:body   ...body...
                    :status 200}))

(fact "`fields-match?` is true when all fields match the entity"
      (r/fields-match? {:a "1"
                        :b "2"}
                       {:a "1"
                        :b "2"
                        :c "3"}) => true
      (r/fields-match? {:a "1"
                        :b "2"
                        :c "3"}
                       {:a "1"
                        :b "2"}) => false
      (r/fields-match? {:a "1"}
                       {:a "2"}) => false)

(fact "`fields-match?` converts entity values to strings for the comparison"
      (r/fields-match? {:a "1"}
                       {:a 1}) => true)

(fact "`find-by-fields` searches a sequential of entities for matches"
      (r/find-by-fields
       {:a "1"}
       [{:b 1}
        {:a 1
         :b 1}
        {:a 1
         :c 1}])
      => [{:a 1
           :b 1}
          {:a 1
           :c 1}])

(fact "`find-by-fields` with empty fields matches full set"
      (r/find-by-fields
       {}
       [{:b 1}
        {:c 1}])
      => [{:b 1}
          {:c 1}])

(fact "`entities` lists the values for a given topic"
      (r/entities {:request {:database {:data {...topic-name... {...key1... ...val1...
                                                                 ...key2... ...val2...}}}}}
                  ...topic-name...)
      => [...val1... ...val2...])
