(ns melt.rest-api
  (:require [io.pedestal.http :as http]
            [io.pedestal.http.route :as route]
            [melt.read-topic :as rt]))

(defn- response [status body & {:as headers}]
  {:status  status
   :body    body
   :headers headers})

(def ok (partial response 200))

(defn db-interceptor [db]
  {:name  :database-interceptor
   :enter (fn [context]
            (update context :request assoc :database @db))
   :leave (fn [context]
            (if-let [[op & args] (:tx-data context)]
              (do
                (apply swap! db op args)
                (assoc-in context [:request :database] @db))
              context))})

(def entity-render
  {:name  :entity-render
   :leave (fn [context]
            (if-let [x (:result context)]
              (assoc context :response (ok x))
              context))})

(defn fields-match? [fields entity]
  (= fields
     (reduce-kv #(assoc %1 %2 (str %3))
                {}
                (select-keys entity (keys fields)))))

(defn find-by-fields [fields entities]
  (let [results (filter (partial fields-match? fields) entities)]
    results))

(defn entities [context topic]
  (if-let [topic-data (get-in context [:request :database :data topic])]
    (or (vals topic-data) [])))

(defn topic-view [topic]
  {:name  :topic-view
   :enter (fn [context]
            (if-let [the-list (entities context topic)]
              (let [params   (get-in context [:request :query-params] {})
                    filtered (find-by-fields params the-list)]
                (assoc context :result filtered :topic topic))
              context))})

(defn topic-query-route [db-interceptor topic]
  (let [common-interceptors [http/json-body entity-render db-interceptor]
        path                (str "/" topic)]
    (println "Creating route for " path)
    [path
     :get
     (conj common-interceptors (topic-view topic))
     :route-name (keyword (str topic "-query"))]))

(defn routes [topics topic-data]
  (route/expand-routes
   (set (map (partial topic-query-route (db-interceptor topic-data))
             topics))))

(defn start-server [topics topic-data port]
  (http/start (http/create-server {::http/routes (routes topics topic-data)
                                   ::http/type   :jetty
                                   ::http/port   port})))

(defn- fully-consume [consumer-props topics]
  (rt/read-topics-loop consumer-props topics 1))

(defn start-api [consumer-props topics port]
  (println "Loading topic data")
  (let [topic-data (atom (fully-consume consumer-props topics))]
    (with-open [c (rt/background-consume consumer-props topics topic-data)]
      (println "Starting http server")
      (start-server topics topic-data port))))
