(ns melt.test-runner
  (:require [clojure.spec.alpha :as s]
            [midje.repl :refer [load-facts]]))

(defn -main
  "Return non-zero exit code if a test failure is found"
  []
  (s/check-asserts true)
  (let [results (load-facts)]
    (if (< 0 (:failures results))
      (System/exit 1))))
