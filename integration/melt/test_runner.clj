(ns melt.test-runner
  (:require [midje.repl :refer [load-facts]]))

(defn -main
  "Return non-zero exit code if a test failure is found"
  []
  (let [results (load-facts 'melt.integration-test)]
    (if (< 0 (:failures results))
      (System/exit 1))))
