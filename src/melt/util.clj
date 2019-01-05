(ns melt.util)

(defn mkdirs
  "Create the directories if they don't exist. Returns whether the
   directory exists after this call."
  [path]
  (let [file (java.io.File. path)]
    (if (not (.exists file))
      (.mkdirs file)
      true)))
