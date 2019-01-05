(ns melt.util)

(defn mkdirs
  "Create the directories if they don't exist. Returns whether the
   directory exists after this call."
  [path]
  (if (not (.exists (java.io.File. path)))
    (.mkdirs path)
    true))
