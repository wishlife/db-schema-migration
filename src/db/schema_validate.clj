(ns db.schema-validate
  (:import (java.io File)
           (java.net URL)
           (java.text SimpleDateFormat)
           (java.util Date))
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:gen-class))

(defn- exec
  ([cmd-array]
   (exec cmd-array nil))
  ([cmd-array input]
   (let [^Process proc (.exec (Runtime/getRuntime) (into-array String cmd-array))]
     (if input
       (send (agent nil) (fn [_] (with-open [os (.getOutputStream proc)]
                                   (spit os input))))
       (.. proc getOutputStream close))
     (let [out (send (agent nil) #(slurp %2) (.getInputStream proc))
           err (send (agent nil) #(slurp %2) (.getErrorStream proc))
           exit (.waitFor proc)]
       (await out err)
       {:exit exit :out @out :err @err}))))

(defn- cleanup-dump
  [dump]
  ;; the follow could probably be made more efficient by using fewer
  ;; string/replaces, but this is hardly the slowest thin in the
  ;; process.
  (-> dump
      (str/replace #"(?m)^--.*$" "") ;; remove comments
      (str/replace #"(?m)^ALTER TABLE.*OWNER TO.*;$" "") ;; remove table ownership
      (str/replace #"(?m)^(?:REVOKE|GRANT)\s.*;$" "") ;; remove grants/revokes
      (str/replace #"(?<=^|\r\n?|\n)(?:\r\n?|\n)+" ""))) ;; remove empty lines

(defn compare-dump
  [expected actual]
  (let [diff (exec ["diff" (.getFile expected) "-"] actual)]
    (case (:exit diff)
      0 (println "Schema is valid")
      1 (let [actual-file (.format (SimpleDateFormat. "'schema-validate.'YYYY-MM-dd'T'HHmmss'.sql'") (Date.))]
          (println "Schema differences found!")
          (println (:out diff))
          (println "Writing current schema to" actual-file)
          (println "If the above differences are correct, please copy this file over schema-validate.sql")
          (if (.exists (io/resource actual-file))
            (printf "WARNING %s exists! Not overwriting.%n" actual-file)
            (spit actual-file actual)))
      (printf "DIFF ERROR! (code = %d)%n%s%n" (:exit diff) (or (:err diff)
                                                               (:out diff))))
    (:exit diff)))

(defn export-schema
  [database-name]
  {:pre [(re-matches #"^\w+$" database-name)]}
  (let [dump (exec ["pg_dump" "--schema-only" database-name])]
    (if-not (= 0 (:exit dump))
      (throw (ex-info (format "export failed: %s%n" (clojure.string/trim (:err dump))) {:database database-name}))
      (cleanup-dump (:out dump)))))

(defn validate-schema
  [database-name]
  (compare-dump (io/resource "schema-validate.sql")
                (export-schema database-name)))
