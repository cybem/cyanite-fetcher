(ns cyanite-fetcher.core
  (:require [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clj-time.local :as tl]
            [criterium.core :as cr])
  (:gen-class))

;;------------------------------------------------------------------------------
;; Cassandra
;;------------------------------------------------------------------------------


;;------------------------------------------------------------------------------
;; ElasticSearch
;;------------------------------------------------------------------------------


;;------------------------------------------------------------------------------
;; Benchmark
;;------------------------------------------------------------------------------

(defn run-bench
  "Run benchmark."
  [chost eshost path tenant from to]
  (println "Start time:" (tl/format-local-time (tl/local-now) :rfc822))
  (println)
  (println "Cassandra host:\t\t" chost)
  (println "ElasticSearch host:\t" eshost)
  (println)
  (println "Path:\t" path)
  (println "Tenant:\t" tenant)
  (println "From:\t" from)
  (println "To:\t" to))

;;------------------------------------------------------------------------------
;; Command line
;;------------------------------------------------------------------------------

(def cli-options
  [["-h" "--help"]])

(defn usage
  "Construct usage message."
  [options-summary]
  (->> ["Cyanite data fetcher benchmark."
        ""
        "Usage: cyanite-fetcher <cassandra-host> <elasticsearch-host> <path> <tenant> <from> <to>"
        ""
        "Options:"
        options-summary]
       (str/join \newline)))

(defn error-msg
  "Combine error messages."
  [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn exit
  "Print message and exit with status."
  [status msg]
  (println msg)
  (System/exit status))

;;------------------------------------------------------------------------------
;; Main
;;------------------------------------------------------------------------------

(defn -main
  "Main function."
  [& args]
  (let [{:keys [options arguments errors summary]}
        (cli/parse-opts args cli-options)]
    ;; Handle help and error conditions
    (cond
     (:help options) (exit 0 (usage summary))
     (not= (count arguments) 6) (exit 1 (usage summary))
     errors (exit 1 (error-msg errors)))
    ;; Execute
    (let [[chost eshost path tenant from to] arguments]
      (run-bench chost eshost path tenant from to))))
