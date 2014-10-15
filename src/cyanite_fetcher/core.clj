(ns cyanite-fetcher.core
  (:require [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clj-time.local :as tl]
            [criterium.core :as cr]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.document :as esrd])
  (:gen-class))

;;------------------------------------------------------------------------------
;; Cassandra
;;------------------------------------------------------------------------------


;;------------------------------------------------------------------------------
;; ElasticSearch
;;------------------------------------------------------------------------------

(def ES_DEF_TYPE "path")
(def ES_TYPE_MAP {ES_DEF_TYPE {:_all { :enabled false }
                               :_source { :compress false }
                               :properties {:tenant {:type "string" :index "not_analyzed"}
                                            :path {:type "string" :index "not_analyzed"}}}})

(def ^:const period 46)
(def ^:const index "cyanite_paths")

(defn path-depth
  "Get the depth of a path, with depth + 1 if it ends in a period"
  [path]
  (loop [cnt 1
         from-dex 0]
    (let [dex (.indexOf path period from-dex)]
      (if (= dex -1)
        cnt
        (recur (inc cnt) (inc dex))))))

(defn build-es-filter
  "generate the filter portion of an es query"
  [path tenant leafs-only]
  (let [depth (path-depth path)
        p (str/replace (str/replace path "." "\\.") "*" ".*")
        f (vector
           {:range {:depth {:from depth :to depth}}}
           {:term {:tenant tenant}}
           {:regexp {:path p :_cache true}})]
    (if leafs-only (conj f {:term {:leaf true}}) f)))

(defn build-es-query
  "generate an ES query to return the proper result set"
  [path tenant leafs-only]
  {:filtered {:filter {:bool {:must (build-es-filter path tenant leafs-only)}}}})

(defn search
  "search for a path"
  [query scroll tenant path leafs-only]
  (let [res (query :query (build-es-query path tenant leafs-only)
                   :size 100
                   :search_type "query_then_fetch"
                   :scroll "1m")
        hits (scroll res)]
    (map #(:_source %) hits)))

(defn lookup [host tenant path]
  "Look up path."
  (let [full-path-cache (atom #{})
        conn (esr/connect host)
        scrollfn (partial esrd/scroll-seq conn)
        queryfn (partial esrd/search conn index ES_DEF_TYPE)]
    (map :path (search queryfn scrollfn tenant path true))))

(defn es-get-paths
  "Get paths from ElasticSearch."
  [host path tenant]
  (println "Getting paths form ElasticSearch...")
  (let [paths (time (doall (lookup host (or tenant "NONE") path)))]
    (println "Number of paths:" (count paths))
    paths))

;;------------------------------------------------------------------------------
;; Benchmark
;;------------------------------------------------------------------------------

(defn run-bench
  "Run benchmark."
  [chost eshost path tenant from to]
  (println "Start time:" (tl/format-local-time (tl/local-now) :rfc822))
  (println)
  (println "Cassandra host:    " chost)
  (println "ElasticSearch host:" eshost)
  (println)
  (println "Path:  " path)
  (println "Tenant:" tenant)
  (println "From:  " from)
  (println "To:    " to)
  (println)
  (es-get-paths eshost path tenant))

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
