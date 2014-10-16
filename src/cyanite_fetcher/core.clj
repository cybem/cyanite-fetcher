(ns cyanite-fetcher.core
  (:require [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clj-time.local :as tl]
            [clojure.core.reducers :as r]
            [criterium.core :as cr]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.document :as esrd]
            [qbits.alia :as alia])
  (:gen-class))

;;------------------------------------------------------------------------------
;; Cassandra
;;------------------------------------------------------------------------------

(def ^:const keyspace "metric")

(defn fetchq
  "Yields a cassandra prepared statement of 7 arguments:

* `paths`: list of paths
* `tenant`: tenant identifier
* `rollup`: interval between points at this resolution
* `period`: rollup multiplier which determines the time to keep points for
* `min`: return points starting from this timestamp
* `max`: return points up to this timestamp
* `limit`: maximum number of points to return"
  [session]
  (alia/prepare
   session
   (str
    "SELECT path,data,time FROM metric WHERE "
    "path = ? AND tenant = ? AND rollup = ? AND period = ? "
    "AND time >= ? AND time <= ? ORDER BY time ASC;")))

(defn cleaner
  "Cleaner."
  [data f name]
  (println (format "Cleaning data with %s..." name))
  (let [cdata (time (doall (f data)))]
    (println "Number of rows:" (count cdata))
    (newline)
    cdata))

(defn flatter
  "Flatter."
  [data f name]
  (println (format "Flatting data with %s..." name))
  (let [fdata (time (doall (f data)))]
    (println "Number of rows:" (count fdata))
    (newline)
    fdata))

(defn par-fetch [session fetch! paths tenant rollup period from to]
  "Fetch data in parallel fashion."
  (let [futures
        (doall (map #(future
                       (->> (alia/execute
                             session fetch!
                             {:values [% tenant (int rollup)
                                       (int period)
                                       from to]
                              :fetch-size Integer/MAX_VALUE})
                            ;;(map detect-aggregate)
                            (seq)))
                    paths))]
    (map deref futures)))

(defn c-get-data
  "Get data from C*."
  [host paths tenant rollup period from to]
  (let [session (-> (alia/cluster {:contact-points [host]})
                    (alia/connect keyspace))
        fetch! (fetchq session)]
    (println "Getting data form Cassandra...")
    (let [data (time (doall (par-fetch session fetch! paths tenant rollup
                                       period from to)))]
      (newline)
      data)))

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
  (let [paths (time (doall (lookup host tenant path)))]
    (println "Number of paths:" (count paths))
    (newline)
    paths))

;;------------------------------------------------------------------------------
;; Rollups
;;------------------------------------------------------------------------------

(def ^:const rollups '("60s:5356800s"))

(defn to-seconds
  "Takes a string containing a duration like 13s, 4h etc. and
  converts it to seconds"
  [s]
  (let [[_ value unit] (re-matches #"^([0-9]+)([a-z])$" s)
        quantity (Integer/valueOf value)]
    (case unit
      "s" quantity
      "m" (* 60 quantity)
      "h" (* 60 60 quantity)
      "d" (* 24 60 60 quantity)
      "w" (* 7 24 60 60 quantity)
      "y" (* 365 24 60 60 quantity)
      (throw (ex-info (str "unknown rollup unit: " unit) {})))))

(defn convert-shorthand-rollup
  "Converts an individual rollup to a {:rollup :period :ttl} tri"
  [rollup]
  (if (string? rollup)
    (let [[rollup-string retention-string] (str/split rollup #":" 2)
          rollup-secs (to-seconds rollup-string)
          retention-secs (to-seconds retention-string)]
      {:rollup rollup-secs
       :period (/ retention-secs rollup-secs)
       :ttl    (* rollup-secs (/ retention-secs rollup-secs))})
    rollup))

(defn convert-shorthand-rollups
  "Where a rollup has been given in Carbon's shorthand form
   convert it to a {:rollup :period} pair"
  [rollups]
  (map convert-shorthand-rollup rollups))

(defn now
  "Returns a unix epoch"
  []
  (quot (System/currentTimeMillis) 1000))

(defn find-best-rollup
  "Find most precise storage period given the oldest point wanted"
  [from rollups]
  (let [within (fn [{:keys [rollup period] :as rollup-def}]
                 (and (>= (Long/parseLong from) (- (now) (* rollup period)))
                      rollup-def))]
    (some within (sort-by :rollup rollups))))

;;------------------------------------------------------------------------------
;; Benchmark
;;------------------------------------------------------------------------------

(defn run-bench
  "Run benchmark."
  [chost eshost path tenant from to]
  (println "Start time:" (tl/format-local-time (tl/local-now) :rfc822))
  (let [{:keys [rollup period]}
        (find-best-rollup (str from) (convert-shorthand-rollups rollups))
        tenant (or tenant "NONE")
        to (if to (Long/parseLong (str to)) (now))
        from (Long/parseLong (str from))]
    (newline)
    (println "Cassandra host:    " chost)
    (println "ElasticSearch host:" eshost)
    (newline)
    (println "Path:  " path)
    (println "Tenant:" tenant)
    (println "From:  " from)
    (println "To:    " to)
    (newline)
    (println "Rollup:" rollup)
    (println "Period:" period)
    (newline)
    (let [paths (es-get-paths eshost path tenant)
          data (c-get-data chost paths tenant rollup period from to)
          reduce-fdata (flatter data (fn [data] (reduce into data)) "reduce")
          rreduce-fdata (flatter data (fn [data] (r/reduce into [] data)) "r/reduce")
          flatten-fdata (flatter data (fn [data] (flatten data)) "flatten")
          rflatten-fdata (flatter data (fn [data] (into [] (r/flatten data))) "r/flatten")
          remove-cdata (cleaner reduce-fdata (fn [data] (remove nil? data))  "remove")
          rremove-cdata (cleaner reduce-fdata (fn [data] (r/remove nil? data)) "r/remove")]))
  (println "Finish time:" (tl/format-local-time (tl/local-now) :rfc822)))

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
      (run-bench chost eshost path tenant from to)))
  (System/exit 0))
