(ns cyanite-fetcher.core
  (:require [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clj-time.local :as tl]
            [clojure.core.reducers :as r]
            [clojure.core.async :as async]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.document :as esrd]
            [qbits.alia :as alia])
  (:gen-class))

;;------------------------------------------------------------------------------
;; Utils
;;------------------------------------------------------------------------------

(defn print-nitems
  "Print number of items."
  [data]
  (let [nitems (count data)]
    (println "Number of items:" nitems)
    (when (<= nitems 20)
      (println "Data:" data))))

;;------------------------------------------------------------------------------
;; Aggregation
;;------------------------------------------------------------------------------

(defmulti aggregate-with
  "This transforms a raw list of points according to the provided aggregation
   method. Each point is stored as a list of data points, so multiple
   methods make sense (max, min, mean). Additionally, a raw method is
   provided"
  (comp first list))

(defmethod aggregate-with :mean
  [_ {:keys [data] :as metric}]
  (if (seq data)
    (-> metric
        (dissoc :data)
        (assoc :metric (/ (reduce + 0.0 data) (count data))))
    metric))

(defmethod aggregate-with :sum
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric (reduce + 0.0 data))))

(defmethod aggregate-with :max
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric (apply max data))))

(defmethod aggregate-with :min
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric (apply min data))))

(defmethod aggregate-with :raw
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric data)))

(defmethod aggregate-with :avg
           [_ {:keys [data] :as metric}]
  (if (seq data)
    (-> metric
        (dissoc :data)
        (assoc :metric (/ (reduce + 0.0 data) (count data))))
    metric))

;;
;; if no method given parse metric name and select aggregation function
;;
(defn detect-aggregate
  [{:keys [path] :as metric}]
  (if-let [[_ m] (re-find #"^(sum|avg|mean|min|max|raw)\..*" path)]
    (aggregate-with (keyword m) metric)
    (aggregate-with :mean metric)))

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

(defn my-deref
  [f]
  (let [result (deref f 300000 :timeout)]
    (when (= result :timeout)
      (throw (ex-info "Too long!" {})))
    result))

(defn par-fetch
  "Fetch data in parallel fashion."
  [session fetch! paths tenant rollup period from to]
  (let [futures
        (doall (map #(future
                       (try
                         (->> (alia/execute
                               session fetch!
                               {:values [% tenant (int rollup)
                                         (int period)
                                         from to]
                                :fetch-size Integer/MAX_VALUE})
                              (map detect-aggregate)
                              (doall)
                              (seq))
                         (catch Exception e
                           (shutdown-agents)
                           (throw e))))
                    paths))]
    (map my-deref futures)))

(defn par-fetch-pmap
  "Fetch data in parallel fashion using pmap."
  [session fetch! paths tenant rollup period from to]
  (pmap (fn [path] (->> (alia/execute
                         session fetch!
                         {:values [path tenant (int rollup)
                                   (int period)
                                   from to]
                          :fetch-size Integer/MAX_VALUE})
                        (map detect-aggregate)
                        (doall)
                        (seq))) paths))

(defn par-fetch-async
  "Fetch data in parallel fashion using execute-async."
  [session fetch! paths tenant rollup period from to]
  (let [data (atom [])
        channels
        (doall (map #(alia/execute-async session fetch!
                                         {:values [% tenant (int rollup)
                                                   (int period)
                                                   from to]
                                          :fetch-size Integer/MAX_VALUE
                                          :success (fn [rows]
                                                     (->> rows
                                                          (map detect-aggregate)
                                                          (doall)
                                                          (seq)
                                                          (swap! data conj)))})
                    paths))]
    (map my-deref channels)
    @data))

(defn par-fetch-chan
  "Fetch data in parallel fashion using execute-chan."
  [session fetch! paths tenant rollup period from to]
  (async/<!!
   (async/go
     (loop [queries
            (map #(alia/execute-chan session fetch!
                                     {:values [% tenant (int rollup)
                                               (int period)
                                               from to]
                                      :fetch-size Integer/MAX_VALUE})
                 paths)
            query-results []]
       (if (empty? queries)
         query-results
         (let [[result channel] (async/alts! queries)]
           (recur
            (remove #{channel} queries)
            (->> result
                 (map detect-aggregate)
                 (doall)
                 (seq)
                 (into query-results)))))))))

(defn c-get-data
  "Get data from C*."
  [f-par-fetch n-par-fetch host paths tenant rollup period from to]
  (println "Connecting to Cassandra...")
  (let [cluster (alia/cluster {:contact-points [host]
                               :pooling-options {:max-connections-per-host {:local 8192
                                                                            :remote 8192}}})
        session (alia/connect cluster keyspace)
        fetch! (fetchq session)]
    (println (format "Getting data form Cassandra using %s..." n-par-fetch))
    (try
      (let [data (time (doall (f-par-fetch session fetch! paths tenant
                                           rollup period from to)))]
        (newline)
        data)
      (finally
        (alia/shutdown cluster)))))

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
;; Data algorithms
;;------------------------------------------------------------------------------

(defn cleaner
  "Cleaner."
  [data f name]
  (println (format "Cleaning data with %s..." name))
  (let [cdata (time (doall (f data)))]
    (print-nitems cdata)
    (newline)
    cdata))

(defn flatter
  "Flatter."
  [data f name]
  (println (format "Flatting data with %s..." name))
  (let [fdata (time (doall (f data)))]
    (print-nitems fdata)
    (newline)
    fdata))

(defn flatter-cleaner
  "Flatter and cleaner."
  [data]
  (println "Flatting and cleaning data...")
  (let [fcdata (time (doall (remove nil? (reduce into data))))]
    (print-nitems fcdata)
    (newline)
    fcdata))

(defn r-flatter-cleaner
  "Flatter and cleaner based on reducers."
  [data]
  (println "Flatting and cleaning data with reducers...")
  (let [fcdata (time (doall (into [] (r/remove nil? (r/reduce into [] data)))))]
    (print-nitems fcdata)
    (newline)
    fcdata))

;;------------------------------------------------------------------------------
;; Data normalization
;;------------------------------------------------------------------------------

(defn get-fill-in [f-map]
  (defn fill-in
  "Fill in fetched data with nil metrics for a given time range"
  [nils [path data]]
  (hash-map path
            (->> (group-by :time data)
                 (merge nils)
                 (f-map (comp first val))
                 (sort-by :time)
                 (f-map :metric)))))

(defn norm
  "Normalization."
  [data rollup to f-map f-fill-in name]
  (println (format "Running normalization (%s)..." name))
  (let [ndata
        (time (doall (let [min-point  (:time (first data))
                           max-point  (-> to (quot rollup) (* rollup))
                           nil-points (->> (range min-point (inc max-point) rollup)
                                           (f-map (fn [time] {time [{:time time}]}))
                                           (reduce merge {}))
                           by-path    (->> (group-by :path data)
                                           (f-map (partial f-fill-in nil-points))
                                           (reduce merge {}))]
                       {:from min-point
                        :to   max-point
                        :step rollup
                        :series by-path})))]
    (print-nitems (:series ndata))
    (newline)
    ndata))


(defn new-norm
  "New normalization."
  [paths data rollup to name]
  (println (format "Running new normalization (%s)..." name))
  (let [ndata
        (time (doall (let [min-point  (:time (first data))
                           max-point  (-> to (quot rollup) (* rollup))
                           points (range min-point (inc max-point) rollup)
                           dmap (reduce (fn [m e]
                                          (let [path (hash (:path e))
                                                time (:time e)
                                                metric (:metric e)]
                                            (assoc m path (assoc (get m path {}) time metric))))
                                        {} data)
                           metrics (pmap (fn [path]
                                           (let [h-path (hash path)
                                                 serie (get dmap h-path)]
                                             (map #(get serie % nil) points)))
                                         paths)
                           series (zipmap paths metrics)]
                       {:from min-point
                        :to   max-point
                        :step rollup
                        :series series})))]
    (print-nitems (:series ndata))
    (newline)))


;;------------------------------------------------------------------------------
;; Benchmark
;;------------------------------------------------------------------------------

(defn bench-itself
  "Benchmark itself."
  [chost eshost path tenant from to]
  (let [{:keys [rollup period]}
        (find-best-rollup (str from) (convert-shorthand-rollups rollups))
        tenant (or tenant "NONE")
        to (if to (Long/parseLong (str to)) (now))
        from (Long/parseLong (str from))]
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
          data (c-get-data par-fetch "par-fetch" chost paths tenant rollup period from to)
          ;;data (c-get-data par-fetch-chan "par-fetch-chan" chost paths tenant rollup period from to)
          ;;fdata (flatter data (fn [data] (r/reduce into [] data)) "r/reduce")
          ]
      ;;(flatter data (fn [data] (reduce into data)) "reduce")
      ;;(cleaner fdata (fn [data] (into [] (r/remove nil? data))) "r/remove")
      ;;(cleaner fdata (fn [data] (remove nil? data))  "remove")
      ;;(r-flatter-cleaner data)
      (let [fcdata (r-flatter-cleaner data)]
        ;;(norm fcdata rollup to map (get-fill-in map) "map/map")
        (norm fcdata rollup to pmap (get-fill-in map) "pmap/map")
        ;;(norm fcdata rollup to map (get-fill-in pmap) "map/pmap")
        ;;(norm fcdata rollup to pmap (get-fill-in pmap) "pmap/pmap")
        (new-norm paths fcdata rollup to "primal")
        ))))

(defn run-bench
  "Run benchmark."
  [chost eshost path tenant from to]
  (println "Start time:" (tl/format-local-time (tl/local-now) :rfc822))
  (newline)
  (try
    (time (bench-itself chost eshost path tenant from to))
    (catch Exception e
      (shutdown-agents)
      (throw e)))
  (newline)
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
