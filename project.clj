(defproject cyanite-fetcher "0.1.0"
  :description "Cyanite data fetcher benchmark"
  :url "https://github.com/cybem/cyanite-fetcher"
  :license {:name "MIT"
            :url "https://github.com/cybem/cyanite-fetcher/LICENSE"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [clj-time "0.9.0-beta1"]
                 [criterium "0.4.3"]
                 [clojurewerkz/elastisch "2.1.0-beta6"]]
  :main ^:skip-aot cyanite-fetcher.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
