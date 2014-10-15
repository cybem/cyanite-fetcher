(defproject cyanite-fetcher "0.1.0"
  :description "Cyanite data fetcher benchmark"
  :url "https://github.com/cybem/cyanite-fetcher"
  :license {:name "MIT"
            :url "https://github.com/cybem/cyanite-fetcher/LICENSE"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :main ^:skip-aot cyanite-fetcher.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
