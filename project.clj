(defproject kbs "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [compojure "1.5.1"]
                 [ring/ring-defaults "0.3.2"]
                 [ring/ring-json "0.4.0"]
                 [korma "0.4.3"]
                 [org.clojure/java.jdbc "0.7.3"]
                 [com.taoensso/timbre "4.10.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.postgresql/postgresql "42.2.2"]
                 [clj-http "3.7.0"]
                 [clj-time "0.14.0"]
                 [image-resizer "0.1.10"]
                 [org.apache.pdfbox/pdfbox "1.8.2"]
                 [ring-cors "0.1.13"]]
  :plugins [[lein-ring "0.9.7"]]
  :ring {:handler kbs.handler/app}
  :profiles
  {:dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                        [ring/ring-mock "0.3.2"]]}})
