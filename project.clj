(defproject shiva "0.1.0-SNAPSHOT"
  :description "jepsen test for shiva"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main jepsen.shiva.demo
  :dependencies [[org.clojure/clojure "1.10.0"]
		 [jepsen "0.1.14"]
		 [io.transwarp.shiva/shiva-client-shade "1.3"]]
  :repositories [["snapshots" "http://172.16.1.168:8081/artifactory/libs-snapshot-local"]
                 ["releases" "http://172.16.1.168:8081/artifactory/libs-release-local"]]
  :repl-options {:init-ns jepsen.shiva.demo})

(require 'cemerick.pomegranate.aether)
(cemerick.pomegranate.aether/register-wagon-factory! "http" #(org.apache.maven.wagon.providers.http.HttpWagon.))
