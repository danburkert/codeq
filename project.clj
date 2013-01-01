(defproject datomic/codeq "0.1.0-SNAPSHOT"
  :description "codeq does a code-aware import of your git repo into a Datomic db"
  :url "http://datomic.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main datomic.codeq.core
  :plugins [[lein-tar "1.1.0"]]
  :dependencies [[org.clojure/clojure "1.5.0-alpha6"]
                 [com.datomic/datomic-free "0.8.3692"]
                 [commons-codec "1.7"]
                 [clj-time "0.4.4"]
                 [tentacles "0.2.3"]])
