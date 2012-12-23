(ns datomic.codeq.repository
  (:require [clojure.java.shell :as sh]
            [clojure.string :as s])
  (:import java.util.Date))

(defprotocol Repository
  "An interface for git repository types"
  (commits [r] [r sha])
  (commit [r sha])
  (branches [r])
  (branch [r label])
  (tags [r])
  (tag [r label])
  (blob [r sha])
  (tree [r sha])
  (remotes [r])
  (remote [r label]))

(defn- words [^String s] (filterv #(not= % "") (s/split s #"\s+")))

(defn- git-cmd [repo cmd]
  "wrapper for shelling out to the git cli interface.  Sends command to
   git repository, checks for a non-error response, and returns response."
  (sh/with-sh-dir (:path repo)
                  (let [args (conj (seq (s/split cmd #"\s")) "git")
                        rtn (apply sh/sh args)]
                    (assert (= (rtn :exit) 0) "Non-zero return from git")
                    (rtn :out))))

(defn- url->uri [url]
  {:pre [(re-find #"\.git$" url)]}
  "Takes a remote git protocol url and returns the uri and protocol.
  Works with HTTP, HTTPS, SSH and git protocols."
  (let [cut #(s/replace %1 %2 "")
        url' (cut url #"\.git$")]
    (cond
      (re-find #"^https" url') {:uri (cut url' #"^https://") :protocol :https}
      (re-find #"^http"  url') {:uri (cut url' #"^http://")  :protocol :http}
      (re-find #"@" url') {:uri (-> url' (cut #"^.*@") (s/replace #":" "/"))
                            :protocol :ssh}
      (re-find #"^git://" url') {:uri (cut url' #"^git://") :protocol :git}
      :else (throw (IllegalArgumentException.
                     (str "invalid remote repository format: " url))))))

(defrecord Local
  [path]
  Repository
  (commits [r] (commits r ""))
  (commits [r sha]
    (try
      (s/split-lines
        (git-cmd r (str "log --date-order --reverse --pretty=format:%H " sha)))
      (catch java.lang.AssertionError e nil)))
  (commit [r sha]
    (try
      (do
        (assert (= (git-cmd r (str "cat-file -t " sha)) "commit\n"))
        (let
          [p-date  (fn [s] (Date. (* 1000 (Integer/parseInt s))))
           cmd (str "show --quiet "
                    "--pretty=format:%H%n%T%n%P%n%an%n%ae%n%at%n%cn%n%ce%n%ct%n%B "
                    sha)
           [csha tsha pshas aname aemail adate cname cemail cdate & message]
           (s/split-lines (git-cmd r cmd))]
          {:sha csha
           :author    {:name aname :email aemail :date (p-date adate)}
           :committer {:name cname :email cemail :date (p-date cdate)}
           :parents (words pshas)
           :tree tsha
           :message  (apply str (interpose "\n" message))}
          ))
      (catch java.lang.AssertionError e nil)))
  (branches [r]
    (let [bs (-> (git-cmd r "branch -v --no-abbrev")
               (s/replace #"\*" "")
               (s/split-lines))]
      (mapv #(zipmap [:label :commit] (words %)) bs)))
  (branch [r label]
    (first (filter #(= label (:label %)) (branches r))))
  (tags [r]
    (try
      (do
        (let [ts (-> (git-cmd r "show-ref --tags")
                   (s/replace #"refs/tags/" "")
                   (s/split-lines))]
          (mapv #(zipmap [:commit :label] (words %)) ts)))
      (catch java.lang.AssertionError e nil)))
  (tag [r label]
    (first (filter #(= label (:label %)) (tags r))))
  (blob [r sha]
    (try
      (do
        (assert (= (git-cmd r (str "cat-file -t " sha)) "blob\n"))
        (git-cmd r (str "cat-file -p " sha)))
      (catch java.lang.AssertionError e nil)))
  (tree [r sha]
    (try
      (do
        (assert (= (git-cmd r (str "cat-file -t " sha)) "tree\n"))
        (let
          [fs (s/split-lines (git-cmd r (str "cat-file -p " sha)))]
          (mapv #(-> (zipmap [:mode :type :sha :path] (words %))
                   (update-in [:type] keyword)
                   (update-in [:mode] keyword)) fs)))
      (catch java.lang.AssertionError e nil)))
  (remotes [r]
    (let [lines (s/split-lines (git-cmd r "remote -v"))
          remotes (distinct (mapv #(take 2 (words %)) lines))]
      (mapv #(assoc (url->uri (second %)) :label (first %)) remotes)))
  (remote [r label]
    (first (filter #(= label (:label %)) (remotes r)))))

(defn info [repo]
  "Return a map of information about a repository"
  (let [remotes (remotes repo)
        origin (first (filter (fn [remote] (= (:label remote) "origin")) remotes))
        ^String uri (:uri origin)
        name (subs uri (inc (.lastIndexOf uri "/")))]
    {:uri uri :name name}))
