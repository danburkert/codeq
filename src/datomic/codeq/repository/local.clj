(ns datomic.codeq.repository.local
  (:require [datomic.codeq.repository :refer :all]
            [clojure.java.shell :as sh]
            [clojure.string :as s])
  (:import java.util.Date))

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
  (commits [r]
    (let [refs (concat (branches r) (tags r))]
      commits (distinct
                (mapcat (comp (partial commits r) :label) refs))))

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
    ;; Sample output of `git branch -v --no-abrev` of repo in detached HEAD state:
    ;;* (no branch) eeda182df874125ff7cddf0b1e78d36c4383d22d add body parameter to make-request
    ;;  body        02702e84cca8830bf44f834a225efc851d7ca53f intermediate
    ;;  master      05876e556c78d63da944164cdafe54c0ad31c692 [ahead 2] Merge pull request #18 from danburkert/raw-blob
    ;;  oauth2      19433cb2d36cabea3924f41e4cba853568f4dcae fix oauth2 instructions in README
    ;;  raw-blob    993fd1982690e549981f4548e03f6985d27f2959 Add support for raw blob responses
    (let [lines (-> (git-cmd r "branch -v --no-abbrev")
                    (s/replace #"\*" "")
                    s/split-lines)
          branch? (fn [ws] (not (and (= (first ws) "(no") (= (second ws) "branch)"))))]
      (->> lines
           (map words)
           (filter branch?)
           (map (partial zipmap [:label :commit])))))

  (branch [r label]
    (first (filter #(= label (:label %)) (branches r))))

  (tags [r]
    ;; TODO: annotated tags should have a :date entry under the :tagger entry map

    ;; Sample output from `git show-ref --tags`
    ;; a2a50394b87c2df98c94b4cac986fb0274898d23 refs/tags/v1.0
    ;; 7b08a45c5f64c7b0174e5c927b7476a349ffd118 refs/tags/v1.1
    ;; ...

    ;; Sample output from `git show --quiet --pretty=%H 7b08a45c5f64c7b0174e5c927b7476a349ffd118`:
    ;; tag v1.1
    ;; Tagger: Dan Burkert <danburkert@gmail.com>
    ;;
    ;; This is an annoted tag message
    ;; 0a43ee751dd2cca105490129627bee6d73f367bc
    (try
      (let [ts (-> (git-cmd r "show-ref --tags")
                   (s/replace #"refs/tags/" "")
                   (s/split-lines))
            format (fn [[sha label]]
                     (if (= (git-cmd r (str "cat-file -t " sha)) "tag\n")
                       (let [[_ tagger & msg] (s/split-lines
                                                (git-cmd r (str "show --quiet --pretty=%H " sha)))
                             t-words (words tagger)
                             t-name (apply str (interpose " " (rest (butlast t-words))))
                             t-email (subs (last t-words) 1 (dec (count (last t-words))))
                             commit (last msg)
                             msg (apply str (interpose "\n" (rest (butlast msg))))]
                         {:label label :commit commit :sha sha
                          :tagger {:name t-name :email t-email}
                          :message msg :annotated true})
                       {:label label :commit sha :annotated false}))]
        (map (comp format words) ts))
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
          (map #(-> (zipmap [:mode :type :sha :name] (words %))
                   (update-in [:type] keyword)
                   (update-in [:mode] keyword)) fs)))
      (catch java.lang.AssertionError e nil)))

  (remotes [r]
    (let [lines (s/split-lines (git-cmd r "remote -v"))
          remotes (distinct (map #(take 2 (words %)) lines))]
      (map #(assoc (url->uri (second %)) :label (first %)) remotes)))

  (remote [r label]
    (first (filter #(= label (:label %)) (remotes r))))

  (info [r]
      (let [remotes (remotes r)
            origin (first (filter (fn [remote]
                                    (= (:label remote) "origin")) remotes))
            ^String uri (:uri origin)
            name (subs uri (inc (.lastIndexOf uri "/")))]
        {:uri uri :name name})))

(defn local-repo
  [path]
  "Returns the Local repository on the path.  Returns nil if the path does
   not point to a git repository."
  (let [repo (->Local path)]
    (try
      (do
        (git-cmd repo "status")
        repo)
      (catch java.lang.Throwable e nil))))
