(ns datomic.codeq.repositories.local
  (:require [datomic.codeq.repository :refer :all]
            [clojure.java.shell :as sh]
            [clojure.java.io :as io]
            [clojure.string :as s])
  (:import java.util.Date))

(defn- words [^String s] (filterv #(not= % "") (s/split s #"\s+")))

(defn- ^java.io.Reader git-cmd
  "Wrapper for shelling out to the git cli.  Sends commands to git repository
   and returns response."
  [repo & cmds]
  (-> (.exec (Runtime/getRuntime)
             (into-array (conj cmds "git"))
             nil
             (io/as-file (:path repo)))
      (.getInputStream)
      io/reader))

(defn- url->uri
  "Takes a remote git protocol url and returns the uri and protocol.
  Works with HTTP, HTTPS, SSH and git protocols."
  [url]
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

(defrecord Local [path]

  Repository

  (commits [r id]
    (with-open [cs (git-cmd r "log" "--date-order" "--reverse" "--pretty=format:%H" id)]
      (doall (line-seq cs))))

  (commit [r sha]
    (let [p-date  (fn [s] (Date. (* 1000 (Integer/parseInt s))))]
      (when-let [[csha tsha pshas aname aemail adate cname cemail cdate & message]
                 (with-open [fs (git-cmd r "show" "--quiet"  "--pretty=format:%H%n%T%n%P%n%an%n%ae%n%at%n%cn%n%ce%n%ct%n%B" sha)]
                   (doall (line-seq fs)))]
        {:sha csha
         :author    {:name aname :email aemail :date (p-date adate)}
         :committer {:name cname :email cemail :date (p-date cdate)}
         :parents (words pshas)
         :tree tsha
         :message  (s/join "\n" message)})))

  (branches [r]
    ;; Sample output of `git branch -v --no-abrev` of repo in detached HEAD state:
    ;;* (no branch) eeda182df874125ff7cddf0b1e78d36c4383d22d add body parameter to make-request
    ;;  body        02702e84cca8830bf44f834a225efc851d7ca53f intermediate
    ;;  master      05876e556c78d63da944164cdafe54c0ad31c692 [ahead 2] Merge pull request #18 from danburkert/raw-blob
    ;;  oauth2      19433cb2d36cabea3924f41e4cba853568f4dcae fix oauth2 instructions in README
    ;;  raw-blob    993fd1982690e549981f4548e03f6985d27f2959 Add support for raw blob responses
    (let [destar (fn [ws] (if (= (first ws) "*") (rest ws) ws))
          branch? (fn [ws] (not (and (= (first ws) "(no")
                                     (= (second ws) "branch)"))))]
      (with-open [bs (git-cmd r "branch" "-v" "--no-abbrev")]
        (->> (line-seq bs)
             (map (comp destar words))
             (filter branch?)
             (map (partial zipmap [:label :commit]))
             doall))))

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
    (let [ts (with-open [ts (git-cmd r "show-ref" "--tags")]
               (->> ts
                    (line-seq)
                    (mapv (comp (juxt first (comp last #(s/split % #"\/") last))
                                words))))
          format (fn [[sha label]]
                   (if (= (with-open [t (git-cmd r "cat-file" "-t" sha)] (slurp t))
                          "tag\n")
                     (let [[_ tagger & msg] (with-open [t (git-cmd r "show" "--quiet" "--pretty=%H" sha)]
                                              (doall (line-seq t)))
                           t-words (words tagger)
                           t-name (s/join " " (rest (butlast t-words)))
                           t-email (subs (last t-words) 1 (dec (count (last t-words))))
                           commit (last msg)
                           msg (s/join "\n" (rest (butlast msg)))]
                       {:label label :commit commit :sha sha
                        :tagger {:name t-name :email t-email}
                        :message msg :annotated true})
                     {:label label :commit sha :annotated false}))]
      (map format ts)))

  (tag [r label]
    (first (filter #(= label (:label %)) (tags r))))

  (blob [r sha]
    (with-open [b (git-cmd r "cat-file" "-p" sha)]
      (slurp b)))

  (tree [r sha]
    (with-open [fs (git-cmd r "cat-file" "-p" sha)]
      (mapv (comp (fn [[mode type sha name]]
                    {:mode (keyword mode)
                     :type (keyword type)
                     :sha sha
                     :name name})
                  words)
            (line-seq fs))))

  (remotes [r]
    (with-open [rs (git-cmd r "remote" "-v")]
      (->> (line-seq rs)
           (map (comp (partial take 2) words))
           distinct
           (mapv (fn [[label url]]
                   (assoc (url->uri url) :label label))))))

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
  "Returns the Local repository on the path.  Returns nil if the path does
   not point to a git repository."
  [path]
  (let [repo (->Local path)]
    (try
      (with-open [_ (git-cmd repo "status")]
        repo)
      (catch java.lang.Throwable e nil))))
