;;   Copyright (c) Metadata Partners, LLC. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns datomic.codeq.core
  (:require [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.set]
            [clojure.string :as string]
            [datomic.codeq.repository :as repo]
            [datomic.codeq.util :refer [index-get-id cond-> index->id-fn tempid?]]
            [datomic.codeq.analyzer :as az]
            [datomic.codeq.analyzers.clj])
  (:import java.util.Date)
  (:gen-class))

(set! *warn-on-reflection* true)

(def schema
     [
      ;;tx attrs
      {:db/id #db/id[:db.part/db]
       :db/ident :tx/commit
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "Associate tx with this git commit"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :tx/file
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "Associate tx with this git blob"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :tx/analyzer
       :db/valueType :db.type/keyword
       :db/cardinality :db.cardinality/one
       :db/index true
       :db/doc "Associate tx with this analyzer"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :tx/analyzerRev
       :db/valueType :db.type/long
       :db/cardinality :db.cardinality/one
       :db/doc "Associate tx with this analyzer revision"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :tx/op
       :db/valueType :db.type/keyword
       :db/index true
       :db/cardinality :db.cardinality/one
       :db/doc "Associate tx with this operation - one of :import, :analyze"
       :db.install/_attribute :db.part/db}

      ;;git stuff
      {:db/id #db/id[:db.part/db]
       :db/ident :git/type
       :db/valueType :db.type/keyword
       :db/cardinality :db.cardinality/one
       :db/index true
       :db/doc "Type enum for git objects - one of :commit, :tree, :blob, :tag, :branch"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :git/sha
       :db/valueType :db.type/string
       :db/cardinality :db.cardinality/one
       :db/doc "A git sha, should be in repo"
       :db/unique :db.unique/identity
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :repo/commits
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/many
       :db/doc "Associate repo with these git commits"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :repo/uri
       :db/valueType :db.type/string
       :db/cardinality :db.cardinality/one
       :db/doc "A git repo uri"
       :db/unique :db.unique/identity
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :commit/parents
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/many
       :db/doc "Parents of a commit"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :commit/tree
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "Root node of a commit"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :commit/message
       :db/valueType :db.type/string
       :db/cardinality :db.cardinality/one
       :db/doc "A commit message"
       :db/fulltext true
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :commit/author
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "Person who authored a commit"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :commit/authoredAt
       :db/valueType :db.type/instant
       :db/cardinality :db.cardinality/one
       :db/doc "Timestamp of authorship of commit"
       :db/index true
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :commit/committer
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "Person who committed a commit"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :commit/committedAt
       :db/valueType :db.type/instant
       :db/cardinality :db.cardinality/one
       :db/doc "Timestamp of commit"
       :db/index true
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :tree/nodes
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/many
       :db/doc "Nodes of a git tree"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :node/filename
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "filename of a tree node"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :node/paths
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/many
       :db/doc "paths of a tree node"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :node/object
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "Git object (tree/blob) in a tree node"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :git/prior
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "Node containing prior value of a git object"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :email/address
       :db/valueType :db.type/string
       :db/cardinality :db.cardinality/one
       :db/doc "An email address"
       :db/unique :db.unique/identity
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :file/name
       :db/valueType :db.type/string
       :db/cardinality :db.cardinality/one
       :db/doc "A filename"
       :db/fulltext true
       :db/unique :db.unique/identity
       :db.install/_attribute :db.part/db}

      ;;codeq stuff
      {:db/id #db/id[:db.part/db]
       :db/ident :codeq/file
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "Git file containing codeq"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :codeq/loc
       :db/valueType :db.type/string
       :db/cardinality :db.cardinality/one
       :db/doc "Location of codeq in file. A location string in format \"line col endline endcol\", one-based"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :codeq/parent
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "Parent (containing) codeq of codeq (if one)"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :codeq/code
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "Code entity of codeq"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :code/sha
       :db/valueType :db.type/string
       :db/cardinality :db.cardinality/one
       :db/doc "SHA of whitespace-minified code segment text: consecutive ws becomes a single space, then trim. ws-sensitive langs don't minify."
       :db/unique :db.unique/identity
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :code/text
       :db/valueType :db.type/string
       :db/cardinality :db.cardinality/one
       :db/doc "The source code for a code segment"
       ;;:db/fulltext true
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :code/name
       :db/valueType :db.type/string
       :db/cardinality :db.cardinality/one
       :db/doc "A globally-namespaced programming language identifier"
       :db/fulltext true
       :db/unique :db.unique/identity
       :db.install/_attribute :db.part/db}
      ])

(defn ensure-schema [conn]
  (or (-> conn d/db (d/entid :tx/commit))
      @(d/transact conn schema)))

(defn ensure-db [db-uri]
  (let [newdb? (d/create-database db-uri)
        conn (d/connect db-uri)]
    (ensure-schema conn)
    conn))

(def repo-id
  (memoize
    (fn [db repo]
      (let [uri (:uri (repo/info repo))]
        (or (ffirst (d/q '[:find ?e
                           :in $ ?uri
                           :where [?e :repo/uri ?uri]]
                         db uri))
            (d/tempid :db.part/user))))))

(defn commit-tx-data
  "Create transaction data for a commit import.

   TODO: determine whether this function can/should be split up, perhaps
   splitting out node/tree/blob creation into a seperate node-tx-data fuction."
  [db repo {sha :sha message :message tree :tree parents :parents
            {author :email authored :date} :author
            {committer :email committed :date} :committer}]
  (let [repo-id (repo-id db repo)
        repo-name (:name (repo/info repo))
        sha->id (index->id-fn db :git/sha)
        email->id (index->id-fn db :email/address)
        filename->id (index->id-fn db :file/name)
        author-id (email->id author)
        committer-id (email->id committer)
        commit-id (d/tempid :db.part/user)
        tx-data (fn f [inpath {:keys [sha type filename]}] ;; recursively descend through trees & blobs and create transaction data
                  (let [path (str inpath filename)
                        object-id (sha->id sha)
                        filename-id (filename->id filename)
                        path-id (filename->id path)
                        node-id (or (and (not (tempid? object-id))
                                         (not (tempid? filename-id))
                                         (ffirst (d/q '[:find ?e :in $ ?filename ?id
                                                        :where [?e :node/filename ?filename] [?e :node/object ?id]]
                                                      db filename-id object-id)))
                                    (d/tempid :db.part/user))
                        newpath (or (tempid? path-id) (tempid? node-id)
                                    (not (ffirst (d/q '[:find ?node :in $ ?path
                                                        :where [?node :node/paths ?path]]
                                                      db path-id))))
                        data (cond-> []
                                     (tempid? filename-id) (conj [:db/add filename-id :file/name filename])
                                     (tempid? path-id) (conj [:db/add path-id :file/name path])
                                     (tempid? node-id) (conj {:db/id node-id :node/filename filename-id :node/object object-id})
                                     newpath (conj [:db/add node-id :node/paths path-id])
                                     (tempid? object-id) (conj {:db/id object-id :git/sha sha :git/type type}))
                        data (if (and newpath (= type :tree))
                               (let [children (repo/tree repo sha)]
                                 (reduce (fn [data child]
                                           (let [[commit-id cdata] (f (str path "/") child)
                                                 data (into data cdata)]
                                             (cond-> data
                                                     (tempid? object-id) (conj [:db/add object-id :tree/nodes commit-id]))))
                                         data children))
                               data)]
                    [node-id data]))
        [treeid treedata] (tx-data nil {:sha tree :type :tree :filename repo-name})
        tx (into treedata
                 [[:db/add repo-id :repo/commits commit-id]
                  {:db/id (d/tempid :db.part/tx)
                   :tx/commit commit-id
                   :tx/op :import}
                  (cond-> {:db/id commit-id
                           :git/type :commit
                           :commit/tree treeid
                           :git/sha sha
                           :commit/author author-id
                           :commit/authoredAt authored
                           :commit/committer committer-id
                           :commit/committedAt committed
                           }
                          message (assoc :commit/message message)
                          parents (assoc :commit/parents
                                         (mapv (fn [p]
                                                 (let [id (sha->id p)]
                                                   (assert (not (tempid? id))
                                                           (str "Parent " p " not previously imported"))
                                                   id))
                                               parents)))])
        tx (cond-> tx
                   (tempid? author-id)
                   (conj [:db/add author-id :email/address author])
                   (and (not= committer author) (tempid? committer-id))
                   (conj [:db/add committer-id :email/address committer]))]
    tx))

(defn unimported-commits
  "Returns the commit map of all unimported commits in the repository.
   Finds all commits that are reachable from a branch or tag."
  [db repo]
  (let [imported (set (flatten (d/q '[:find ?sha
                                      :where
                                      [?tx :tx/op :import]
                                      [?tx :tx/commit ?e]
                                      [?e :git/sha ?sha]]
                                    db)))
        all (repo/commits repo)
        unimported (remove imported all)]
    (pmap (partial repo/commit repo) unimported)))

(defn import-commits
  "Imports commits from repository into database.  Only imports commits that are not
   already in codeq.

   TODO: determine whether commits that have already been imported by another repository
   are handled correctly.

   TODO: explore whether the db-after value of the last transaction can/needs to be
   returned by this function, so that the caller can be sure it gets a view of the
   database with all commit transactions processed.  All of the commits must be
   processed before refs can be imported.  This could probably be done with a reduce
   by running the transaction in each step of the reduce and return the result in the
   accumulator."
  [conn repo]
  (let [db (d/db conn)
        commits (unimported-commits db repo)]
    (doseq [commit commits]
      (let [db (d/db conn)]
        (println "Importing commit:" (:sha commit))
        (d/transact conn (commit-tx-data db repo commit))))
    (println "Import complete!")))

(defn repository-tx-data
  "Create transaction data for repository import. :repo/uri is a unique/identity
   attribute, so transaction will update existing repository entity if present."
  [repo]
  [{:db/id (d/tempid :db.part/user)
    :repo/uri (:uri (repo/info repo))}])

(defn import-repository
  [conn repo]
  (let [tx-data (repository-tx-data repo)
        info (repo/info repo)]
    (println "Importing repository:" (:uri info) "as:" (:name info))
    (d/transact conn tx-data)))

(defn import-git
  [conn repo]
  (do
    @(import-repository conn repo)
    (import-commits conn repo)
    (d/request-index conn)))


(def analyzers [(datomic.codeq.analyzers.clj/impl)])

(defn run-analyzers
  [conn repo]
  (println "Analyzing...")
  (doseq [a analyzers]
    (let [aname (az/keyname a)
          exts (az/extensions a)
          srevs (set (map first (d/q '[:find ?rev :in $ ?a :where
                                       [?tx :tx/op :schema]
                                       [?tx :tx/analyzer ?a]
                                       [?tx :tx/analyzerRev ?rev]]
                                     (d/db conn) aname)))]
      (println "Running analyzer:" aname "on" exts)
      ;;install schema(s) if not yet present
      (doseq [[rev aschema] (az/schemas a)]
        (when-not (srevs rev)
          (d/transact conn 
                      (conj aschema {:db/id (d/tempid :db.part/tx)
                                     :tx/op :schema
                                     :tx/analyzer aname
                                     :tx/analyzerRev rev}))))
      (let [db (d/db conn)
            arev (az/revision a)
            ;;candidate files
            cfiles (set (map first (d/q '[:find ?f :in $ [?ext ...] :where
                                          [?fn :file/name ?n]
                                          [(.endsWith ^String ?n ?ext)]
                                          [?node :node/filename ?fn]
                                          [?node :node/object ?f]]
                                        db exts)))
            ;;already analyzed files
            afiles (set (map first (d/q '[:find ?f :in $ ?a ?rev :where
                                          [?tx :tx/op :analyze]
                                          [?tx :tx/analyzer ?a]
                                          [?tx :tx/analyzerRev ?rev]
                                          [?tx :tx/file ?f]]
                                        db aname arev)))]
        ;;find files not yet analyzed
        (doseq [f (sort (clojure.set/difference cfiles afiles))]
          ;;analyze them
          (println "analyzing file:" f " - sha: " (:git/sha (d/entity db f)))
          (let [db (d/db conn)
                sha (d/entity db f)
                src (repo/blob repo (:git/sha (d/entity db f)))
                adata (try
                        (az/analyze a db f src)
                        (catch Exception ex
                          (println (.getMessage ex))
                          []))]
            (d/transact conn
                        (conj adata {:db/id (d/tempid :db.part/tx)
                                     :tx/op :analyze
                                     :tx/file f
                                     :tx/analyzer aname
                                     :tx/analyzerRev arev})))))))
  (println "Analysis complete!"))

(defn main [& [location db-uri commit]]
  (if (and location db-uri)
      (let [conn (ensure-db db-uri)
            repo (repo/->Local location)]
        ;;(prn repo-uri)
        (import-git conn repo)
        (run-analyzers conn repo))
      (println "Usage: datomic.codeq.core repo-location db-uri [commit-name]")))

(defn -main
  [& args]
  (apply main args)
  (shutdown-agents)
  (System/exit 0))
