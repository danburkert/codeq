;;   Copyright (c) Metadata Partners, LLC and Contributors. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns datomic.codeq.core
  (:require [datomic.api :as d]
            [clojure.set :as set]
            [clojure.pprint :as pprint]
            [clojure.tools.cli :refer [cli]]
            [datomic.codeq.repository :as repo]
            [datomic.codeq.repositories.local :as local]
            [datomic.codeq.repositories.github :as github]
            [datomic.codeq.util :as util]
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
       :db/ident :repo/refs
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/many
       :db/doc "Associate repo with these git refs"
       :db/unique :db.unique/value
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :ref/commit
       :db/valueType :db.type/ref
       :db/cardinality :db.cardinality/one
       :db/doc "Commit pointed to by git ref"
       :db.install/_attribute :db.part/db}

      {:db/id #db/id[:db.part/db]
       :db/ident :ref/label
       :db/valueType :db.type/string
       :db/cardinality :db.cardinality/one
       :db/doc "Git ref label"
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
       :db/ident :code/highlight
       :db/valueType :db.type/string
       :db/cardinality :db.cardinality/one
       :db/doc "The highlighted HTML source code for a code segment"
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
  "Create transaction data for a commit import."
  [db repo {sha :sha message :message tree :tree parents :parents
            {author :email authored :date} :author
            {committer :email committed :date} :committer}]
  (let [repo-id (repo-id db repo)
        repo-name (:name (repo/info repo))
        sha->id (util/index->id-fn db :git/sha)
        email->id (util/index->id-fn db :email/address)
        filename->id (util/index->id-fn db :file/name)
        author-id (email->id author)
        committer-id (email->id committer)
        commit-id (d/tempid :db.part/user)
        tx-data (fn f [inpath {:keys [sha type name]}] ;; recursively descend through trees & blobs and create transaction data
                  (let [path (str inpath name)
                        object-id (sha->id sha)
                        filename-id (filename->id name)
                        path-id (filename->id path)
                        node-id (or (and (not (util/tempid? object-id))
                                         (not (util/tempid? filename-id))
                                         (ffirst (d/q '[:find ?e :in $ ?filename ?id
                                                        :where [?e :node/filename ?filename] [?e :node/object ?id]]
                                                      db filename-id object-id)))
                                    (d/tempid :db.part/user))
                        newpath (or (util/tempid? path-id) (util/tempid? node-id)
                                    (not (ffirst (d/q '[:find ?node :in $ ?path
                                                        :where [?node :node/paths ?path]]
                                                      db path-id))))
                        data (cond-> []
                               (util/tempid? filename-id) (conj [:db/add filename-id :file/name name])
                               (util/tempid? path-id) (conj [:db/add path-id :file/name path])
                               (util/tempid? node-id) (conj {:db/id node-id :node/filename filename-id :node/object object-id})
                               newpath (conj [:db/add node-id :node/paths path-id])
                               (util/tempid? object-id) (conj {:db/id object-id :git/sha sha :git/type type}))
                        data (if (and newpath (= type :tree))
                               (let [children (repo/tree repo sha)]
                                 (reduce (fn [data child]
                                           (let [[commit-id cdata] (f (str path "/") child)
                                                 data (into data cdata)]
                                             (cond-> data
                                               (util/tempid? object-id) (conj [:db/add object-id :tree/nodes commit-id]))))
                                         data children))
                               data)]
                    [node-id data]))
        [treeid treedata] (tx-data nil {:sha tree :type :tree :name repo-name})
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
                           :commit/committedAt committed}
                    message (assoc :commit/message message)
                    parents (assoc :commit/parents
                                   (mapv (fn [p]
                                           (let [id (sha->id p)]
                                             (assert (not (util/tempid? id))
                                                     (str "Parent " p " not previously imported"))
                                             id))
                                         parents)))])
        tx (cond-> tx
             (util/tempid? author-id)
             (conj [:db/add author-id :email/address author])
             (and (not= committer author) (util/tempid? committer-id))
             (conj [:db/add committer-id :email/address committer]))]
    tx))

(defn removed-refs
  "Returns the set of refs that have been removed from the repo since the last
   import."
  ;; Removed refs have an entity in codeq, but no corresponding ref in the
  ;; repository with identical :type and :label.
  [db repo]
  (let [repo-id (repo-id db repo)
        codeq-refs (util/qmap '[:find ?type ?label ?e
                                :in $ ?repo-id
                                :where
                                [?e :git/type ?type]
                                [?e :ref/label ?label]
                                [?repo-id :repo/refs ?e]]
                              [:type :label :id] db repo-id)
        repo-refs (repo/refs repo)]
    (set/join codeq-refs
              (set/difference
                (set/project codeq-refs [:type :label])
                (set/project repo-refs [:type :label])))))

(defn unimported-refs
  "Returns the set of refs which have been added or changed in the repository
   since the last import (or all refs if first import)."
  ;; Unimported refs are in the repository, but have no corresponding
  ;; codeq ref with identical :type :label and :commit attributes
  [db repo]
  (let [repo-id (repo-id db repo)
        codeq-refs (util/qmap '[:find ?type ?commit-sha ?label
                                :in $ ?repo-id
                                :where
                                [?e :git/type ?type]
                                [?e :ref/label ?label]
                                [?e :ref/commit ?c]
                                [?c :git/sha ?commit-sha]
                                [?repo-id :repo/refs ?e]]
                              [:type :commit :label] db repo-id)
        repo-refs (repo/refs repo)]
    (set/join repo-refs
              (set/difference
                (set/project repo-refs [:type :label :commit])
                (set codeq-refs)))))

(defn ref-tx-data
  "Create transaction data for ref import."
  ;; Possible scenarios:
  ;; 1) New ref: Create ref entity, and add reference attribute from the repo to
  ;;             the ref.
  ;; 2) Updated ref: Update commit, but don't touch repo entity reference."
  [db repo {:keys [type label commit]}]
  (if-let [commit-id (ffirst (d/q '[:find ?e
                                    :in $ ?sha
                                    :where
                                    [?tx :tx/op :import]
                                    [?tx :tx/commit ?e]
                                    [?e :git/sha ?sha]]
                                  db commit))]
    (let [uri (:uri (repo/info repo))
          ref-id (or (ffirst (d/q '[:find ?ref
                                    :in $ ?label ?type ?uri
                                    :where
                                    [?repo :repo/uri ?uri]
                                    [?repo :repo/refs ?ref]
                                    [?ref :git/type ?type]
                                    [?ref :ref/label ?label]]
                                  db label type uri))
                     (d/tempid :db.part/user))
          entity-tx {:db/id ref-id
                     :ref/commit commit-id
                     :ref/label label
                     :git/type type}
          reference-tx {:db/id (repo-id db repo) :repo/refs ref-id}]
      (if (util/tempid? ref-id)
        [entity-tx reference-tx]
        [entity-tx]))
    (throw (IllegalStateException.
             (str "while importing ref: " label ", commit: " commit " has not been imported")))))

(defn ref-retract-data
  "Create transaction data for ref retraction."
  [db repo ref] [[:db.fn/retractEntity (:id ref)]])

(defn import-refs
  "Import branches and tags into codeq."
  [conn repo]
  (let [db (d/db conn)
        refs (repo/refs repo)
        unimported (unimported-refs db repo)
        removed (removed-refs db repo)]
    (doseq [ref unimported]
      (println "Importing" (name (:type ref)) (:label ref))
      (->> ref
           (ref-tx-data db repo)
           (d/transact conn)))
    (doseq [ref removed]
      (println "Removing" (name (:type ref)) (:label ref))
      (->> ref
           (ref-retract-data db repo)
           (d/transact conn)))))

(defn unimported-commits
  "Returns the commit map of all unimported commits in the repository.
   Finds all commits that are reachable from a branch or tag."
  [db repo commit-id]
  (let [imported (set (map first (d/q '[:find ?sha
                                        :where
                                        [?tx :tx/op :import]
                                        [?tx :tx/commit ?e]
                                        [?e :git/sha ?sha]]
                                      db)))
        all (if commit-id
              (repo/commits repo commit-id)
              (repo/all-commits repo))
        unimported (remove imported all)]
    (pmap (partial repo/commit repo) unimported)))

(defn import-commits
  "Imports commits from repository into database.  Only imports commits that are not
   already in codeq."
  [conn repo commit-id]
  (let [db (d/db conn)
        commits (unimported-commits db repo commit-id)]
    (doseq [commit commits]
      (let [db (d/db conn)]
        (println "Importing commit:" (:sha commit))
        (d/transact conn (commit-tx-data db repo commit))))
    (println "Import complete!")))

(defn repository-tx-data
  "Create transaction data for repository import. :repo/uri is a unique/identity
   attribute, so transaction will update existing repository entity if present."
  [db repo]
  [{:db/id (d/tempid :db.part/user)
    :repo/uri (:uri (repo/info repo))}])

(defn import-repository
  [conn repo]
  (let [db (d/db conn)
        tx-data (repository-tx-data db repo)
        info (repo/info repo)]
    (println "Importing repository:" (:uri info))
    (d/transact conn tx-data)))

(def analyzers [(datomic.codeq.analyzers.clj/impl)])

(defn run-analyzers
  [conn repo {:keys [text highlight]}]
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
        (doseq [f (sort (set/difference cfiles afiles))]
          ;;analyze them
          (println "analyzing file:" f " - sha: " (:git/sha (d/entity db f)))
          (let [db (d/db conn)
                sha (d/entity db f)
                src (repo/blob repo (:git/sha (d/entity db f)))
                adata (try
                        (az/analyze a db f src)
                        (catch Exception ex
                          (println (.getMessage ex))
                          []))
                tdata (conj adata {:db/id (d/tempid :db.part/tx)
                                   :tx/op :analyze
                                   :tx/file f
                                   :tx/analyzer aname
                                   :tx/analyzerRev arev})]
            (cond->> tdata
              highlight (util/highlight (subs (first exts) 1))
              (not text) (util/strip-attribute :code/text)
              true (d/transact conn)))))))
  (println "Analysis complete!"))

(defn main [repo {:keys [datomic commit refs] :as opts}]
  (let [conn (ensure-db datomic)]
    (import-repository conn repo)
    (import-commits conn repo commit)
    (when refs (import-refs conn repo))
    (d/request-index conn)
    (run-analyzers conn repo opts)))

(def ^{:private true} cli-menu
  [["-h" "--help" "Display this menu." :flag true :default false]
   ["-r" "--repo" "Repository URI. Local file path or Github clone URL."]
   ["-t" "--token" "Github OAuth token. Required for Github imports."]
   ["-d" "--datomic" "Datomic database URI." :default "datomic:free://localhost:4334/git"]
   ["-c" "--commit" "Commit SHA, branch, or tag to import."]
   ["--refs" "Import branches and tags into codeq." :flag true :default true]
   ["--text" "Import text of codeqs." :flag true :default true]
   ["--highlight" "Import highlighted codeq text." :flag true :default false]])

(defn -main [& args]
  (let
    [[opts _ msg] (apply cli args cli-menu)
     repo (when (:repo opts)
            (if (:token opts)
              (github/github-repo (:repo opts) (:token opts))
              (local/local-repo (:repo opts))))]
    (if (and (not (:help opts))
             repo)
      (main repo opts)
      (println msg))
    (shutdown-agents)
    (System/exit 0)))

(comment
  (def uri "datomic:mem://sample4")
  (def repo (local/local-repo "/Users/dcb/src/git-sample"))
  (main repo uri "master")

  (def conn (d/connect uri))
  (def db (d/db conn))
  (seq (d/datoms db :aevt :file/name))
  (seq (d/datoms db :aevt :commit/message))
  (seq (d/datoms db :aevt :tx/file))
  (count (seq (d/datoms db :aevt :code/sha)))
  (take 20 (seq (d/datoms db :aevt :code/text)))
  (seq (d/datoms db :aevt :code/name))
  (count (seq (d/datoms db :aevt :codeq/code)))
  (d/q '[:find ?e :where [?f :file/name "core.clj"] [?n :node/filename ?f] [?n :node/object ?e]] db)
  (d/q '[:find ?m :where [_ :commit/message ?m] [(.contains ^String ?m "\n")]] db)
  (d/q '[:find ?m :where [_ :code/text ?m] [(.contains ^String ?m "(ns ")]] db)
  (sort (d/q '[:find ?var ?def :where [?cn :code/name ?var] [?cq :clj/def ?cn] [?cq :codeq/code ?def]] db))
  (sort (d/q '[:find ?var ?def :where [?cn :code/name ?var] [?cq :clj/ns ?cn] [?cq :codeq/code ?def]] db))
  (sort (d/q '[:find ?var ?def ?n :where
               [?cn :code/name ?var]
               [?cq :clj/ns ?cn]
               [?cq :codeq/file ?f]
               [?n :node/object ?f]
               [?cq :codeq/code ?def]] db))
  )
