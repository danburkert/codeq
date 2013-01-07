(ns datomic.codeq.repository-test
  (:use clojure.test
        datomic.codeq.repository
        datomic.codeq.repository.local
        datomic.codeq.repository.github)
  (:require [clojure.set :as set]))

(declare repo)

;; Helper Functions
(defn- tree->blobs [tree-sha]
  "Get all blobs with a root of tree-sha"
  (let [files (tree repo tree-sha)]
    (flatten (map (fn [file]
              (if (= (:type file) :tree)
                (tree->blobs (:sha file))
                file)) files))))

(defn- nodes [tree-sha]
  "Return sequence of subnodes of node tree-sha."
  (let [subnodes (tree repo tree-sha)]
    (flatten (map (fn [node]
              (condp = (:type node)
                :blob node
                :tree (conj (nodes (:sha node))
                            node)))
            (tree repo tree-sha)))))

(defn- sha? [sha]
  "check that the sha is valid"
  (re-matches #"[a-f0-9]{40}" sha))

(defn- commit? [{:keys [author committer] :as commit}]
  "check that the commit map is valid"
  (and (contains? commit :sha)
       (sha? (:sha commit))
       (contains? commit :parents)
       (contains? commit :tree)
       (contains? commit :message)
       (contains? commit :author)
       (contains? commit :committer)
       (contains? author :name)
       (contains? author :email)
       (contains? author :date)
       (contains? committer :name)
       (contains? committer :email)
       (contains? committer :date)))

(defn- branch? [branch]
  "check that a branch map is valid"
  (and (contains? branch :commit)
       (contains? branch :label)
       (string? (:label branch))
       (sha? (:commit branch))))

(defn- tag? [tag]
  "check that a tag map is a valid lightweight or annotated tag."
  (let [lightweight-tag? (fn [tag]
                           (and (= (:annotated tag) false)
                                (contains? tag :commit)
                                (contains? tag :label)
                                (string? (:label tag))
                                (sha? (:commit tag))))
        annotated-tag? (fn [{tagger :tagger :as tag}]
                         (and (= (:annotated tag) true)
                              (contains? tag :commit)
                              (contains? tag :message)
                              (contains? tag :label)
                              (contains? tagger :name)
                              (contains? tagger :email)
                              (contains? tagger :date)
                              (string? (:label tag))
                              (string? (:message tag))
                              (sha? (:commit tag))))]
    (or (lightweight-tag? tag)
        (annotated-tag? tag))))

(defn- node? [node]
  "check that a node map is a valid tree or blob node"
  (contains? node :name)
  (contains? node :sha)
  (contains? node :type)
  (sha? (:sha node))
  (string? (:name node))
  (if-let [mode (:mode node)]
    (condp = (:type node)
          :tree (#{:040000} mode)
          :blob (#{:100644 :100755 :120000} mode))
    true))

(defn- remote? [remote]
  "check that a remote map is valid"
  (and (contains? remote :label)
       (contains? remote :uri)
       (string? (:label remote))
       (string? (:uri remote))
       (if-let [protocol (:protocol remote)]
         (#{:ssh :http :https :git} protocol)
         true)))

;; Tests

(deftest commits-test
  (testing "test commits function returns list of valid shas"
    (is (every? sha? (commits repo)))))

(deftest commit-test
  (testing "test commit function returns valid commit map"
    (let [commit-shas (commits repo)
          commits (pmap (partial commit repo) commit-shas)]
      (is (every? commit? commits)))))

(deftest branches-test
  (testing "test branches function returns valid branch maps"
    (let [branches (branches repo)]
      (is (every? branch? branches)))))

(deftest branch-test
  (testing "test branch function returns valid branch map"
    (let [all-branches (branches repo)
          labels (map :label all-branches)
          branches (map (partial branch repo) labels)]
      (is (every? branch? branches)))))

(deftest tags-test
  (testing "test tags function returns valid tag maps"
    (let [tags (tags repo)]
      (is (every? tag? tags)))))

(deftest tag-test
  (testing "test tag function returns valid tag map"
    (let [all-tags (tags repo)
          labels (map :label all-tags)
          tags (map (partial tag repo) labels)]
      (is (every? tag? tags)))))

(deftest blob-test
  (testing "test blob function returns string"
    (let [commit-shas (commits repo)
          root-tree-shas (distinct (pmap (comp :tree (partial commit repo)) commit-shas))
          blobs (mapcat tree->blobs root-tree-shas)
          blob-shas (distinct (pmap :sha blobs))
          blobs (pmap (partial blob repo) blob-shas)]
      (is (every? string? blobs)))))

(deftest tree-test
  (testing "test tree function returns valid tree-node maps"
    (let [commit-shas (commits repo)
          root-tree-shas (distinct (pmap (comp :tree (partial commit repo)) commit-shas))
          nodes (distinct (mapcat nodes root-tree-shas))]
      (is (every? node? nodes)))))

(deftest remotes-test
  (testing "test remotes function returns valid remote maps"
    (let [remotes (remotes repo)]
      (is (every? remote? remotes)))))

(deftest remote-test
  (testing "test remote function returns valid remote map"
    (let [all-remotes (remotes repo)
          remote-labels (map :label all-remotes)
          remotes (pmap (partial remote repo) remote-labels)]
      (is (every? remote? remotes)))))

(defn test-repo [repo]
  (with-redefs [repo repo]
    (commits-test)
    (commit-test)
    (branches-test)
    (branch-test)
    (tags-test)
    (tag-test)
    (blob-test)
    (tree-test)
    (remotes-test)
    (remote-test)))

(defn test-ns-hook []
  (let [config (-> "test-config.clj" slurp read-string)]
    (do
      (doseq [{uri :uri token :token} (:github config)]
        (println "testing repository" uri)
        (test-repo (github-repo uri token)))
      (doseq [{path :path} (:local config)]
        (println "testing repository" path)
        (test-repo (local-repo path))))))
