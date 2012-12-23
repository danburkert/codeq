(ns datomic.codeq.repository-test
  (:use clojure.test
        datomic.codeq.repository)
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

(defn- subtrees [root]
  (conj (mapcat (fn [file] (when (= (:type file) :tree)
                             (subtrees file)))
                (tree repo (:sha root)))
        root))

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

(defn- ref? [ref]
  "check that a branch or tag map is valid"
  (and (contains? ref :commit)
       (contains? ref :label)
       (string? (:label ref))
       (sha? (:commit ref))))

(defn- tree? [tree]
  "check that a tree map is valid"
  (and (contains? tree :filename)
       (contains? tree :sha)
       (contains? tree :type)
       (sha? (:sha tree))
       (#{:tree :blob} (:type tree))
       (string? (:filename tree))))

(defn- remote? [remote]
  "check that a remote map is valid"
  (and (contains? remote :label)
       (contains? remote :uri)
       (string? (:label remote))
       (string? (:uri remote))
       (#{nil :ssh :http :https :git} (:protocol remote))))

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
      (is (every? ref? branches)))))

(deftest branch-test
  (testing "test branch function returns valid branch map"
    (let [all-branches (branches repo)
          labels (map :label all-branches)
          branches (map (partial branch repo) labels)]
      (is (every? ref? branches)))))

(deftest tags-test
  (testing "test tags function returns valid tag maps"
    (let [tags (tags repo)]
      (is (every? ref? tags)))))

(deftest tag-test
  (testing "test tag function returns valid tag map"
    (let [all-tags (tags repo)
          labels (map :label all-tags)
          tags (map (partial tag repo) labels)]
      (is (every? ref? tags)))))

(deftest blob-test
  (testing "test blob function returns string"
    (let [commit-shas (commits repo)
          root-tree-shas (distinct (pmap (comp :tree (partial commit repo)) commit-shas))
          blobs (mapcat tree->blobs root-tree-shas)
          blob-shas (distinct (pmap :sha blobs))
          blobs (pmap (partial blob repo) blob-shas)]
      (is (every? string? blobs)))))

(deftest tree-test
  (testing "test tree function returns valid tree map"
    (let [commit-shas (commits repo)
          root-tree-shas (distinct (pmap (comp :tree (partial commit repo)) commit-shas))
          root-trees (mapcat (partial tree repo) root-tree-shas)
          trees (distinct (mapcat subtrees root-trees))]
      (is (every? tree? trees)))))

(deftest remotes-test
  (testing "test remotes function returns valid remote maps"
    (let [remotes (remotes repo)]
      (is (every? remote? remotes)))))

;; fastest with just remotes pmap
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
  (let [repos (:repositories (-> "test-config.clj" slurp read-string))]
    (doseq [repo repos] (test-repo repo))))
