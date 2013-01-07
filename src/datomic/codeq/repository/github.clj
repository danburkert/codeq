(ns datomic.codeq.repository.github
  (:require [datomic.codeq.repository :refer :all]
            [clojure.set :as set]
            [clj-time.coerce :as tc]
            [clj-time.format :as tf]
            [tentacles.data :as data]
            [tentacles.repos :as repos]))

(defn- get-commit
  [{:keys [owner repo token commit-index]} sha]
  "Retrieves sha from the commit-index.  If the index does not contain the
   commit, then then the commit chain is requested from github, and added to
   the index."
  ;; Currently the Github API only follows one parent of commits in the history,
  ;; so it cannot be relied on to provide a full commit history of a chain.  By
  ;; always accessing commits via this function, we can request missing commits
  ;; explicitly, and reduce requests by batching & saving results in the index
  ;; atom.
  (or (@commit-index sha)
      (let [cs (repos/commits owner repo {:all-pages true
                                          :per-page 100
                                          :oauth-token token
                                          :sha sha})
            pdate #(tc/to-date (tf/parse (tf/formatters :date-time-no-ms) %))
            psha #(subs % (inc (.lastIndexOf % "/")))
            format (fn [c]
                     (-> (:commit c)
                         (assoc :tree (get-in c [:commit :tree :sha])
                                :sha (:sha c))
                         (assoc :parents
                                (mapv (comp psha :url) (:parents c)))
                         (assoc-in [:author :date]
                                   (pdate (get-in c [:commit :author :date])))
                         (assoc-in [:committer :date]
                                   (pdate (get-in c [:commit :committer :date])))
                         (dissoc :url :comment_count)))]
        (loop [[{sha :sha :as c} :as cs] cs]
          (when-not (or (empty? cs)
                        (find @commit-index sha))
            (let [commit (format c)]
              (swap! commit-index assoc sha (format c))
              (recur (rest cs)))))
        (@commit-index sha))))

(defn- commit-chain
  [r shas]
  "Returns all commits in the commit-chains of the supplied shas, ordered by
   commit date."
  (let [build-chain
        (fn [[c & cs :as commits] chain]
          "commits - worklist of commits yet to be added to the chain
           chain - accumulator which collects the commits in the chain"
          (cond
            (empty? commits) chain
            (chain c) (recur cs chain)
            :else (recur (concat (map (partial get-commit r) (:parents c)) cs)
                         (conj chain c))))]
    (sort-by #(get-in % [:committer :date])
             (build-chain (map (partial get-commit r) shas) #{}))))

(def  ^{:private true} branches-mem
  (memoize
    (fn [{:keys [owner repo token] :as r}]
      (repos/branches owner repo {:oauth-token token}))))

(def ^{:private true} tree-mem
  (memoize
    (fn [{:keys [owner repo token] :as r} sha]

      (let [format-node #(-> %
                             (dissoc :url)
                             (dissoc :size)
                             (dissoc :path)
                             (assoc :name (:path %))
                             (update-in [:mode] keyword)
                             (update-in [:type] keyword))
            tree (data/tree owner repo sha {:oauth-token token})]
        (map format-node (:tree tree))))))

(def ^{:private true} info-mem
  (memoize
    (fn [{:keys [owner repo token] :as r}]
      (repos/specific-repo owner repo {:oauth-token token}))))

(def ^{:private true} tags-mem
  (memoize
    (fn [{:keys [owner repo token]}]
      (let [annotated (->> (data/references owner repo {:oauth-token token})
                           (filter #(= (get-in % [:object :type]) "tag"))
                           (map (comp
                                  (fn [{:keys [message object tag tagger sha]}]
                                    {:message message
                                     :label tag
                                     :sha sha
                                     :commit (:sha object)
                                     :tagger tagger
                                     :annotated true})
                                  (fn [sha] (data/tag owner repo sha
                                                      {:oauth-token token}))
                                  (fn [tag] (get-in tag [:object :sha])))))
            all (->> (repos/tags owner repo {:oauth-token token})
                     (map (fn [tag] {:label (:name tag)
                                     :commit (get-in tag [:commit :sha])}))
                     set)
            lightweight (map #(assoc % :annotated false)
                             (set/difference (set all)
                                             (set (map #(select-keys
                                                          % [:commit :label])
                                                       annotated))))]
        (concat annotated lightweight)))))

(defrecord Github
  [owner repo token commit-index]
  Repository

  (commits [r]
    (->> (branches r)
         (concat (tags r))
         (map :commit)
         (commit-chain r)
         (map :sha)))

  (commits [r sha]
    {:pre [(re-matches #"[a-f0-9]{40}" sha)]}
    (map :sha (commit-chain r [sha])))

  (commit [r sha]
    {:pre [(re-matches #"[a-f0-9]{40}" sha)]}
    (let [sha (if (re-matches #"[a-f0-9]{40}" sha)
                sha
                (:sha (or (tag r sha) (branch r sha))))]
          (get-commit r sha)))

  (branches [r]
    (map (fn [branch] {:label (:name branch)
                       :commit (get-in branch [:commit :sha])})
         (branches-mem r)))

  (branch [r label]
    (first (filter #(= (:label %) label) (branches r))))

  (tags [r]
    (tags-mem r))

  (tag [r label]
    (first (filter #(= (:label %) label) (tags r))))

  (blob [r sha]
    (data/blob owner repo sha {:oauth-token token
                               :accept "application/vnd.github.beta.raw+json"}))

  (tree [r sha]
    (tree-mem r sha))

  (remotes [r]
    (let [info (info-mem r)
          repo->uri #(str "github.com/" (:full_name %))
          origin {:label "origin" :uri (repo->uri info)}
          roots (if-not (:fork info) []
                  [{:label "parent" :uri (repo->uri (:parent info))}
                   {:label "source" :uri (repo->uri (:source info))}])]
      (conj roots origin)))

  (remote [r label]
    (let [rs (remotes r)]
      (first (filter #(= (:label %) label) rs)))))

(defn github-repo
  [uri token]
  {:pre [(string? uri) (string? token)]}
  "Returns a Github repository of the uri.  Uses the github OAuth token in
   all api calls.  Returns nil if uri or token are invalid.  Example URIs:
     http://github.com/clojure/clojure.git
     https://github.com/clojure/clojure.git
     git@github.com:clojure/clojure.git
     git://github.com/clojure/clojure.git"
  (if-let [[_ owner repo] ((some-fn
                             (partial re-matches #"^https?://github.com/([^/]+)/([^/]+)\.git$")
                             (partial re-matches #"^git@github.com:([^/]+)/([^/]+)\.git$")
                             (partial re-matches #"^git://github.com/([^/]+)/([^/]+)\.git$"))
                           uri)]
    (when (contains? (repos/specific-repo owner repo {:oauth-token token}) :name)
      (->Github owner repo token (atom {})))))
