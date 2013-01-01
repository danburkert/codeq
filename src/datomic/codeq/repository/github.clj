(ns datomic.codeq.repository.github
  (:require [datomic.codeq.repository :refer :all]
            [clj-time.coerce :as tc]
            [clj-time.format :as tf]
            [tentacles.data :as data]
            [tentacles.repos :as repos]))

;(datomic.codeq.repository.github/->Github "danburkert"
                                          ;"sicp"
                                          ;"356a41581dec551bc91b6e3a26fc47783378f0fb")

(def commits-mem
  (memoize
    (fn [{:keys [owner repo token] :as r} sha]
      (let [pdate #(tc/to-date (tf/parse (tf/formatters :date-time-no-ms) %))
            cs (repos/commits owner repo {:all-pages true
                                          :per-page 100
                                          :oauth-token token
                                          :sort "oldest"
                                          :sha sha})
            clean (fn [c]
                    (-> (:commit c)
                      (assoc :tree (get-in c [:commit :tree :sha])
                             :sha (:sha c))
                      (assoc-in [:author :date]
                                (pdate (get-in c [:commit :author :date])))
                      (assoc-in [:committer :date]
                                (pdate (get-in c [:commit :committer :date])))
                      (dissoc :url :comment_count)))]
        (mapv clean cs)))))

(def branches-mem
  (memoize
    (fn [{:keys [owner repo token] :as r}]
      (repos/branches owner repo {:oauth-token token}))))

(def info-mem
  (memoize
    (fn [{:keys [owner repo token] :as r}]
      (repos/specific-repo owner repo {:oauth-token token}))))

(def tags-mem
  (memoize
    (fn [{:keys [owner repo token] :as r}]
      (repos/tags owner repo {:oauth-token token}))))

(defn- pref-match?
  [pref s]
  (= pref (subs s 0 (count pref))))

(defrecord Github
  [owner repo token]
  Repository
  (commits [r] (commits r ""))
  (commits [r sha]
    (->> (commits-mem r sha)
         (map :sha)
         reverse
         vec))
  (commit [r sha]
    (let [cs (commits r)]
      (first (filter #(pref-match? sha (:sha %)) cs))))
  (branches [r]
    (let [bs (branches-mem r)]
      (mapv (fn [branch] {:label (:name branch)
                          :sha (get-in branch [:commit :sha])})
            bs)))
  (branch [r label]
    (let [bs (branches r)]
      (first (filter #(= (:label %) label) bs))))
  (tags [r]
    (let [ts (tags-mem r)]
      (mapv (fn [branch] {:label (:name branch)
                          :sha (get-in branch [:commit :sha])})
            ts)))
  (tag [r label]
    (let [ts (tags r)]
      (first (filter #(= (:label %) label) ts))))
  (blob [r sha]
    (data/blob owner repo sha {:oauth-token token
                               :accept "application/vnd.github.beta.raw+json"}))
  (tree [r sha]
    (let [clean #(-> %
                     (dissoc :url)
                     (dissoc :size)
                     (update-in [:mode] keyword)
                     (update-in [:type] keyword))
          tree (data/tree owner repo sha {:oauth-token token})]
      (mapv clean (:tree tree))))
  (remotes [r]
    (let [info (info-mem r)
          repo->uri #(str "github.com/" (:full_name %))
          origin {:label "origin" :uri (repo->uri info)}
          roots (if-not (:fork info) {}
                  (let [parent {:label "parent" :uri (repo->uri (:parent info))}
                        source {:label "source" :uri (repo->uri (:source info))}]
                    (if (= (:uri parent) (:uri source))
                      [source]
                      [source parent])))]
      (conj roots origin)))
  (remote [r label]
    (let [rs (remotes r)]
      (first (filter #(= (:label %) label) rs)))))
