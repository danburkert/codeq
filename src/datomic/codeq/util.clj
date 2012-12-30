;;   Copyright (c) Metadata Partners, LLC. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns datomic.codeq.util
  (:require [datomic.api :as d]))

(set! *warn-on-reflection* true)

(defn index-get-id
  "Get id of attribute attr in database db of value v"
  [db attr v]
  (let [d (first (d/index-range db attr v nil))]
    (when (and d (= (:v d) v))
      (:e d))))

(defn index->id-fn
  "Return function which takes a value, and returns the id of
   the attribute with the passed in value.  Finds existing id
   or creates new temporary id."
  [db attr]
  (memoize
   (fn [x]
     (or (index-get-id db attr x)
         (d/tempid :db.part/user)))))

(defmacro cond->
  [init & steps]
  (assert (even? (count steps)))
  (let [g (gensym)
        pstep (fn [[pred step]] `(if ~pred (-> ~g ~step) ~g))]
    `(let [~g ~init
           ~@(interleave (repeat g) (map pstep (partition 2 steps)))]
       ~g)))

(def tempid? map?)

(defn qmap
  "Returns the results of a query in map form with specified keys"
  [query keys db & args]
  (->> (apply d/q query db args)
    (mapv (partial zipmap keys))))
