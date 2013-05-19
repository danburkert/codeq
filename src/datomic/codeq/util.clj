;;   Copyright (c) Metadata Partners, LLC and Contributors. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns datomic.codeq.util
  (:require [datomic.api :as d]
            [clojure.java.io :as io]))

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

(def tempid? map?)

(defn qmap
  "Returns the results of a query in map form with specified keys"
  [query keys db & args]
  (mapv (partial zipmap keys)
        (apply d/q query db args)))

(defn strip-attribute
  "Removes datoms of attribute from a sequence of transactions datoms."
  [attribute tx-data]
  (->> tx-data
       (filter (fn [tx] (or (map? tx)
                            (not= (nth tx 2) attribute))))
       (map (fn [tx] (if (map? tx)
                       (dissoc tx attribute)
                       tx)))))

(defn- pygmentize
  "Highlight text with pygments."
  [file-ext text]
  (let [proc (.exec (Runtime/getRuntime)
                    (str "pygmentize -l " file-ext " -fhtml"))]
    (with-open [^java.io.Writer input (io/writer (.getOutputStream proc))]
      (.write input text))
    (with-open [output (.getInputStream proc)]
      (slurp output))))

(defn highlight
  "Include :code/highlight attribute in new code segment entities included in
   transaction data.  Depends on pygmentize being on the path.

   This will highlight the code segment's text using a highlighter appropriate
   for the importing analyser. So in the incredibly unlikely event that there
   are two identical code segments from different languages that would require
   seperate highlighting parsers, the first imported will win."
  [file-ext tx-data]
  (let [add-tx (fn [tx]
                 (cond
                   (map? tx)
                   (if (contains? tx :code/text)
                     (assoc tx :code/highlight (pygmentize file-ext (:code/text tx)))
                     tx)
                   (vector? tx)
                   (if (and (= (nth tx 2) :code/text) (= (nth tx 0) :db/add))
                     {:db/id (nth tx 1)
                      (nth tx 2) (nth tx 3)
                      :code/highlight (pygmentize file-ext (nth tx 3))}
                     tx)
                   :else tx))]
    (map add-tx tx-data)))

(comment
  (pygmentize "clj" "(+ 1 1)")
  )
