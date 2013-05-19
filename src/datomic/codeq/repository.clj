 (ns datomic.codeq.repository
   (:require [clojure.string :as s]))

(defprotocol Repository
  "Protocol defining interface with git repository."

  (commits [repo] [repo id]
    "Returns a seq of commit-shas in the chain of the commit specified by the
     id.  The id can be a full or partial sha, or a ref name.  Sequence is in
     commit order, eg:
       (\"c3bd979cfe65da35253b25cb62aad4271430405c\" ;; initial commit
        \"20f8db11804afc8c5a1752257d5fdfcc2d131d08\"
        \"d78aca3ec2e13e837026c4e0d9b2e25a5a536dce\"
        ...)")

  (commit [repo id]
    "Returns commit data of the commit identified by id.  The id can be a full
    or partial sha, or a ref name.  Example return:
       {:sha \"20f8db11804afc8c5a1752257d5fdfcc2d131d08\"
        :author {:name \"Rich Hickey\"
                 :email \"richhickey@gmail.com\"
                 :date #inst \"2012-09-28T21:55:25.000-00:00\"}
        :committer {:name \"Rich Hickey\"
                    :email \"richhickey@gmail.com\"
                    :date #inst \"2012-09-28T21:55:25.000-00:00\"}
        :parents [\"c3bd979cfe65da35253b25cb62aad4271430405c\"]
        :tree \"ba63180c1d120b469b275aef5da479ab6c3e2afd\"
        :message \"git tree walk\"}")

  (branches [repo]
    "Returns a sequence of the branches in the repository.  See branch for data
     representation. Ordering is unspecified.")

  (branch [repo label]
    "Returns the branch specified by the label argument, eg:
       {:label \"master\"
        :commit \"846aa5dbb06b2a43cdb8b699890343c25912242b\"}")

  (tags [repo]
    "Returns a sequence of the tags in the repository.  See tag for data
     representation. Ordering is unspecified.")

  (tag [repo label]
    "Returns the tag specified by the label argument.  Annotated tags have more
     data than lightweight tags, eg:
       {:label \"v0.9\" ;; annotated
        :commit \"0a43ee751dd2cca105490129627bee6d73f367bc\"
        :sha \"a2a50394b87c2df98c94b4cac986fb0274898d23\"
        :tagger {:name \"Dan Burkert\"
                 :email \"danburkert@gmail.com\"}
        :message \"This is an annoted tag message\"
        :annotated true}
       {:label \"v1.0\" ;; lightweight
        :sha \"7b08a45c5f64c7b0174e5c927b7476a349ffd118\"
        :annotated false}")

  (blob [repo sha]
    "Returns the text the blob (file) specified by sha argument in a string.")

  (tree [repo sha]
    "Returns the sequence of objects in the tree specified by the sha argument.
     Order is unspecfied. eg:
       ({:name \"README.md\"
         :sha \"b60ea231eb47eb98395237df17550dee9b38fb72\"
         :type :blob}
        {:name \"doc\"
         :sha \"bcfca612efa4ff65b3eb07f6889ebf73afb0e288\"
         :type :tree}
        ...)
     Optionally includes the mode of the file (permissions, etc.) when available.")

  (remotes [repo]
    "Returns a sequence of remote repositories associated with this repository.
     See remote for data representation. Order is unspecified.")

  (remote [repo label]
    "Returns the remote repository specified by label argument, eg:
       {:label \"origin\"
        :uri \"github.com/Datomic/codeq\"}
     Optionally includes the protocol of the remote when available.")

  (info [repo]
    "Returns a map of info about the repository.  Includes the repository's
     name and uri.  Optionally includes more info if available."))

;; Generic Repository Functions

(defn refs
  "Returns a sequence of the branches and tags in the repository, with an
   appropriate :type tag"
  [repo]
  (let [branches (branches repo)
        tags (tags repo)]
    (concat
      (map #(assoc % :type :branch) branches)
      (map #(assoc % :type :tag) tags))))

(defn all-commits
  "Returns a seq of all commits in the repository reachable from a branch or tag."
  [repo]
  (->> (refs repo)
       (map :commit)
       (distinct)
       (mapcat (partial commits repo))
       (distinct)))

(defn commit-directory
  "Returns a tree representation of every node (tree and blob) in the
   working directory of repo at the commit identified by sha."
  [repo sha]
  ((fn tree-expand
     [path {:keys [name type sha] :as node}]
     (let [new-path (conj path name)
           full-path (s/join "/" new-path)]
       (cond-> node
               true (assoc :path full-path)
               (= type :tree)
               (assoc :files (map (partial tree-expand new-path)
                                  (tree repo sha))))))
   [] {:type :tree
       :mode :040000
       :sha (:tree (commit repo sha))
       :name (:name (info repo))}))
