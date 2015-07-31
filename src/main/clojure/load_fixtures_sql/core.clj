(ns load-fixtures-sql.core
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info error]]
            [clojure.tools.reader.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.lang System]))

(defn not-nil? [maybe-nil] (not (nil? maybe-nil)))
(defn not-empty? [maybe-empty] (not (empty? maybe-empty)))

(defn wild-card-transform
  "Find wild card transform maps"
  [k transform-map]
  (let [k-depth (count k)
        tm-filtered-by-depth (filter (fn [[k v]] (= (count k) k-depth)) transform-map)]
    (filter (fn [[tm-k tm-v]]
              (let [wild-cards (into #{} (keep-indexed (fn [idx item] (when (= identity item) idx)) tm-k))]
                (when (not-empty? wild-cards)
                  (let [matches (into #{} (map-indexed (fn [tm-k-i tm-k-v]
                                                         (if (contains? wild-cards tm-k-i)
                                                           true
                                                           (= (get k tm-k-i) tm-k-v))) tm-k))]
                    (= matches #{true}))))) tm-filtered-by-depth)))

(defn properties->map
  "Convert the properties list to a map, transforming the value
  whenever a transformer is defined for that key."
  ([properties]
     (properties->map properties {}))
  ([properties transformer-for]
     (letfn [(keyfor [s] (mapv keyword (str/split (key s) #"\.")))]
       (reduce (fn [accum entry]
                 (let [k (keyfor entry)]
                   (if-let [f (transformer-for k)]
                     (assoc-in accum k (f (val entry)))
                     (if-let [wild-card-f (second (first (wild-card-transform k transformer-for)))]
                       (assoc-in accum k (wild-card-f (val entry)))
                       (assoc-in accum k (val entry))))))
               {} properties))))

(defn resolve-resource
  "Sometimes the project dir is not on the classpath"
  [resource-name]
  (let [fn-parts (str/split resource-name (re-pattern (System/getProperty "file.separator")))]
    (if (> (count fn-parts) 1)
      (first (filter not-nil? (map io/resource [(last fn-parts) resource-name])))
      (io/resource resource-name))))

(defn load-properties
  "Load named properties file from the classpath, return a map with
  values optionally transformed by `transform-map`."
  ([resource-name]
     (load-properties resource-name {}))
  ([resource-name transform-map]
     (when-let [resource (resolve-resource resource-name)]
       (let [properties (doto (java.util.Properties.) (.load (io/input-stream resource)))]
         (properties->map properties transform-map)))))

(def transform-map {[:e-mail :exception-handler :recipient] #(str/split % #",")
                    [:fixtures identity :tables] #(str/split % #",")
                    [:postgres :opts-dump-schema] #(str/split % #" ")
                    [:postgres :opts-dump-table] #(str/split % #" ")})

(defn read-config
  "Read `config-file`, apply `config-transformers`"
  [config-file]
  (load-properties config-file transform-map))

(defn extract-resource-paths
  [project accumulator item]
  (conj accumulator (get-in project item)))

(defn search-resource-paths
  [resource-paths config-file-name]
  (when-let [config-file (first (filter #(.exists %) (map (fn [rp] (java.io.File. (str/join (System/getProperty "file.separator") [rp config-file-name]))) resource-paths)))]
    (.getPath config-file)))

(defn resolve-config-file
  "Locate the config file by looking in the projects resources dirs"
  [project config-file-path]
  (let [resource-paths (conj (flatten (filter not-nil? (reduce (partial extract-resource-paths project) [] [[:profiles :test :resource-paths] [:resource-paths]]))) "resources")
        _ (info "Searching resource-paths: " resource-paths " for " config-file-path)
        fn-parts (str/split config-file-path (re-pattern (System/getProperty "file.separator")))]
    (cond
     (= (first fn-parts) "resources")
     (search-resource-paths resource-paths (str/join (System/getProperty "file.separator") (rest fn-parts)))
     (= (count fn-parts) 1)
     (search-resource-paths (conj resource-paths "") (first fn-parts))
     :else config-file-path)))

(defn get-project-map
  "Obtain the project.clj as a map"
  []
  (apply hash-map (drop 3 (edn/read-string (slurp "project.clj")))))

(defn fixture-path-key
  [f-db f-col f-type f-name]
  (keyword (str/join "-" [f-db f-col f-type f-name])))

(defn fixture-path
  [root f-col f-type f-name]
  (str/join (System/getProperty "file.separator") [root (str/join "." [f-col f-type f-name "sql"])]))

(defn fixture-map-for-table
  [db root table]
  [(fixture-path-key db "initialise" "table" table) (fixture-path root "initialise" "table" table)])

(defn fixtures-map-for-db
  "Return key val pairs for a db entry from the config"
  [[db db-config]]
  (let [create-pair [(fixture-path-key (name db) "initialise" "database" "create") (fixture-path (:root db-config) "initialise" "database" "create")]
        table-pairs (map (partial fixture-map-for-table (name db) (:root db-config)) (:tables db-config))]
    (conj table-pairs create-pair)))

(defn get-fixture-map-from-config
  "Return a map of the fixture files generated by this config"
  [config]
  (let [db-stuff (concat (map fixtures-map-for-db (:fixtures config)))]
    (into {} (apply concat db-stuff))))

(defn load-fixtures
  "Load the configuration file, presumably as used by lein-fixtures-sql, search for sql fixtures and provide a map of the resulting paths"
  ([]
    (load-fixtures "resources/fixtures-sql.properties"))
  ([config-file-path]
    (let [project (get-project-map)
          resolved-config-file-path (resolve-config-file project config-file-path)
          _ (info (str/join "" ["Reading config from '" resolved-config-file-path "'"]))]
      (if-let [config (read-config resolved-config-file-path)]
        (get-fixture-map-from-config config)
        (println "No properties file found!")))))
