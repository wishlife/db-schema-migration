(ns db.schema
  (:require [db.simple-jdbc :refer [execute]]
            [db.schema-validate :refer [validate-schema]])
  (:import (java.sql Connection
                     DriverManager
                     PreparedStatement
                     ResultSet
                     Statement))
  (:gen-class))

;; This is the basic record format of an upgrade
;;
;; The version-name must be a unique keyword from all other upgrades
;;
;; The upgrade and downgrade fields are either a SQL string or a
;; function to run when performing the upgrade/downgrade process.
;; When it is a function, it will recieve a single argument of a
;; java.sql.Connection to use for the upgrade.  The connection will
;; have auto-commit turned off, and the result will be committed after
;; normal return from the function.
;;
;; When possible, it is highly preferred to have the upgrade and
;; downgrade be SQL strings.  This is because the SQL will then be
;; recorded in the schema_version table.
(defrecord Migration [version-name upgrade downgrade])

(defn create-table
  "Helper function to make a 'CREATE TABLE' migration.  The upgrade
  SQL is a 'CREATE TABLE <table-name> ( <defs joined on ','> ).  The
  downgrade SQL is a 'DROP TABLE IF EXISTS'"
  [vname table-name defs]
  (Migration. vname
              (format "CREATE TABLE %s (\n%s\n)"
                      table-name
                      (clojure.string/join ",\n" defs))
              (str "DROP TABLE IF EXISTS " table-name)))

(defn create-view
  "Helper function to make a 'CREATE VIEW' migration.  This can save a
  little typing."
  [vname view query]
  (Migration. vname
              (str "CREATE VIEW " view " AS " query)
              (str "DROP VIEW IF EXISTS " view)))

(defn create-enum-table
  "Helper function to create a table that acts a an enum."
  [vname enum-name names]
  (Migration. vname
              ;; We use VARCHAR(63) for enum names as that postgres's
              ;; limit on CREATE TYPE [...] ENUM names.
              (str "CREATE TABLE " enum-name " (name VARCHAR(63) PRIMARY KEY);\n"
                   (when (not (empty? names))
                     (str "INSERT INTO " enum-name " VALUES "
                          (->> (map #(str "('" % "')") names)
                               (clojure.string/join ",")))))
              (str "DROP TABLE IF EXISTS " enum-name)))

(defn insert-enum-table
  "Helper function to add new values to an enum."
  [vname enum-name names]
  (Migration. vname
              (str "INSERT INTO " enum-name " VALUES "
                   (->> (map #(str "('" % "')") names)
                        (clojure.string/join ",")))
              ""))

(defn- database-name
  [db-subname]
  (clojure.string/replace db-subname #"^.*/|\?.*$" ""))

(defn- open-connection
  [{:keys [classname subprotocol subname user password] :as db}]
  (if (not (and classname subprotocol subname user password))
    (printf "Missing database(%s) parameters: classname: %s, subprotocol: %s, subname: %s, user: %s, password: %s.%n" db classname subprotocol subname user password))
  (let [url (format "jdbc:%s:%s" subprotocol subname)]
    (Class/forName classname)
    (try
      (DriverManager/getConnection url user password)
      (catch Exception ex
        (let [database (database-name subname)]
          (printf "Could not open connection to %s.%n" url)
          (printf "Please verify database exists and that %s has access to it.%n" user)
          (printf "If this is a new database, please run the following command in psql%n")
          (printf "CREATE USER %s WITH PASSWORD '%s';%n" user password)
          (printf "CREATE DATABASE %s ENCODING 'UTF8' OWNER %s;%n" database user)
          (printf "GRANT ALL PRIVILEGES ON DATABASE %s TO %s;%n" database user))
        (throw ex)))))

(defn run-migrations
  [^Connection conn action migrations]
  (let [count-to-run (count migrations)]
    (try
      (loop [no 1 migrations migrations]
        (if-let [migration (first migrations)]
          (let [ver (name (:version-name migration))
                process (get migration action)]
            (printf "Running %s for %s (%d of %d)..." (name action) ver no count-to-run)
            (flush)
            (condp #(%1 %2) process
              string? (execute conn process)
              fn? (process conn)
              (throw (IllegalArgumentException. (str "don't know how to run " ver))))
            (-> (case action
                  :upgrade (execute conn "INSERT INTO schema_version (version_name,sql) VALUES (?,?)" [ver (str process)])
                  :downgrade (execute conn "DELETE FROM schema_version WHERE version_name = ?" [ver]))
                (= 1) (or (throw (IllegalStateException. "version not bumped"))))
            (.commit conn)
            (println " done.")
            (recur (inc no) (next migrations)))
          (println (if (= 1 no) "No migrations to run." "Migrations complete."))))
      (catch Exception ex
        (printf " failed (%s)%n" (.getMessage ex))
        (.rollback conn)
        (throw ex)))))

(defn- migrate-schema
  [action migration-namespace] ;; TODO: add support for additional parameter [target-version]
  {:pre [(contains? #{:upgrade :downgrade} action)]}
  (let [migration-namespace (or migration-namespace "db-schema-migration")
        migration-path (-> (name migration-namespace)
                                (clojure.string/replace #"-" "_")
                                (clojure.string/replace #"\." "/"))]
    (load (str "/" migration-path)) ; load db-schema-migration/levels and db-schema-migration/db from resource db_schema_migration.clj
    (println "migration-namespace:" migration-namespace)
    (println "migration-path:" migration-path)
    (println "symbol:" (symbol migration-namespace "db"))
    (println "resolve:" (resolve (symbol migration-namespace "db")))
    (println "deref:" (deref (resolve (symbol migration-namespace "db"))))
    (println "result:" (deref (resolve (symbol migration-namespace "db"))))
    (with-open [^Connection conn (open-connection  (deref (resolve (symbol migration-namespace "db"))))]
      ;; Disable sanity check, no need any more
      ;; (migration-sanity-check conn)
      (.setAutoCommit conn false)
      (println "Creating schema_version table if it does not exist")
      (execute conn "CREATE TABLE IF NOT EXISTS schema_version (
                     version_name VARCHAR(80) NOT NULL UNIQUE,
                     migrated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                     sql TEXT NOT NULL
                  )")
      (.commit conn)
      (let [version-set (->> (execute conn "SELECT version_name FROM schema_version")
                             (map :version-name)
                             (map keyword)
                             (into #{}))]
        (printf "Schema currently has %d of %d upgrades%n" (count version-set) (count (deref (resolve (symbol migration-namespace "levels")))))
        (case action
          :upgrade
          (->> (deref (resolve (symbol migration-namespace "levels")))
               (remove #(contains? version-set (:version-name %)))
               (run-migrations conn :upgrade))

          :downgrade
          ;; downgrades would work something like this:
          ;; BUT MAKE SURE target-version EXISTS, or it will downgrade to an empty database!
          ;; (->> (reverse migration-levels)
          ;;      (take-while #(not= target-version (:version-name %)))
          ;;      (filter #(contains? version-set (:version-name %)))
          ;;      (run-migrations conn :downgrade))
          (throw (UnsupportedOperationException. "downgrade is not currently supported")))))))

(defn -main
  [& args]
  (-> (try
        (if (> (count args) 2)
          (throw (IllegalArgumentException. "Additional command arguments are not supported"))
          (case (first args)
            nil (println "Please specify one of 'upgrade', 'downgrade', or 'validate'")
            "validate" (validate-schema (database-name))
            ("upgrade" "downgrade") (migrate-schema (keyword (first args)) (second args))
            (throw (UnsupportedOperationException. (str "Invalid command: " (first args))))))
        (catch Throwable ex
          (.printStackTrace ex)
          2) ;; exit code
        (finally
          ;; validate uses agents to read sub-process streams.  we
          ;; need to shut them down.
          (shutdown-agents)
          (flush)))
      (or 0) ;; convert nil to exit code 0
      (System/exit)))
