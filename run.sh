rm -Rf /tmp/hive
rm -Rf metastore_db
sbt clean compile flywayClean flywayMigrate run