rmdir C:\tmp\hive /s /q
rmdir metastore_db /s /q
mkdir C:\tmp\hive
winutils.exe chmod -R 777 C:\tmp\hive
sbt clean compile flywayClean flywayMigrate run
