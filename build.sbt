import Dependencies._

import scala.language.postfixOps

name := "SparkJdbcHiveIntegration"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++= (spark dependencies) ++ (h2 dependencies) ++ (typesafe dependencies)

scalafmtOnCompile in ThisBuild := true

flywayUrl := "jdbc:h2:file:./target/PLANES"

flywayUser := "SA"
