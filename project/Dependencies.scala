import sbt._

trait Dependency {
  def organization: String

  def version: String

  def dependencies: Seq[ModuleID]
}

object Dependencies {

  object spark extends Dependency {
    lazy val organization: String = "org.apache.spark"
    lazy val version: String = "2.0.0"
    lazy val dependencies: Seq[ModuleID] = Seq(
      "spark-core",
      "spark-sql",
      "spark-hive"
    ).map(dep => organization %% dep % version)
  }

  object h2 extends Dependency {
    lazy val organization: String = "com.h2database"
    lazy val version: String = "1.4.191"
    lazy val dependencies: Seq[ModuleID] = Seq(organization % "h2" % version)
  }

  object typesafe extends Dependency {
    lazy val organization: String = "com.typesafe"
    lazy val version: String = "1.3.1"
    lazy val dependencies: Seq[ModuleID] = Seq(organization % "config" % version)
  }

}