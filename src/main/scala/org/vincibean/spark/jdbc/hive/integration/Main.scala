package org.vincibean.spark.jdbc.hive.integration

import java.util.Properties

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.desc
import org.vincibean.spark.jdbc.hive.integration.domain.{Flight, Plane}
import com.typesafe.config.{Config, ConfigFactory}

object Main {

  val conf: Config = ConfigFactory.load()
  val resultJdbcUrl: String = conf.getString("jdbc.result.url")
  val resultTable: String = conf.getString("jdbc.result.table")

  val connectionProperties: Properties = {
    val props = new Properties()
    props.put("user", conf.getString("jdbc.username"))
    props.put("password", conf.getString("jdbc.password"))
    props
  }

  def main(args: Array[String]): Unit = {
    val wh = sys.props
      .get("user.dir")
      .map(x => new java.io.File(x))
      .map(_.toURI)
      .map(_.toString)
      .map(_ + "spark-warehouse")
      .getOrElse(conf.getString("application.warehouse"))
    val spark = SparkSession
      .builder()
      .appName(conf.getString("application.name"))
      .config("spark.sql.warehouse.dir", wh)
      .master(conf.getString("application.master"))
      .enableHiveSupport()
      .getOrCreate()
    try {
      val flights = readFlightDataset(spark)
      val planes = readPlaneDataset(spark)
      import spark.implicits._
      flights
        .as("f")
        .filter($"f.cancelled" === 0)
        .filter($"f.time.actualElapsedTime" > 0)
        .filter($"f.time.arrivalDelay" > 15)
        .join(planes.as("p"), $"f.tailNum" === $"p.tailNum")
        .select(
          $"p.tailNum".as("tailNum"),
          $"p.aircraftType".as("aircraftType"),
          $"p.constructionYear".as("constructionYear"),
          $"f.time.actualElapsedTime".as("flightTime"),
          $"f.time.arrivalDelay".as("delay"),
          ($"f.time.arrivalDelay" / $"f.time.actualElapsedTime").as(
            "delayRatio")
        )
        .write
        .jdbc(resultJdbcUrl, resultTable, connectionProperties)
      spark.read
        .jdbc(resultJdbcUrl, resultTable, connectionProperties)
        .orderBy(desc("delayRatio"))
        .show()
    } finally { spark.stop() }
  }

  private def readFlightDataset(spark: SparkSession): Dataset[Flight] = {
    import spark.sql
    import spark.implicits._
    // Determine the current working directory. If not defined default to "/tmp".
    val pwd = sys.props
      .get("user.dir")
      .getOrElse(conf.getString("application.defaultWorkingDir"))
      .replace("""\""", """/""")
    sql(
      s"""
        CREATE EXTERNAL TABLE IF NOT EXISTS flights (
        year INT,
        month INT,
        dayofmonth INT,
        dayofweek INT,
        departuretime INT,
        scheduleddeptime INT,
        arrivaltime INT,
        scheduledarrivaltime INT,
        uniquecarrier STRING,
        flightnum INT,
        tailnum STRING,
        actualelapsedtime INT,
        scheduledelapsedtime INT,
        airtime INT,
        arrivaldelay INT,
        departuredelay INT,
        origin STRING,
        dest STRING,
        distance INT,
        taxiintime INT,
        taxiouttime INT,
        cancelled INT,
        cancellationcode STRING,
        diverted INT,
        carrierdelay INT,
        weatherdelay INT,
        nasdelay INT,
        securitydelay INT,
        lateaircraftdelay INT)
        row format delimited fields terminated by '|'
        stored as textfile LOCATION '$pwd/src/main/resources/airline-flights/flights' """
    )
    sql("SELECT * FROM flights").map(Flight.parse)
  }

  private def readPlaneDataset(spark: SparkSession): Dataset[Plane] = {
    // We need to load the H2 Driver first
    Class.forName(conf.getString("jdbc.driver"))
    import spark.implicits._
    spark.read
      .jdbc(conf.getString("jdbc.planes.url"),
            conf.getString("jdbc.planes.table"),
            connectionProperties)
      .as[Plane]
  }

}
