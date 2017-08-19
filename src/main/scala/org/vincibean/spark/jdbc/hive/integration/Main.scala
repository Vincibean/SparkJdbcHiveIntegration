package org.vincibean.spark.jdbc.hive.integration

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.vincibean.spark.jdbc.hive.integration.domain.{Flight, Plane}

object Main {

  val appName = "Spark JDBC Hive Integration"
  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
  val master = "local[*]"
  val defaultUserDir = "/tmp"
  val jdbcDriver = "org.h2.Driver"
  val username = "SA"
  val password = ""
  val planesJdbcAddress = "jdbc:h2:file:./target/planes"
  val planesTable = "PLANES"
  val resultJdbcAddress = "jdbc:h2:file:./target/result"
  val resultTable = "RESULT"

  val connectionProperties: Properties = {
    val props = new Properties()
    props.put("user", username)
    props.put("password", password)
    props
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
    try {
      import spark.sql
      import spark.implicits._
      // Determine the current working directory. If not defined default to "/tmp".
      val pwd = sys.props.get("user.dir").getOrElse(defaultUserDir)
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
      val flights = sql("SELECT * FROM flights").map(Flight.parse)
      // We need to load the H2 Driver first
      Class.forName(jdbcDriver)
      val planes = spark.read
        .jdbc(planesJdbcAddress, planesTable, connectionProperties)
        .as[Plane]
      val res = flights
        .as("f")
        .filter($"f.cancelled" === 0)
        .filter($"f.time.actualElapsedTime" > 0)
        .filter($"f.time.arrivalDelay" > 15)
        .join(planes.as("p"), $"f.tailNum" === $"p.tailNum")
        .select(
          $"p.tailNum".as("tailNum"),
          $"f.time.actualElapsedTime".as("flightTime"),
          $"f.time.arrivalDelay".as("delay"),
          ($"f.time.arrivalDelay" / $"f.time.actualElapsedTime").as("ratio")
        )
      res.write.jdbc(resultJdbcAddress, resultTable, connectionProperties)
      spark.read
        .jdbc(resultJdbcAddress, resultTable, connectionProperties)
        .orderBy(desc("ratio"))
        .show()
    } finally { spark.stop() }
  }

}
