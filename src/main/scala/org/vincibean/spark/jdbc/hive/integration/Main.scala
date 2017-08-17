package org.vincibean.spark.jdbc.hive.integration

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.vincibean.spark.jdbc.hive.integration.domain.{Flight, Plane}

object Main {

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession
      .builder()
      .appName("Spark JDBC Hive Integration")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    try {
      import spark.sql
      import spark.implicits._
      // Determine the current working directory. If not defined default to "/tmp".
      val pwd = sys.props.get("user.dir").getOrElse("/tmp")
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
      flights.createOrReplaceTempView("flights")
      // We need to load the H2 Driver first
      Class.forName("org.h2.Driver")
      val connectionProperties = new Properties()
      connectionProperties.put("user", "SA")
      connectionProperties.put("password", "")
      val planes = spark.read
        .jdbc("jdbc:h2:file:./target/planes", "PLANES", connectionProperties)
        .as[Plane]
      planes.createOrReplaceTempView("planes")
      flights.join(planes, flights("tailnum") === planes("TAILNUM")).show()
    } finally { spark.stop() }
  }

}
