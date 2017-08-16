package org.vincibean.spark.jdbc.hive.integration

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.vincibean.spark.jdbc.hive.integration.domain.Plane

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

    import spark.sql

    sql(
      """
        CREATE EXTERNAL TABLE IF NOT EXISTS flights (
        year INT,
        month INT,
        day_of_month INT,
        day_of_week INT,
        departure_time INT,
        scheduled_dep_time INT,
        arrival_time INT,
        scheduled_arrival_time INT,
        unique_carrier STRING,
        flight_num INT,
        tail_num STRING,
        actual_elapsed_time INT,
        scheduled_elapsed_time INT,
        air_time INT,
        arrival_delay INT,
        departure_delay INT,
        origin STRING,
        dest STRING,
        distance INT,
        taxi_in_time INT,
        taxi_out_time INT,
        cancelled INT,
        cancellation_code STRING,
        diverted INT,
        carrier_delay INT,
        weather_delay INT,
        nas_delay INT,
        security_delay INT,
        late_aircraft_delay INT)
        row format delimited fields terminated by '|'
        stored as textfile LOCATION 'src/main/resources/airline-flights/flights' """
    )

    Class.forName("org.h2.Driver") // We need to load the H2 Driver

    val connectionProperties = new Properties()
    connectionProperties.put("user", "SA")
    connectionProperties.put("password", "")

    import spark.implicits._

    val planes = spark.read
      .jdbc("jdbc:h2:file:./target/planes", "PLANES", connectionProperties)
      .as[Plane]

    planes.createOrReplaceTempView("planes")

    sql(
      "SELECT * FROM flights JOIN planes ON flights.tailNum = planes.tailNum")
      .show()

    spark.stop()
  }

}
