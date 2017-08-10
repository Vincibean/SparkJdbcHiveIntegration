package org.vincibean.spark.jdbc.hive.integration

import java.util.Properties

import org.apache.spark.sql.SparkSession

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
        year STRING,
        month STRING,
        day_of_month STRING,
        day_of_week STRING,
        departure_time STRING,
        scheduled_dep_time STRING,
        arrival_time STRING,
        scheduled_arrival_time STRING,
        unique_carrier STRING,
        flight_num STRING,
        tail_num STRING,
        actual_elapsed_time STRING,
        scheduled_elapsed_time STRING,
        air_time STRING,
        arrival_delay STRING,
        departure_delay STRING,
        origin STRING,
        dest STRING,
        distance STRING,
        taxi_in_time STRING,
        taxi_out_time STRING,
        cancelled STRING,
        cancellation_code STRING,
        diverted STRING,
        carrier_delay STRING,
        weather_delay STRING,
        nas_delay STRING,
        security_delay STRING,
        late_aircraft_delay STRING)
        row format delimited fields terminated by '|'
        stored as textfile LOCATION 'src/main/resources/airline-flights/flights' """
    )

    Class.forName("org.h2.Driver") // We need to load the H2 Driver

    val connectionProperties = new Properties()
    connectionProperties.put("user", "SA")
    connectionProperties.put("password", "")

    val planes = spark.read
      .jdbc("jdbc:h2:file:./target/planes", "PLANE", connectionProperties)

    planes.createOrReplaceTempView("planes")

    sql(
      "SELECT * FROM flights JOIN planes ON flights.tailNum = planes.tailNum")
      .show()

    spark.stop()
  }

}
