package org.vincibean.spark.jdbc.hive.integration.domain

import org.apache.spark.sql.Row
import org.vincibean.spark.jdbc.hive.integration.util.NullCellHandler.RowWithDefaults

object Flight {

  def parse(row: Row): Flight = Flight(
    date = FlightDate.parse(row),
    time = FlightTime.parse(row),
    uniqueCarrier = row.getString(row.fieldIndex("uniquecarrier")),
    flightNum = row.getIntOrElse(row.fieldIndex("flightnum"), -1),
    tailNum = row.getString(row.fieldIndex("tailnum")),
    origin = row.getString(row.fieldIndex("origin")),
    dest = row.getString(row.fieldIndex("dest")),
    distance = row.getIntOrElse(row.fieldIndex("distance"), -1),
    cancelled = row.getIntOrElse(row.fieldIndex("cancelled"), -1),
    cancellationCode = row.getString(row.fieldIndex("cancellationcode")),
    diverted = row.getIntOrElse(row.fieldIndex("diverted"), -1),
    carrierDelay = row.getIntOrElse(row.fieldIndex("carrierdelay"), -1),
    weatherDelay = row.getIntOrElse(row.fieldIndex("weatherdelay"), -1),
    nasDelay = row.getIntOrElse(row.fieldIndex("nasdelay"), -1),
    securityDelay = row.getIntOrElse(row.fieldIndex("securitydelay"), -1),
    lateAircraftDelay =
      row.getIntOrElse(row.fieldIndex("lateaircraftdelay"), -1)
  )

}

case class Flight(date: FlightDate,
                  time: FlightTime,
                  uniqueCarrier: String,
                  flightNum: Int,
                  tailNum: String,
                  origin: String,
                  dest: String,
                  distance: Int,
                  cancelled: Int,
                  cancellationCode: String,
                  diverted: Int,
                  carrierDelay: Int,
                  weatherDelay: Int,
                  nasDelay: Int,
                  securityDelay: Int,
                  lateAircraftDelay: Int)
