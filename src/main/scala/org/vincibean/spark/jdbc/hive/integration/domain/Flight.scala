package org.vincibean.spark.jdbc.hive.integration.domain

import org.apache.spark.sql.Row
import org.vincibean.spark.jdbc.hive.integration.util.NullCellHandler.RowWithDefaults

object Flight {

  def parse(row: Row): Flight = Flight(
    date = FlightDate.parse(row),
    time = FlightTime.parse(row),
    uniquecarrier = row.getString(row.fieldIndex("uniquecarrier")),
    flightnum = row.getIntOrElse(row.fieldIndex("flightnum"), -1),
    tailnum = row.getString(row.fieldIndex("tailnum")),
    origin = row.getString(row.fieldIndex("origin")),
    dest = row.getString(row.fieldIndex("dest")),
    distance = row.getIntOrElse(row.fieldIndex("distance"), -1),
    cancelled = row.getIntOrElse(row.fieldIndex("cancelled"), -1),
    cancellationcode = row.getString(row.fieldIndex("cancellationcode")),
    diverted = row.getIntOrElse(row.fieldIndex("diverted"), -1),
    carrierdelay = row.getIntOrElse(row.fieldIndex("carrierdelay"), -1),
    weatherdelay = row.getIntOrElse(row.fieldIndex("weatherdelay"), -1),
    nasdelay = row.getIntOrElse(row.fieldIndex("nasdelay"), -1),
    securitydelay = row.getIntOrElse(row.fieldIndex("securitydelay"), -1),
    lateaircraftdelay =
      row.getIntOrElse(row.fieldIndex("lateaircraftdelay"), -1)
  )

}

case class Flight(date: FlightDate,
                  time: FlightTime,
                  uniquecarrier: String,
                  flightnum: Int,
                  tailnum: String,
                  origin: String,
                  dest: String,
                  distance: Int,
                  cancelled: Int,
                  cancellationcode: String,
                  diverted: Int,
                  carrierdelay: Int,
                  weatherdelay: Int,
                  nasdelay: Int,
                  securitydelay: Int,
                  lateaircraftdelay: Int)
