package org.vincibean.spark.jdbc.hive.integration.domain

import org.apache.spark.sql.Row
import org.vincibean.spark.jdbc.hive.integration.util.NullCellHandler._

object Flight {

  def parse(row: Row): Flight = Flight(
    date = FlightDate.parse(row),
    time = FlightTime.parse(row),
    uniqueCarrier = row.getString(row.fieldIndex("uniquecarrier")),
    flightNum = row.getIntOrElse(row.fieldIndex("flightnum"), defaultInt),
    tailNum = row.getString(row.fieldIndex("tailnum")),
    origin = row.getString(row.fieldIndex("origin")),
    dest = row.getString(row.fieldIndex("dest")),
    distance = row.getIntOrElse(row.fieldIndex("distance"), defaultInt),
    cancelled = row.getIntOrElse(row.fieldIndex("cancelled"), defaultInt),
    cancellationCode = row.getString(row.fieldIndex("cancellationcode")),
    diverted = row.getIntOrElse(row.fieldIndex("diverted"), defaultInt),
    carrierDelay = row.getIntOrElse(row.fieldIndex("carrierdelay"), defaultInt),
    weatherDelay = row.getIntOrElse(row.fieldIndex("weatherdelay"), defaultInt),
    nasDelay = row.getIntOrElse(row.fieldIndex("nasdelay"), defaultInt),
    securityDelay =
      row.getIntOrElse(row.fieldIndex("securitydelay"), defaultInt),
    lateAircraftDelay =
      row.getIntOrElse(row.fieldIndex("lateaircraftdelay"), defaultInt)
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
