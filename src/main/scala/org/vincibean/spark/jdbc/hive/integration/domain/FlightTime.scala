package org.vincibean.spark.jdbc.hive.integration.domain

import org.apache.spark.sql.Row
import org.vincibean.spark.jdbc.hive.integration.util.NullCellHandler.RowWithDefaults

object FlightTime {

  def parse(row: Row): FlightTime = FlightTime(
    departuretime = row.getIntOrElse(row.fieldIndex("departuretime"), -1),
    scheduleddeptime = row.getIntOrElse(row.fieldIndex("scheduleddeptime"), -1),
    arrivaltime = row.getIntOrElse(row.fieldIndex("arrivaltime"), -1),
    scheduledarrivaltime =
      row.getIntOrElse(row.fieldIndex("scheduledarrivaltime"), -1),
    actualelapsedtime =
      row.getIntOrElse(row.fieldIndex("actualelapsedtime"), -1),
    scheduledelapsedtime =
      row.getIntOrElse(row.fieldIndex("scheduledelapsedtime"), -1),
    airtime = row.getIntOrElse(row.fieldIndex("airtime"), -1),
    arrivaldelay = row.getIntOrElse(row.fieldIndex("arrivaldelay"), -1),
    departuredelay = row.getIntOrElse(row.fieldIndex("departuredelay"), -1),
    taxiintime = row.getIntOrElse(row.fieldIndex("taxiintime"), -1),
    taxiouttime = row.getIntOrElse(row.fieldIndex("taxiouttime"), -1)
  )

}

case class FlightTime(departuretime: Int,
                      scheduleddeptime: Int,
                      arrivaltime: Int,
                      scheduledarrivaltime: Int,
                      actualelapsedtime: Int,
                      scheduledelapsedtime: Int,
                      airtime: Int,
                      arrivaldelay: Int,
                      departuredelay: Int,
                      taxiintime: Int,
                      taxiouttime: Int)
