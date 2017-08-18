package org.vincibean.spark.jdbc.hive.integration.domain

import org.apache.spark.sql.Row
import org.vincibean.spark.jdbc.hive.integration.util.NullCellHandler.RowWithDefaults

object FlightTime {

  def parse(row: Row): FlightTime = FlightTime(
    departureTime = row.getIntOrElse(row.fieldIndex("departuretime"), -1),
    scheduledDepTime = row.getIntOrElse(row.fieldIndex("scheduleddeptime"), -1),
    arrivalTime = row.getIntOrElse(row.fieldIndex("arrivaltime"), -1),
    scheduledArrivalTime =
      row.getIntOrElse(row.fieldIndex("scheduledarrivaltime"), -1),
    actualElapsedTime =
      row.getIntOrElse(row.fieldIndex("actualelapsedtime"), -1),
    scheduledElapsedTime =
      row.getIntOrElse(row.fieldIndex("scheduledelapsedtime"), -1),
    airTime = row.getIntOrElse(row.fieldIndex("airtime"), -1),
    arrivalDelay = row.getIntOrElse(row.fieldIndex("arrivaldelay"), -1),
    departureDelay = row.getIntOrElse(row.fieldIndex("departuredelay"), -1),
    taxiInTime = row.getIntOrElse(row.fieldIndex("taxiintime"), -1),
    taxiOutTime = row.getIntOrElse(row.fieldIndex("taxiouttime"), -1)
  )

}

case class FlightTime(departureTime: Int,
                      scheduledDepTime: Int,
                      arrivalTime: Int,
                      scheduledArrivalTime: Int,
                      actualElapsedTime: Int,
                      scheduledElapsedTime: Int,
                      airTime: Int,
                      arrivalDelay: Int,
                      departureDelay: Int,
                      taxiInTime: Int,
                      taxiOutTime: Int)
