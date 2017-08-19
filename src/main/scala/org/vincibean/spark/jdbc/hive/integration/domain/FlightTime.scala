package org.vincibean.spark.jdbc.hive.integration.domain

import org.apache.spark.sql.Row
import org.vincibean.spark.jdbc.hive.integration.util.NullCellHandler._

object FlightTime {

  def parse(row: Row): FlightTime = FlightTime(
    departureTime =
      row.getIntOrElse(row.fieldIndex("departuretime"), defaultInt),
    scheduledDepTime =
      row.getIntOrElse(row.fieldIndex("scheduleddeptime"), defaultInt),
    arrivalTime = row.getIntOrElse(row.fieldIndex("arrivaltime"), defaultInt),
    scheduledArrivalTime =
      row.getIntOrElse(row.fieldIndex("scheduledarrivaltime"), defaultInt),
    actualElapsedTime =
      row.getIntOrElse(row.fieldIndex("actualelapsedtime"), defaultInt),
    scheduledElapsedTime =
      row.getIntOrElse(row.fieldIndex("scheduledelapsedtime"), defaultInt),
    airTime = row.getIntOrElse(row.fieldIndex("airtime"), defaultInt),
    arrivalDelay = row.getIntOrElse(row.fieldIndex("arrivaldelay"), defaultInt),
    departureDelay =
      row.getIntOrElse(row.fieldIndex("departuredelay"), defaultInt),
    taxiInTime = row.getIntOrElse(row.fieldIndex("taxiintime"), defaultInt),
    taxiOutTime = row.getIntOrElse(row.fieldIndex("taxiouttime"), defaultInt)
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
