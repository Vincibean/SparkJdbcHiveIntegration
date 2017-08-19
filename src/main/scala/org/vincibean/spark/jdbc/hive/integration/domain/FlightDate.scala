package org.vincibean.spark.jdbc.hive.integration.domain

import org.apache.spark.sql.Row
import org.vincibean.spark.jdbc.hive.integration.util.NullCellHandler._

object FlightDate {

  def parse(row: Row): FlightDate = FlightDate(
    year = row.getIntOrElse(row.fieldIndex("year"), defaultInt),
    month = row.getIntOrElse(row.fieldIndex("month"), defaultInt),
    dayOfMonth = row.getIntOrElse(row.fieldIndex("dayofmonth"), defaultInt),
    dayOfWeek = row.getIntOrElse(row.fieldIndex("dayofweek"), defaultInt)
  )

}

case class FlightDate(year: Int, month: Int, dayOfMonth: Int, dayOfWeek: Int)
