package org.vincibean.spark.jdbc.hive.integration.util

import org.apache.spark.sql.Row

object NullCellHandler {

  type Position = Int

  implicit class RowWithDefaults(row: Row) {

    def getIntOrElse(position: Position, default: Int): Int =
      if (row.isNullAt(position)) {
        default
      } else {
        row.getInt(position)
      }

  }

}
