package org.vincibean.spark.jdbc.hive.integration.domain

case class Flight(date: FlightDate,
                  times: FlightTime,
                  uniqueCarrier: String,
                  flightNum: Int,
                  tailNum: String,
                  origin: String,
                  dest: String,
                  distance: Int,
                  canceled: Int,
                  cancellationCode: String,
                  diverted: Int,
                  carrierDelay: Int,
                  weatherDelay: Int,
                  nasDelay: Int,
                  securityDelay: Int,
                  lateAircraftDelay: Int)
