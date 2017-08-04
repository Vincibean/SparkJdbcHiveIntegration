package org.vincibean.spark.jdbc.hive.integration.domain

case class FlightTime(departureTime: Int,
                      scheduledDepTime: Int,
                      arrivalTime: Int,
                      scheduledArrivalTime: Int,
                      actualElapsedTime: Int,
                      crsElapsedTime: Int,
                      airTime: Int,
                      arrivalDelay: Int,
                      departureDelay: Int,
                      taxiInTime: Int,
                      taxiOutTime: Int)
