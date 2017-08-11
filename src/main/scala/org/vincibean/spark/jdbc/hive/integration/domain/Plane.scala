package org.vincibean.spark.jdbc.hive.integration.domain

case class Plane(tailNum: String,
                 `type`: String,
                 manufacturer: String,
                 issue_date: String,
                 model: String,
                 status: String,
                 aircraft_type: String,
                 engine_type: String,
                 construction_year: String)
