package org.vincibean.spark.jdbc.hive.integration.domain

case class Plane(tailNum: String,
                 kind: String,
                 manufacturer: String,
                 issueDate: String,
                 model: String,
                 status: String,
                 aircraftType: String,
                 engineType: String,
                 year: Int)
