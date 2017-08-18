package org.vincibean.spark.jdbc.hive.integration.domain

case class Plane(tailNum: String,
                 `type`: String,
                 manufacturer: String,
                 issueDate: String,
                 model: String,
                 status: String,
                 aircraftType: String,
                 engineType: String,
                 constructionYear: String)
