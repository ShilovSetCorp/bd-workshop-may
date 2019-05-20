package com.epam.workshop

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}

class KafkaGateway(implicit ss: SparkSession) extends Serializable {

  def writeStreamEntity[T](entity: Dataset[T], bootstrapServer: String, topic: String): StreamingQuery = ???
}
