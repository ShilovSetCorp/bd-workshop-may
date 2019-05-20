package com.epam.workshop

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}

class KafkaGatewayExample(implicit ss: SparkSession) extends Serializable {

  def writeStreamEntity[T](entity: Dataset[T], bootstrapServer: String, topic: String): StreamingQuery = entity
    .toDF("value")
    .withColumn("key", lit("key"))
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option("checkpointLocation", "kafka-checkpoint")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("topic", topic)
    .start()
}
