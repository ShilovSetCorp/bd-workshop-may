package com.epam.workshop

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

class HdfsGatewayExample(implicit ss: SparkSession) extends Serializable {

  def readEntity[T: TypeTag](path: String)(implicit encoder: Encoder[T]): Dataset[T] =
    ss
      .read
      .schema(ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType])
      .option("escape", "\"")
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .csv(path)
      .as[T]

  def readStreamEntity[T: TypeTag](path: String)(implicit encoder: Encoder[T]): Dataset[T] =
    ss
      .readStream
      .schema(ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType])
      .option("escape", "\"")
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .csv(path)
      .as[T]

  def writeEntity[T](entity: Dataset[T], path: String, saveMode: SaveMode): Unit = entity
    .coalesce(1)
    .write
    .mode(saveMode)
    .parquet(path)

  def writeStreamEntity[T](entity: Dataset[T], path: String, outputMode: OutputMode, format: String): StreamingQuery = entity
    .coalesce(1)
    .writeStream
    .outputMode(outputMode)
    .format(format)
    .option("checkpointLocation", "hdfs-checkpoint")
    .start(path)
}
