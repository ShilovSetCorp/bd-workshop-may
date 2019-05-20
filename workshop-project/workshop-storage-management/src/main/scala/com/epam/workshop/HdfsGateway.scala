package com.epam.workshop

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

class HdfsGateway(implicit ss: SparkSession) extends Serializable {

  def readEntity[T: TypeTag](path: String)(implicit encoder: Encoder[T]): Dataset[T] = ???

  def readStreamEntity[T: TypeTag](path: String)(implicit encoder: Encoder[T]): Dataset[T] = ???

  def writeEntity[T](entity: Dataset[T], path: String, saveMode: SaveMode): Unit = ???

  def writeStreamEntity[T](entity: Dataset[T], path: String, outputMode: OutputMode, format: String): StreamingQuery = ???
}
