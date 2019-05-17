package com.epam.workshop

import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

class HdfsStorage(implicit ss: SparkSession) extends Serializable {

  def readEntity[T: TypeTag](path: String)(implicit encoder: Encoder[T]): Dataset[T] = ???

  def writeEntity(entity: Dataset[_], path: String, saveMode: SaveMode): Unit = ???
}
