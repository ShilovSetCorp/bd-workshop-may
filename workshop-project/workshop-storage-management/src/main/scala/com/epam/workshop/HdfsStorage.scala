package com.epam.workshop

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

class HdfsStorage(implicit ss: SparkSession) {

  def readEntity[T: TypeTag](path: String)(implicit encoder: Encoder[T]): Dataset[T] = ???

  def writeEntity(entity: Dataset[_], path: String, saveMode: SaveMode): Unit = ???
}
