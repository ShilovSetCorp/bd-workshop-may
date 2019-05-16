package com.epam.workshop

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

class HdfsStorageExample(implicit ss: SparkSession) {

  def readEntity[T: TypeTag](path: String)(implicit encoder: Encoder[T]): Dataset[T] =
    ss
      .read
      .schema(ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType])
      .option("escape", "\"")
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .csv(path)
      .as[T]

  def writeEntity(entity: Dataset[_], path: String, saveMode: SaveMode): Unit = entity
    .toDF()
    .coalesce(1)
    .write
    .mode(saveMode)
    .parquet(path)
}
