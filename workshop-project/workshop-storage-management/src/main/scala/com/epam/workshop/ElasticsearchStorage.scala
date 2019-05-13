package com.epam.workshop

import org.apache.spark.sql.Dataset
import org.elasticsearch.spark.sql._

class ElasticsearchStorage {

  def writeEntity(entity: Dataset[_], esServer: String, esIndex: String): Unit = ???
}
