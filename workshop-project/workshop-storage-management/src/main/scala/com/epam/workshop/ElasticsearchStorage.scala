package com.epam.workshop

import org.apache.spark.sql.Dataset

class ElasticsearchStorage extends Serializable {

  def writeEntity(entity: Dataset[_], esServer: String, esIndex: String): Unit = ???
}
