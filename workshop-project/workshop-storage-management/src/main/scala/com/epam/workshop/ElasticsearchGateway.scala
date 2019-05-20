package com.epam.workshop

import org.apache.spark.sql.Dataset

class ElasticsearchGateway extends Serializable {

  def writeEntity(entity: Dataset[_], esServer: String, esPort: String, esIndex: String): Unit = ???
}
