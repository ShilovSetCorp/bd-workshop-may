package com.epam.workshop

import org.apache.spark.sql.Dataset
import org.elasticsearch.spark.sql._

class ElasticsearchStorageExample {

  def writeEntity(entity: Dataset[_], esServer: String, esIndex: String): Unit =
    entity
      .saveToEs(
        esIndex,
        Map("es.nodes" -> esServer)
      )
}
