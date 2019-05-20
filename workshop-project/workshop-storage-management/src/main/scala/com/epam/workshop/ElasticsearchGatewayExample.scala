package com.epam.workshop

import org.apache.spark.sql.Dataset
import org.elasticsearch.spark.sql._

class ElasticsearchGatewayExample extends Serializable {

  def writeEntity(entity: Dataset[_], esServer: String, esPort: String, esIndex: String): Unit =
    entity.saveToEs(
      esIndex,
      Map(
        "es.nodes" -> esServer,
        "es.port" -> esPort,
        "es.index.auto.create" -> "true"
      )
    )
}
