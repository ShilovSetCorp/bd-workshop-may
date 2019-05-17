package com.epam.workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.codehaus.jackson.map.ObjectMapper

class QuestionsProducer(implicit ss: SparkSession, ssc: StreamingContext) {

  def produceQuestions(inputPath: String,
                       outputPath: String,
                       bootstrapServer: String,
                       topic: String): Unit = {

    ssc
      .textFileStream(inputPath)
      .foreachRDD(rdd => rdd
        .map(str => RawQuestion.fromList(str.split(",").toList))
        .foreachPartition(partitionRdd => {

          //write raw questions

          //create kafka producer

          val objectMapper = new ObjectMapper

          //send message

          //close producer
        })
      )
  }
}
