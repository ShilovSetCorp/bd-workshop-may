package com.epam.workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

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

          partitionRdd
            .foreach(question => {
              val value = CommonPost(
                question.id.toLong,
                question.postTypeId.toLong,
                question.id.toLong,
                question.creationDate,
                question.ownerUserId.toLong,
                question.tags,
                question.score.toLong,
                question.acceptedAnswerId.toLong,
                question.favoriteCount.toLong
              )

              //send message
            })

          //close producer
        })
      )
  }
}
