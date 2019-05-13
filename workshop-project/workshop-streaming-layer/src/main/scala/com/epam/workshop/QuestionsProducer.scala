package com.epam.workshop

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.codehaus.jackson.map.ObjectMapper

class QuestionsProducer(implicit ss: SparkSession, ssc: StreamingContext) {

  def produceQuestions(inputPath: String,
                       outputPath: String,
                       bootstrapServer: String,
                       topic: String): Unit = {

    import ss.implicits._

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
                question.body,
                question.tags,
                question.title,
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
