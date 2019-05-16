package com.epam.workshop

import java.util.Properties

import com.epam.workshop.QuestionsProducerExample.prepareTagsUdf
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.codehaus.jackson.map.ObjectMapper

class QuestionsProducerExample(implicit ss: SparkSession, ssc: StreamingContext) {

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
          val rawQuestions = ss.createDataset[RawQuestion](partitionRdd.toSeq)
          new HdfsStorageExample()
            .writeEntity(
              rawQuestions
                .withColumn("tags", prepareTagsUdf(rawQuestions.col("tags")))
                .as[RawQuestion],
              outputPath,
              SaveMode.Append
            )

          val kafkaConfig = new Properties()
          kafkaConfig.put("key.serializer", classOf[StringSerializer])
          kafkaConfig.put("value.serializer", classOf[StringSerializer])
          kafkaConfig.put("bootstrap.servers", bootstrapServer)

          val producer = new KafkaProducer[String, String](kafkaConfig)

          val objectMapper = new ObjectMapper

          partitionRdd
            .foreach(question => {
              val value = CommonPost(
                question.id,
                question.postTypeId,
                question.id,
                question.creationDate,
                question.ownerUserId,
                question.tags,
                question.score,
                question.acceptedAnswerId,
                question.favoriteCount
              )
              producer.send(
                new ProducerRecord(
                  topic,
                  question.id,
                  objectMapper.writeValueAsString(value)
                )
              )
            })

          producer.close()
        })
      )
  }
}

object QuestionsProducerExample {

  private val prepareTagsUdf = udf((col: String) => col
    .drop(1)
    .dropRight(1)
    .replaceAll("><", " ")
  )
}
