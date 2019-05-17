package com.epam.workshop

import java.util.Properties

import com.epam.workshop.QuestionsProducerExample.{prepareTagsUdf, questionToCommon}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.codehaus.jackson.map.ObjectMapper

class QuestionsProducerExample(implicit ss: SparkSession, ssc: StreamingContext) {

  def produceQuestions(hdfsStorage: HdfsStorageExample,
                       inputPath: String,
                       outputPath: String,
                       bootstrapServer: String,
                       topic: String): Unit = {

    import ss.implicits._

    ssc
      .textFileStream(inputPath)
      .map(str => RawQuestion.fromList(str.split(",").toList))
      .foreachRDD { rdd => {

        val df = rdd.toDF()
        df
          .withColumn("tags", prepareTagsUdf(df.col("tags")))
          .as[RawQuestion]
          .map(questionToCommon)
          .foreachPartition(partitionDs => {

            val kafkaConfig = new Properties()
            kafkaConfig.put("key.serializer", classOf[StringSerializer])
            kafkaConfig.put("value.serializer", classOf[StringSerializer])
            kafkaConfig.put("bootstrap.servers", bootstrapServer)

            val producer = new KafkaProducer[String, String](kafkaConfig)

            val objectMapper = new ObjectMapper

            partitionDs
              .foreach(post =>
                producer.send(
                  new ProducerRecord(
                    topic,
                    post.id,
                    objectMapper.writeValueAsString(post)
                  )
                )
              )
            producer.close()
          })

        hdfsStorage.writeEntity(
          df.as[RawQuestion],
          outputPath,
          SaveMode.Append
        )
      }
      }

    ssc.start()
    ssc.awaitTermination()
  }
}

object QuestionsProducerExample {

  private val prepareTagsUdf = udf((col: String) => col
    .drop(1)
    .dropRight(1)
    .replaceAll("><", " ")
  )

  private def questionToCommon(question: RawQuestion) =
    CommonPost(
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
}
