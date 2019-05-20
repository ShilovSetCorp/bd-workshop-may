package com.epam.workshop

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

class QuestionsProducerExample(implicit ss: SparkSession) {

  def produceQuestions(hdfsGateway: HdfsGatewayExample,
                       kafkaGateway: KafkaGatewayExample,
                       inputPath: String,
                       outputPath: String,
                       bootstrapServer: String,
                       topic: String): Unit = {

    import ss.implicits._

    val questionsStream = hdfsGateway.readStreamEntity[RawQuestion](inputPath)

    val writeToHdfsQuery = hdfsGateway.writeStreamEntity(questionsStream, outputPath, OutputMode.Append(), "parquet")

    val writeToKafkaQuery = {

      val questionsDf = questionsStream.toDF()
      val posts = questionsDf
        .withColumn("tags", QuestionsProducerExample.prepareTagsUdf(questionsDf.col("tags")))
        .as[RawQuestion]
        .map(QuestionsProducerExample.questionToCommon)
        .mapPartitions(_.map(post => new ObjectMapper().writeValueAsString(post)))

      kafkaGateway.writeStreamEntity(posts, bootstrapServer, topic)
    }

    writeToHdfsQuery.awaitTermination()
    writeToKafkaQuery.awaitTermination()
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
