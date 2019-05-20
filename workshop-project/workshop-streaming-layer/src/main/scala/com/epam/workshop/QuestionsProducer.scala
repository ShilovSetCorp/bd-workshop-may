package com.epam.workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

class QuestionsProducer(implicit ss: SparkSession) {

  def produceQuestions(hdfsGateway: HdfsGateway,
                       kafkaGateway: KafkaGateway,
                       inputPath: String,
                       outputPath: String,
                       bootstrapServer: String,
                       topic: String): Unit = {

    //implement streaming from text file
  }
}

object QuestionsProducer {

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
