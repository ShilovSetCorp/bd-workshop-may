package com.epam.workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.streaming.StreamingContext

class QuestionsProducer(implicit ss: SparkSession, ssc: StreamingContext) {

  def produceQuestions(hdfsStorage: HdfsStorage,
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
