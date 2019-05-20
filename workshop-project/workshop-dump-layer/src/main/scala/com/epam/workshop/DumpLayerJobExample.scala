package com.epam.workshop

import com.epam.workshop.DumpLayerJobExample.{answerWithQuestionToCommon, prepareTagsUdf, questionToCommon}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class DumpLayerJobExample(implicit ss: SparkSession) {

  def processDump(hdfsStorage: HdfsGatewayExample,
                  elasticsearchStorage: ElasticsearchGatewayExample,
                  questionsInputPath: String,
                  answersInputPath: String,
                  questionsOutputPath: String,
                  answersOutputPath: String,
                  commonOutputPath: String,
                  esServer: String,
                  esPort: String,
                  esIndex: String): Unit = {

    import ss.implicits._

    val rawQuestions: Dataset[RawQuestion] = hdfsStorage.readEntity[RawQuestion](questionsInputPath)
    val answers = hdfsStorage.readEntity[RawAnswer](answersInputPath)

    hdfsStorage.writeEntity(rawQuestions, questionsOutputPath, SaveMode.Overwrite)
    hdfsStorage.writeEntity(answers, answersOutputPath, SaveMode.Overwrite)

    val questions = rawQuestions
      .withColumn("tags", prepareTagsUdf(rawQuestions.col("tags")))
      .as[RawQuestion]

    val answersCommonView = answers
      .joinWith(
        questions,
        answers("parentId") equalTo questions("id"),
        "inner"
      )
      .map(answerWithQuestionToCommon(_))

    val questionsCommonView = questions.map(questionToCommon(_))

    val commonPosts = (answersCommonView union questionsCommonView).cache()

    hdfsStorage.writeEntity(commonPosts, commonOutputPath, SaveMode.Overwrite)

    elasticsearchStorage.writeEntity(commonPosts, esServer, esPort, esIndex)
  }
}

object DumpLayerJobExample {

  private val prepareTagsUdf = udf((col: String) => col
    .drop(1)
    .dropRight(1)
    .replaceAll("><", " ")
  )

  private def answerWithQuestionToCommon(answerWithQuestion: (RawAnswer, RawQuestion)) =
    answerWithQuestion match {
      case (answer, question) =>
        CommonPost(
          answer.id,
          answer.postTypeId,
          question.id,
          answer.creationDate,
          answer.ownerUserId,
          question.tags,
          answer.score,
          null,
          null
        )
    }

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
