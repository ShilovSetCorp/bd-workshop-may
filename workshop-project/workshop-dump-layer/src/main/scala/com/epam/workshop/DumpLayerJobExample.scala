package com.epam.workshop

import com.epam.workshop.DumpLayerJobExample.{answerWithQuestionToCommon, prepareTagsUdf, questionToCommon}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class DumpLayerJobExample(implicit ss: SparkSession) {

  def processDump(questionsInputPath: String,
                  answersInputPath: String,
                  questionsOutputPath: String,
                  answersOutputPath: String,
                  esServer: String,
                  esIndex: String): Unit = {

    import ss.implicits._

    val hdfsStorage = new HdfsStorageExample()

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

    new ElasticsearchStorageExample()
      .writeEntity(
        answersCommonView union questionsCommonView,
        esServer,
        esIndex
      )
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
          answer.id.toLong,
          answer.postTypeId.toLong,
          question.id.toLong,
          answer.creationDate,
          answer.ownerUserId.toLong,
          question.tags,
          answer.score.toLong,
          0L,
          0L
        )
    }

  private def questionToCommon(question: RawQuestion) =
    CommonPost(
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
}
