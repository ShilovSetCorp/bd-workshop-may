package com.epam.workshop

import com.epam.workshop.DumpLayerJob.{answerWithQuestionToCommon, prepareTagsUdf, questionToCommon}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class DumpLayerJob(implicit ss: SparkSession) {

  def processDump(questionsInputPath: String,
                  answersInputPath: String,
                  questionsOutputPath: String,
                  answersOutputPath: String,
                  esServer: String,
                  esIndex: String): Unit = {

    import ss.implicits._

    //create hdfs storage

    //read questions and answers

    //write raw questions and answers

    //replace '<' and '>'

    //implement questions->common

    //implement answers->common

    //write common posts into Elasticsearch
  }
}

object DumpLayerJob {

  //implement replacement logic for tags
  private val prepareTagsUdf = ???

  private def answerWithQuestionToCommon(answerWithQuestion: (RawAnswer, RawQuestion)) =
    answerWithQuestion match {
      case (answer, question) =>
        CommonPost(
          answer.id.toLong,
          answer.postTypeId.toLong,
          question.id.toLong,
          answer.creationDate,
          answer.ownerUserId.toLong,
          answer.body,
          question.tags,
          question.title,
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
      question.body,
      question.tags,
      question.title,
      question.score.toLong,
      question.acceptedAnswerId.toLong,
      question.favoriteCount.toLong
    )
}
