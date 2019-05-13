package com.epam.workshop

import org.apache.spark.sql.SparkSession

class DumpLayerJob(implicit ss: SparkSession) {

  def processDump(questionsInputPath: String,
                  answersInputPath: String,
                  questionsOutputPath: String,
                  answersOutputPath: String,
                  esServer: String,
                  esIndex: String): Unit = {

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
