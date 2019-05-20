package com.epam.workshop

import org.apache.spark.sql.SparkSession

class DumpLayerJob(implicit ss: SparkSession) {

  def processDump(hdfsStorage: HdfsGateway,
                  elasticsearchStorage: ElasticsearchGateway,
                  questionsInputPath: String,
                  answersInputPath: String,
                  questionsOutputPath: String,
                  answersOutputPath: String,
                  commonOutputPath: String,
                  esServer: String,
                  esPort: String,
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
