package com.epam.workshop

import org.rogach.scallop.{ScallopConf, ScallopOption}

class DumpLayerArgs(args: Seq[String]) extends ScallopConf(args) {

  val questionsInput: ScallopOption[String] = opt[String](
    descr = "Questions input path"
  )

  val answersInput: ScallopOption[String] = opt[String](
    descr = "Answers input path"
  )

  val questionsOutput: ScallopOption[String] = opt[String](
    descr = "Questions output path"
  )

  val answersOutput: ScallopOption[String] = opt[String](
    descr = "Answers output path"
  )

  val commonOutput: ScallopOption[String] = opt[String](
    descr = "Common output path"
  )

  val esServer: ScallopOption[String] = opt[String](
    descr = "Elasticsearch server"
  )

  val esPort: ScallopOption[String] = opt[String](
    descr = "Elasticsearch port"
  )

  val esIndex: ScallopOption[String] = opt[String](
    descr = "Elasticsearch common posts index"
  )

  verify()
}

object Main extends App {

  val dumpLayerArgs = new DumpLayerArgs(args)

  //create spark session

  //start job
}
