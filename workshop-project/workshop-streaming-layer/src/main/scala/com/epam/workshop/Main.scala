package com.epam.workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.language.postfixOps

class StreamingLayerArgs(args: Seq[String]) extends ScallopConf(args) {

  val inputPath: ScallopOption[String] = opt[String](
    descr = "Questions for streaming input file"
  )

  val outputPath: ScallopOption[String] = opt[String](
    descr = "Questions output path"
  )

  val bootstrapServer: ScallopOption[String] = opt[String](
    descr = "Kafka bootstrap server"
  )

  val topic: ScallopOption[String] = opt[String](
    descr = "Questions kafka topic"
  )

  verify()
}

object Main extends App {

  val streamingLayerArgs = new StreamingLayerArgs(args)

  //create spark session

  //create streaming context

  //start job
}
