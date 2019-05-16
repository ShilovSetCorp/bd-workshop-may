package com.epam.workshop

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import shapeless.HList._
import shapeless._
import shapeless.syntax.std.traversable._

@JsonCreator
case class RawQuestion(@JsonProperty("acceptedAnswerId") acceptedAnswerId: String,
                       @JsonProperty("answerCount") answerCount: String,
                       @JsonProperty("body") body: String,
                       @JsonProperty("commentCount") commentCount: String,
                       @JsonProperty("creationDate") creationDate: String,
                       @JsonProperty("favoriteCount") favoriteCount: String,
                       @JsonProperty("id") id: String,
                       @JsonProperty("ownerUserName") ownerUserName: String,
                       @JsonProperty("ownerUserId") ownerUserId: String,
                       @JsonProperty("postTypeId") postTypeId: String,
                       @JsonProperty("score") score: String,
                       @JsonProperty("tags") tags: String,
                       @JsonProperty("viewCount") viewCount: String)

object RawQuestion {

  def fromList(list: List[String]): RawQuestion =
    (RawQuestion.apply _)
      .tupled(
        list
          .toHList[String :: String :: String :: String :: String :: String :: String :: String :: String :: String :: String :: String :: String :: HNil]
          .get
          .tupled
      )
}
