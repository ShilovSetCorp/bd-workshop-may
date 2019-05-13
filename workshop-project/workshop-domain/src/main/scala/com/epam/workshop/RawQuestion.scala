package com.epam.workshop

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import shapeless.HList._
import shapeless._
import shapeless.syntax.std.traversable._

@JsonCreator
case class RawQuestion(@JsonProperty("id") id: String,
                       @JsonProperty("postTypeId") postTypeId: String,
                       @JsonProperty("acceptedAnswerId") acceptedAnswerId: String,
                       @JsonProperty("creationDate") creationDate: String,
                       @JsonProperty("score") score: String,
                       @JsonProperty("viewCount") viewCount: String,
                       @JsonProperty("body") body: String,
                       @JsonProperty("ownerUserId") ownerUserId: String,
                       @JsonProperty("lastEditorUserId") lastEditorUserId: String,
                       @JsonProperty("lastEditorDisplayName") lastEditorDisplayName: String,
                       @JsonProperty("lastEditDate") lastEditDate: String,
                       @JsonProperty("lastActivityDate") lastActivityDate: String,
                       @JsonProperty("title") title: String,
                       @JsonProperty("tags") tags: String,
                       @JsonProperty("answerCount") answerCount: String,
                       @JsonProperty("commentCount") commentCount: String,
                       @JsonProperty("favoriteCount") favoriteCount: String,
                       @JsonProperty("communityOwnedDate") communityOwnedDate: String)

object RawQuestion {

  def fromList(list: List[String]): RawQuestion =
    (RawQuestion.apply _).tupled(
      list.toHList[String :: String :: String :: String :: String :: String :: String :: String :: String :: String ::
        String :: String :: String :: String :: String :: String :: String :: String :: HNil].get.tupled
    )
}
