package com.epam.workshop

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}

import scala.beans.BeanProperty

@JsonCreator
case class CommonPost(@BeanProperty @JsonProperty("id") id: String,
                      @BeanProperty @JsonProperty("postType") postType: String,
                      @BeanProperty @JsonProperty("questionId") questionId: String,
                      @BeanProperty @JsonProperty("creationDate") creationDate: String,
                      @BeanProperty @JsonProperty("userId") userId: String,
                      @BeanProperty @JsonProperty("tags") tags: String,
                      @BeanProperty @JsonProperty("score") score: String,
                      @BeanProperty @JsonProperty("acceptedAnswerId") acceptedAnswerId: String,
                      @BeanProperty @JsonProperty("favoriteCount") favoriteCount: String)
