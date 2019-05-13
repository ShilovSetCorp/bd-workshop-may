package com.epam.workshop

case class CommonPost(id: Long,
                      postType: Long,
                      questionId: Long,
                      creationDate: String,
                      userId: Long,
                      body: String,
                      tags: String,
                      title: String,
                      score: Long,
                      acceptedAnswerId: Long,
                      favoriteCount: Long)
