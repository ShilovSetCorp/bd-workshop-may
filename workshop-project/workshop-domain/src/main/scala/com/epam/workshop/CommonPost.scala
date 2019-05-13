package com.epam.workshop

case class CommonPost(id: Long,
                      postType: Long,
                      questionId: Long,
                      creationDate: String,
                      userId: Long,
                      tags: String,
                      score: Long,
                      acceptedAnswerId: Long,
                      favoriteCount: Long)
