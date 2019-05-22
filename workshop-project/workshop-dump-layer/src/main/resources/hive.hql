--Create
DROP TABLE IF EXISTS common_posts;
CREATE EXTERNAL TABLE common_posts (
	id STRING,
	postType STRING,
	questionId STRING,
	creationDate STRING,
	userId STRING,
	tags STRING,
	score STRING,
	acceptedAnswerId STRING,
	favoriteCount STRING)
STORED AS PARQUET
LOCATION '/user/workshop/output/common';

--Filter
SELECT * FROM
	common_posts
INNER JOIN
	(SELECT questionId, max(score) AS max_score FROM
		(SELECT * FROM common_posts
			WHERE postType = '2') AS answers
	GROUP BY questionId) AS most_liked
ON common_posts.questionId = most_liked.questionId
