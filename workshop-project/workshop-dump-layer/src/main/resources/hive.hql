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

--Count
SELECT COUNT(*) FROM common_posts;

--Filter
SELECT common_posts.questionId AS question, common_posts.id AS answer, most_liked.max_score AS max_score FROM
	common_posts
INNER JOIN
	(SELECT questionId, max(score) AS max_score FROM
		(SELECT * FROM common_posts
			WHERE postType = '2') AS answers
	GROUP BY questionId) AS most_liked
ON (common_posts.score = most_liked.max_score
AND common_posts.questionId = most_liked.questionId
AND common_posts.postType = '2')
SORT BY max_score DESC;
