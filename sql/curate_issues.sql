CREATE OR REPLACE TABLE `PROJECT.DATASET.issues` AS
SELECT
  JSON_VALUE(payload, '$.repository.full_name') AS repo,
  CAST(JSON_VALUE(payload, '$.number') AS INT64) AS issue_number,
  JSON_VALUE(payload, '$.title') AS title,
  JSON_VALUE(payload, '$.state') AS state,
  TIMESTAMP(JSON_VALUE(payload, '$.created_at')) AS created_at,
  TIMESTAMP(NULLIF(JSON_VALUE(payload, '$.closed_at'), 'null')) AS closed_at,
  (SELECT ARRAY_AGG(JSON_VALUE(l, '$.name')) FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.labels')) l) AS labels,
  JSON_VALUE(payload, '$.user.login') AS author,
  JSON_VALUE(payload, '$.assignee.login') AS assignee,
  ARRAY_TO_STRING((SELECT ARRAY_AGG(JSON_VALUE(l, '$.name')) FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.labels')) l), ',') LIKE '%bug%' AS is_bug,
  ARRAY_TO_STRING((SELECT ARRAY_AGG(JSON_VALUE(l, '$.name')) FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.labels')) l), ',') LIKE '%feature%' AS is_feature
FROM `PROJECT.DATASET.raw_issues`;
