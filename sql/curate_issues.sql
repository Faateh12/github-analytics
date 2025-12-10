CREATE OR REPLACE TABLE `PROJECT.DATASET.issues` AS
SELECT
  JSON_VALUE(j, '$.repository.full_name')                                       AS repo,
  SAFE_CAST(JSON_VALUE(j, '$.number') AS INT64)                                  AS issue_number,
  JSON_VALUE(j, '$.title')                                                       AS title,
  JSON_VALUE(j, '$.state')                                                       AS state,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_VALUE(j, '$.created_at'))    AS created_at,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', NULLIF(JSON_VALUE(j, '$.closed_at'), 'null')) AS closed_at,
  ARRAY(
    SELECT JSON_VALUE(l, '$.name') FROM UNNEST(JSON_QUERY_ARRAY(j, '$.labels')) l
  )                                                                              AS labels,
  JSON_VALUE(j, '$.user.login')                                                  AS author,
  JSON_VALUE(j, '$.assignee.login')                                              AS assignee,
  ARRAY_TO_STRING(
    ARRAY(SELECT JSON_VALUE(l, '$.name') FROM UNNEST(JSON_QUERY_ARRAY(j, '$.labels')) l), ','
  ) LIKE '%bug%'                                                                 AS is_bug,
  ARRAY_TO_STRING(
    ARRAY(SELECT JSON_VALUE(l, '$.name') FROM UNNEST(JSON_QUERY_ARRAY(j, '$.labels')) l), ','
  ) LIKE '%feature%'                                                             AS is_feature
FROM `PROJECT.DATASET.raw_issues` t,
UNNEST([TO_JSON_STRING(t)]) AS j;
