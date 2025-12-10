CREATE OR REPLACE TABLE `PROJECT.DATASET.pull_requests` AS
SELECT
  JSON_VALUE(j, '$.base.repo.full_name')                                         AS repo,
  SAFE_CAST(JSON_VALUE(j, '$.number') AS INT64)                                   AS pr_number,
  JSON_VALUE(j, '$.title')                                                       AS title,
  JSON_VALUE(j, '$.state')                                                       AS state,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_VALUE(j, '$.created_at'))    AS created_at,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', NULLIF(JSON_VALUE(j, '$.merged_at'), 'null')) AS merged_at,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', NULLIF(JSON_VALUE(j, '$.closed_at'), 'null')) AS closed_at,
  JSON_VALUE(j, '$.user.login')                                                  AS author,
  SAFE_CAST(JSON_VALUE(j, '$.additions') AS INT64)                                AS additions,
  SAFE_CAST(JSON_VALUE(j, '$.deletions') AS INT64)                                AS deletions,
  SAFE_CAST(JSON_VALUE(j, '$.changed_files') AS INT64)                            AS changed_files,
  NULL                                                                            AS linked_issue
FROM `PROJECT.DATASET.raw_prs` t,
UNNEST([TO_JSON_STRING(t)]) AS j;
