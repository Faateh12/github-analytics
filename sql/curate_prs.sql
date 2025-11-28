CREATE OR REPLACE TABLE `PROJECT.DATASET.pull_requests` AS
SELECT
  JSON_VALUE(payload, '$.base.repo.full_name') AS repo,
  CAST(JSON_VALUE(payload, '$.number') AS INT64) AS pr_number,
  JSON_VALUE(payload, '$.title') AS title,
  JSON_VALUE(payload, '$.state') AS state,
  TIMESTAMP(JSON_VALUE(payload, '$.created_at')) AS created_at,
  TIMESTAMP(NULLIF(JSON_VALUE(payload, '$.merged_at'), 'null')) AS merged_at,
  TIMESTAMP(NULLIF(JSON_VALUE(payload, '$.closed_at'), 'null')) AS closed_at,
  JSON_VALUE(payload, '$.user.login') AS author,
  CAST(JSON_VALUE(payload, '$.additions') AS INT64) AS additions,
  CAST(JSON_VALUE(payload, '$.deletions') AS INT64) AS deletions,
  CAST(JSON_VALUE(payload, '$.changed_files') AS INT64) AS changed_files,
  NULL AS linked_issue
FROM `PROJECT.DATASET.raw_prs`;
