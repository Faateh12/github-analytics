CREATE OR REPLACE TABLE `PROJECT.DATASET.pr_cycle_times` AS
SELECT repo, pr_number, created_at, merged_at,
  SAFE_DIVIDE(TIMESTAMP_DIFF(merged_at, created_at, HOUR), 24.0) AS cycle_days
FROM `PROJECT.DATASET.pull_requests`
WHERE merged_at IS NOT NULL;
