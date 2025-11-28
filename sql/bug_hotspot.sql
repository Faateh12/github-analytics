CREATE OR REPLACE TABLE `PROJECT.DATASET.bug_hotspots` AS
WITH base AS (
  SELECT repo, labels, created_at, closed_at
  FROM `PROJECT.DATASET.issues`
  WHERE ARRAY_TO_STRING(labels, ',') LIKE '%bug%'
    AND created_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY))
),
exploded AS (
  SELECT repo, l AS label, created_at, closed_at
  FROM base, UNNEST(labels) l
)
SELECT repo, label,
  COUNTIF(created_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))) AS opened_30d,
  COUNTIF(closed_at  >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))) AS closed_30d,
  APPROX_QUANTILES(TIMESTAMP_DIFF(closed_at, created_at, DAY), 2)[OFFSET(1)] AS median_close_days
FROM exploded
WHERE label LIKE '%bug%'
GROUP BY repo, label
ORDER BY opened_30d DESC;
