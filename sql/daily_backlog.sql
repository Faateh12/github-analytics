CREATE OR REPLACE TABLE `PROJECT.DATASET.daily_issue_backlog` AS
WITH calendar AS (
  SELECT DATE(day) d
  FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', CURRENT_DATE())) day
),
expanded AS (
  SELECT i.repo, c.d,
    CASE WHEN DATE(i.created_at) = c.d THEN 1 ELSE 0 END AS opened_today,
    CASE WHEN DATE(i.closed_at)  = c.d THEN 1 ELSE 0 END AS closed_today,
    CASE WHEN i.created_at <= TIMESTAMP(c.d) AND (i.closed_at IS NULL OR DATE(i.closed_at) > c.d) THEN 1 ELSE 0 END AS is_open
  FROM `PROJECT.DATASET.issues` i
  JOIN calendar c
)
SELECT d AS date, repo,
  SUM(opened_today) AS opened_today,
  SUM(closed_today) AS closed_today,
  SUM(is_open) AS open_issues
FROM expanded
GROUP BY date, repo
ORDER BY date;
