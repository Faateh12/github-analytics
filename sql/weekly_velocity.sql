CREATE OR REPLACE TABLE `PROJECT.DATASET.weekly_velocity` AS
WITH weeks AS (
  SELECT DATE_TRUNC(date, WEEK(MONDAY)) AS week_start, repo,
         SUM(CASE WHEN closed_today>0 THEN closed_today ELSE 0 END) AS issues_closed
  FROM `PROJECT.DATASET.daily_issue_backlog`
  GROUP BY week_start, repo
),
prs AS (
  SELECT DATE_TRUNC(DATE(merged_at), WEEK(MONDAY)) AS week_start, repo, COUNT(*) AS prs_merged
  FROM `PROJECT.DATASET.pull_requests`
  WHERE merged_at IS NOT NULL
  GROUP BY week_start, repo
)
SELECT w.week_start, w.repo, w.issues_closed, COALESCE(p.prs_merged,0) AS prs_merged
FROM weeks w LEFT JOIN prs p USING (week_start, repo)
ORDER BY week_start;
