import os, json, datetime, time
from flask import Flask, request, jsonify
import requests
from google.cloud import storage, bigquery, secretmanager
from google.cloud import aiplatform as vertex_ai
from vertexai.generative_models import GenerativeModel

app = Flask(__name__)

PROJECT = os.environ["PROJECT_ID"]
BQ_DATASET = os.environ.get("BQ_DATASET", "gh_analytics")
RAW_BUCKET = os.environ["RAW_BUCKET"]

def _github_token():
    sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT}/secrets/GITHUB_TOKEN/versions/latest"
    return sm.access_secret_version(request={"name": name}).payload.data.decode().strip()

def _gh_get(url, params):
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"token {_github_token()}",
        "User-Agent": "gh-analytics-min"
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code == 403 and "rate limit" in r.text.lower():
        time.sleep(60)
        r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def _upload_jsonl(path, records):
    # wrap each record into {"payload": original}
    body = "\n".join(json.dumps({"payload": r}) for r in records)
    storage.Client().bucket(RAW_BUCKET).blob(path).upload_from_string(
        body, content_type="application/json"
    )

def _bq_load_jsonl(table, uri):
    client = bigquery.Client()
    job = client.load_table_from_uri(
        uri,
        f"{PROJECT}.{BQ_DATASET}.{table}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[bigquery.SchemaField("payload", "JSON")],  # <- single JSON column
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
    )
    job.result()

@app.post("/sync")
def sync():
    repo = request.args["repo"]
    since = request.args.get("since")
    since_iso = f"{since}T00:00:00Z" if since else (
        (datetime.datetime.utcnow() - datetime.timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")
    )

    # Issues (filter out PRs that appear here)
    issues, page = [], 1
    while True:
        chunk = _gh_get(f"https://api.github.com/repos/{repo}/issues",
                        {"state":"all","since":since_iso,"per_page":100,"page":page})
        chunk = [c for c in chunk if "pull_request" not in c]
        if not chunk: break
        issues.extend(chunk)
        if len(chunk) < 100: break
        page += 1

    # PRs
    prs, page = [], 1
    while True:
        chunk = _gh_get(f"https://api.github.com/repos/{repo}/pulls",
                        {"state":"all","per_page":100,"page":page})
        if not chunk: break
        prs.extend(chunk)
        if len(chunk) < 100: break
        page += 1

    tag = (since or datetime.date.today().isoformat())
    base = repo.replace("/","_")
    issues_path = f"{base}/issues/{tag}.jsonl"
    prs_path    = f"{base}/prs/{tag}.jsonl"

    _upload_jsonl(issues_path, issues)
    _upload_jsonl(prs_path, prs)

    _bq_load_jsonl("raw_issues", f"gs://{RAW_BUCKET}/{issues_path}")
    _bq_load_jsonl("raw_prs",    f"gs://{RAW_BUCKET}/{prs_path}")

    return jsonify({"repo": repo, "issues": len(issues), "prs": len(prs)})

def _run_sql_string(sql_text: str):
    sql_text = sql_text.replace("PROJECT", PROJECT).replace("DATASET", BQ_DATASET)
    bigquery.Client().query(sql_text).result()

@app.post("/aggregate/daily")
def aggregate_daily():
    _run_sql_string(CURATE_ISSUES_SQL)
    _run_sql_string(WEEKLY_VELOCITY_SQL)
    return jsonify({"status":"aggregated"})

@app.post("/digest/weekly")
def digest_weekly():
    client = bigquery.Client()
    q = """
    WITH wk AS (
    SELECT *
    FROM `PROJECT.DATASET.weekly_velocity`
    WHERE DATE(week_start) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), WEEK(MONDAY))
    ),
    bugs AS (
    SELECT *
    FROM `PROJECT.DATASET.bug_hotspots`
    )
    SELECT TO_JSON_STRING(STRUCT(
    (SELECT ARRAY_AGG(t) FROM wk t) AS weekly_velocity,
    (SELECT ARRAY_AGG(b ORDER BY b.opened_30d DESC LIMIT 15) FROM bugs b) AS bug_hotspots
    )) AS json_report;
    """

    q = q.replace("PROJECT", PROJECT).replace("DATASET", BQ_DATASET)
    rows = list(bigquery.Client().query(q).result())

    if not rows:
        return jsonify({"error":"no data"}), 400
    payload = rows[0]["json_report"]

    vertex_ai.init(project=PROJECT, location="us-central1")
    text = GenerativeModel("gemini-2.0-flash-001").generate_content(
        f"""You are an EM assistant.
Return a 3-5 sentence summary + 3 risks + 3 priorities.
JSON data:
{payload}
"""
    ).candidates[0].content.parts[0].text

    today = datetime.date.today().isoformat()
    path = f"digests/{today}.md"
    storage.Client().bucket(RAW_BUCKET).blob(path).upload_from_string(text, content_type="text/markdown")
    return jsonify({"digest_gcs": f"gs://{RAW_BUCKET}/{path}"})

@app.get("/")
def health():
    return "ok", 200

# --- Inline SQL (keeps it minimal and avoids file path issues) ---

CURATE_ISSUES_SQL = r"""
CREATE SCHEMA IF NOT EXISTS `PROJECT.DATASET`;

CREATE OR REPLACE TABLE `PROJECT.DATASET.curated_issues` AS
SELECT
  JSON_VALUE(payload, '$.id')                AS issue_id,
  JSON_VALUE(payload, '$.number')            AS number,
  JSON_VALUE(payload, '$.state')             AS state,
  JSON_VALUE(payload, '$.title')             AS title,
  JSON_VALUE(payload, '$.user.login')        AS author,
  JSON_VALUE(payload, '$.html_url')          AS html_url,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_VALUE(payload,'$.created_at')) AS created_at,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_VALUE(payload,'$.closed_at'))  AS closed_at,
  ARRAY(
    SELECT JSON_VALUE(l, '$.name')
    FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.labels')) AS l
  ) AS labels
FROM `PROJECT.DATASET.raw_issues`;
"""

WEEKLY_VELOCITY_SQL = r"""
CREATE OR REPLACE TABLE `PROJECT.DATASET.weekly_velocity` AS
SELECT
  DATE_TRUNC(TIMESTAMP_TRUNC(created_at, DAY), WEEK(MONDAY)) AS week_start,
  COUNTIF(state = 'open')                                   AS opened_count,  -- created in that week
  COUNTIF(closed_at IS NOT NULL
          AND DATE_TRUNC(TIMESTAMP_TRUNC(closed_at, DAY), WEEK(MONDAY))
                = DATE_TRUNC(TIMESTAMP_TRUNC(created_at, DAY), WEEK(MONDAY))) AS closed_in_same_week
FROM `PROJECT.DATASET.curated_issues`
WHERE created_at IS NOT NULL
GROUP BY week_start
ORDER BY week_start DESC;
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
