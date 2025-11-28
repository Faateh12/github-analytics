import os, json, datetime, time, requests
from flask import Flask, request, jsonify
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
    return sm.access_secret_version(request={"name": name}).payload.data.decode()

def _gh_get(url, params):
    r = requests.get(
        url,
        headers={"Authorization": f"token {_github_token()}",
                 "Accept": "application/vnd.github+json"},
        params=params, timeout=30
    )
    if r.status_code == 403 and "rate limit" in r.text.lower():
        time.sleep(60)  # simple backoff
        r = requests.get(url, headers={"Authorization": f"token {_github_token()}"}, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def _upload_jsonl(path, records):
    storage.Client().bucket(RAW_BUCKET).blob(path).upload_from_string(
        "\n".join(json.dumps(r) for r in records), content_type="application/json"
    )

def _bq_load_jsonl(table, uri):
    client = bigquery.Client()
    job = client.load_table_from_uri(
        uri,
        f"{PROJECT}.{BQ_DATASET}.{table}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
    )
    job.result()

@app.post("/sync")
def sync():
    """Pull issues (no PRs) and PRs since a date; write to GCS; load to BigQuery raw_*."""
    repo = request.args["repo"]  # owner/name
    since = request.args.get("since")  # YYYY-MM-DD
    since_iso = (since+"T00:00:00Z") if since else (
        (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z"))

    # Issues (exclude PRs)
    issues, page = [], 1
    while True:
        chunk = _gh_get(f"https://api.github.com/repos/{repo}/issues",
                        {"state":"all","since":since_iso,"per_page":100,"page":page})
        chunk = [c for c in chunk if "pull_request" not in c]
        if not chunk: break
        issues.extend(chunk)
        if len(chunk)<100: break
        page += 1

    # PRs
    prs, page = [], 1
    while True:
        chunk = _gh_get(f"https://api.github.com/repos/{repo}/pulls",
                        {"state":"all","per_page":100,"page":page})
        if not chunk: break
        prs.extend(chunk)
        if len(chunk)<100: break
        page += 1

    date_tag = (since or datetime.date.today().isoformat())
    base = repo.replace("/","_")
    issues_path = f"{base}/issues/{date_tag}.jsonl"
    prs_path    = f"{base}/prs/{date_tag}.jsonl"

    _upload_jsonl(issues_path, issues)
    _upload_jsonl(prs_path, prs)

    _bq_load_jsonl("raw_issues", f"gs://{RAW_BUCKET}/{issues_path}")
    _bq_load_jsonl("raw_prs",    f"gs://{RAW_BUCKET}/{prs_path}")

    return jsonify({"repo":repo,"issues":len(issues),"prs":len(prs)})

def _run_sql(path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    sql = sql.replace("PROJECT", PROJECT).replace("DATASET", BQ_DATASET)
    client = bigquery.Client()
    client.query(sql).result()

@app.post("/aggregate/daily")
def aggregate_daily():
    _run_sql("sql/curate_issues.sql")
    _run_sql("sql/curate_prs.sql")
    _run_sql("sql/daily_backlog.sql")
    _run_sql("sql/weekly_velocity.sql")
    # enable later if desired:
    # _run_sql("sql/bug_hotspots.sql")
    # _run_sql("sql/pr_cycle_times.sql")
    return jsonify({"status":"aggregated"})

@app.post("/digest/weekly")
def digest_weekly():
    client = bigquery.Client()
    q = """
        WITH wk AS (
        SELECT * FROM `PROJECT.DATASET.weekly_velocity`
        WHERE week_start = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), WEEK(MONDAY))
        ),
        bugs AS (
        SELECT * FROM `PROJECT.DATASET.bug_hotspots`
        )
        SELECT TO_JSON_STRING((
        SELECT AS STRUCT
            (SELECT ARRAY_AGG(t) FROM wk t) AS weekly_velocity,
            (SELECT ARRAY_AGG(b) FROM bugs b ORDER BY b.opened_30d DESC LIMIT 15) AS bug_hotspots
        )) AS json_report;
        """
    q = q.replace("PROJECT", PROJECT).replace("DATASET", BQ_DATASET)
    rows = list(client.query(q).result())
    if not rows:
        return jsonify({"error":"no data"}), 400
    payload = rows[0]["json_report"]

    vertex_ai.init(project=PROJECT, location="us-central1")
    model = GenerativeModel("gemini-1.5-pro")
    prompt = f"""You are a pragmatic engineering manager assistant.
    Return:
    1) ~150-word summary, 2) 3 risks (short), 3) 3 priorities (short).
    JSON:
    {payload}
    """
    resp = model.generate_content(prompt)
    text = resp.candidates[0].content.parts[0].text

    today = datetime.date.today().isoformat()
    path = f"digests/{today}.md"
    storage.Client().bucket(RAW_BUCKET).blob(path).upload_from_string(text, content_type="text/markdown")
    return jsonify({"digest_gcs": f"gs://{RAW_BUCKET}/{path}"})

@app.get("/")
def health():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
