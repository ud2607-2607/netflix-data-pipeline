from flask import Flask, request, jsonify, render_template
from google.cloud import bigquery
import anthropic

app = Flask(__name__)
bq = bigquery.Client(project="netflix-pipeline-490116")
claude = anthropic.Anthropic()

SCHEMA = """
CRITICAL: fact_watches has NO genre, country, device, or age_group columns.
You MUST JOIN to get these:
- genre/content_title/content_type → JOIN dim_content ON content_id
- country/device/age_group → JOIN dim_users ON user_id

You are a data analyst for a Netflix streaming analytics pipeline.

TABLE: netflix-pipeline-490116.netflix_dataset.fact_watches
  - watch_id, user_id, content_id, watch_duration_min,
    total_duration_min, completed, is_rewatch, rating, timestamp

TABLE: netflix-pipeline-490116.netflix_dataset.agg_most_watched_title (dbt)
  - content_title, content_type, genre, watch_count

TABLE: netflix-pipeline-490116.netflix_dataset.agg_watches_by_country (dbt)
  - country, watch_count, avg_rating, avg_watch_duration, rewatch_count

TABLE: netflix-pipeline-490116.netflix_dataset.dim_content
  - content_id, content_title, content_type, genre, season, episode

TABLE: netflix-pipeline-490116.netflix_dataset.dim_users
  - user_id, age_group, country, device

RULES FOR WRITING SQL:
- Always use fully qualified table names with backticks
- fact_watches is your main events table with 5000 rows
- For title rankings use agg_most_watched_title
- For country rankings use agg_watches_by_country
- For genre/content questions JOIN dim_content ON content_id
- For device/country/age questions JOIN dim_users ON user_id
- dim_users and dim_content have duplicates — always deduplicate:
  WITH deduped_users AS (
    SELECT DISTINCT user_id, country, device, age_group
    FROM `netflix-pipeline-490116.netflix_dataset.dim_users`
  ),
  deduped_content AS (
    SELECT DISTINCT content_id, content_title, content_type, genre
    FROM `netflix-pipeline-490116.netflix_dataset.dim_content`
  )
- For time questions use TIMESTAMP_TRUNC(timestamp, MONTH) on fact_watches
- Always LIMIT 20 unless asked for everything
- Never SELECT *
"""

conversation_history = []

# This method generates SQL using Claude, runs it against BigQuery, and returns results or errors
def generate_sql(question):
    messages = conversation_history + [{
        "role": "user",
        "content": f'Write a BigQuery SQL query to answer: "{question}"\nReturn ONLY raw SQL, no explanation, no markdown.'
    }]
    response = claude.messages.create(
        model="claude-opus-4-6",
        max_tokens=500,
        system=SCHEMA,
        messages=messages
    )
    return response.content[0].text.strip()

# runs it against BigQuery, and returns results or errors as a list of dictionaries
def run_query(sql):
    try:
        rows = bq.query(sql).result()
        return [dict(row) for row in rows], None
    except Exception as e:
        return None, str(e)

#This method takes in the BigQuery results and generates a plain English explanation using Claude
def explain_results(question, sql, results):
    messages = conversation_history + [{
        "role": "user",
        "content": f'Question: "{question}"\nSQL: {sql}\nResults: {results}\nAnswer in plain English, be concise, highlight key numbers.'
    }]
    response = claude.messages.create(
        model="claude-opus-4-6",
        max_tokens=500,
        system=SCHEMA,
        messages=messages
    )
    return response.content[0].text.strip()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/ask", methods=["POST"])
def ask():
    data = request.json
    question = data.get("question", "").strip()
    if not question:
        return jsonify({"error": "No question provided"}), 400

    sql = generate_sql(question)
    results, error = run_query(sql)

    if error:
        return jsonify({"error": f"Query failed: {error}", "sql": sql})

    if not results:
        return jsonify({"answer": "No results found.", "sql": sql})

    answer = explain_results(question, sql, results)
    conversation_history.append({"role": "user", "content": question})
    conversation_history.append({"role": "assistant", "content": answer})

    return jsonify({"answer": answer, "sql": sql})


if __name__ == "__main__":
    app.run(debug=True, port=5000)