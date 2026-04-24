import re
from google.cloud import bigquery
import anthropic

bq = bigquery.Client(project="netflix-pipeline-490116")
claude = anthropic.Anthropic()

# -------------------------------------------------------
# SCHEMA CONTEXT
# This is what teaches Claude your database structure.
# The more detail here, the better SQL it writes.
# This is the "R" in RAG — giving the model the right
# context to reason over before it answers.
# -------------------------------------------------------

SCHEMA = """
CRITICAL: fact_watches has NO genre, country, device, or age_group columns.
You MUST JOIN to get these:
- genre/content_title/content_type → JOIN dim_content ON content_id
- country/device/age_group → JOIN dim_users ON user_id

You are a data analyst for a Netflix streaming analytics pipeline.
You have access to a BigQuery database with these tables:

TABLE: netflix-pipeline-490116.netflix_dataset.fact_watches
  - content_id       (STRING)  unique id for each piece of content
  - content_type     (STRING)  "Movie" or "TV Show"
  - genre            (STRING)  e.g. "Drama", "Sci-Fi", "Crime"
  - watch_duration_min (FLOAT) how long the user watched in minutes
  - total_duration_min (FLOAT) total length of the content
  - completed        (BOOLEAN) true if user watched 85%+ of content
  - rating           (INTEGER) 1-5 star rating, can be NULL
  - is_rewatch       (BOOLEAN) true if user watched it before
  - timestamp        (TIMESTAMP) when the watch event happened
  - country          (STRING)  e.g. "US", "UK", "India", "Brazil"
  - age_group        (STRING)  e.g. "18-24", "25-34", "35-44"
  - device           (STRING)  e.g. "Smart TV", "Mobile", "Laptop"
  - user_id          (STRING)  unique user identifier

TABLE: netflix-pipeline-490116.netflix_dataset.agg_most_watched_title  (built by dbt)
  - content_title    (STRING)  title name e.g. "The Crown", "Glass Onion"
  - content_type     (STRING)  "Movie" or "TV Show"
  - genre            (STRING)
  - watch_count      (INTEGER) total number of watches

TABLE: netflix-pipeline-490116.netflix_dataset.agg_watches_by_country  (built by dbt)
  - country          (STRING)
  - watch_count      (INTEGER)
  - avg_rating       (FLOAT)
  - avg_watch_duration (FLOAT) average minutes watched
  - rewatch_count    (INTEGER)

RULES FOR WRITING SQL:
- Always use fully qualified table names with backticks
- fact_watches is your main events table with 5000 rows — always start here
- To get genre, content_title, content_type: JOIN fact_watches to dim_content on content_id
- To get country, device, age_group: JOIN fact_watches to dim_users on user_id
- For title rankings use agg_most_watched_title (already aggregated by dbt)
- For country rankings use agg_watches_by_country (already aggregated by dbt)
- For time-based questions use TIMESTAMP_TRUNC(timestamp, MONTH) on fact_watches
- For completion rate: AVG(CASE WHEN completed THEN 1.0 ELSE 0.0 END) * 100
- Always add LIMIT 20 unless user asks for everything
- Never use SELECT * — only select columns needed
- dim_users has duplicate user_ids — always deduplicate it with:
  WITH deduped_users AS (
    SELECT DISTINCT user_id, country, device, age_group 
    FROM `netflix-pipeline-490116.netflix_dataset.dim_users`
  )
  Then JOIN fact_watches to deduped_users instead of dim_users directly
- dim_users has duplicate user_ids — always deduplicate with a CTE:
  WITH deduped_users AS (
    SELECT DISTINCT user_id, country, device, age_group 
    FROM `netflix-pipeline-490116.netflix_dataset.dim_users`
  ),
  deduped_content AS (
    SELECT DISTINCT content_id, content_title, content_type, genre
    FROM `netflix-pipeline-490116.netflix_dataset.dim_content`
  )
  Then JOIN fact_watches to deduped_users and deduped_content instead of the raw dim tables

EXAMPLE JOIN for genre questions:
SELECT dc.genre, COUNT(*) as watch_count
FROM `netflix-pipeline-490116.netflix_dataset.fact_watches` fw
JOIN `netflix-pipeline-490116.netflix_dataset.dim_content` dc ON fw.content_id = dc.content_id
GROUP BY dc.genre ORDER BY watch_count DESC

EXAMPLE JOIN for device questions:
SELECT du.device, COUNT(*) as watch_count
FROM `netflix-pipeline-490116.netflix_dataset.fact_watches` fw
JOIN `netflix-pipeline-490116.netflix_dataset.dim_users` du ON fw.user_id = du.user_id
GROUP BY du.device ORDER BY watch_count DESC
"""

# -------------------------------------------------------
# STEP 1: Claude writes the SQL
# We ask Claude to ONLY return SQL, nothing else.
# This is "Text-to-SQL" — the core of the RAG pattern.
# -------------------------------------------------------

def generate_sql(user_question, conversation_history):
    """Ask Claude to turn a natural language question into SQL."""
    
    messages = conversation_history + [{
        "role": "user",
        "content": f"""Write a BigQuery SQL query to answer this question:

"{user_question}"

Return ONLY the SQL query. No explanation, no markdown, no backticks. Just raw SQL."""
    }]
    
    response = claude.messages.create(
        model="claude-opus-4-6",
        max_tokens=500,
        system=SCHEMA,
        messages=messages
    )
    
    return response.content[0].text.strip()


# -------------------------------------------------------
# STEP 2: Run the SQL against BigQuery
# -------------------------------------------------------

def run_query(sql):
    """Execute SQL and return results as a list of dicts."""
    try:
        rows = bq.query(sql).result()
        return [dict(row) for row in rows], None
    except Exception as e:
        return None, str(e)


# -------------------------------------------------------
# STEP 3: Claude explains the results in plain English
# We pass both the original question AND the raw data
# back to Claude so it can give a human answer.
# -------------------------------------------------------

def explain_results(user_question, sql, results, conversation_history):
    """Ask Claude to explain the query results in plain English."""
    
    messages = conversation_history + [{
        "role": "user", 
        "content": f"""The user asked: "{user_question}"

You ran this SQL:
{sql}

The results were:
{results}

Now answer the user's question in plain English using these results.
Be concise, highlight the most interesting finding, and mention specific numbers."""
    }]
    
    response = claude.messages.create(
        model="claude-opus-4-6",
        max_tokens=500,
        system=SCHEMA,
        messages=messages
    )
    
    return response.content[0].text.strip()


# -------------------------------------------------------
# MAIN CHAT LOOP
# Two Claude calls per question:
#   Call 1 → write SQL
#   Call 2 → explain results
# -------------------------------------------------------

conversation_history = []

print("\n🎬 Netflix Analytics Chatbot (Text-to-SQL)")
print("Ask anything about your streaming data. Type 'quit' to exit.\n")
print("Try: 'which genre is most popular?' or 'what spiked in February?'\n")

while True:
    user_input = input("You: ").strip()
    if user_input.lower() == "quit":
        break
    if not user_input:
        continue

    # Call 1: generate SQL
    print("⏳ Writing SQL...")
    sql = generate_sql(user_input, conversation_history)
    print(f"\n📝 SQL: {sql}\n")

    # Run it against BigQuery
    results, error = run_query(sql)

    if error:
        print(f"❌ Query failed: {error}")
        print("Try rephrasing your question.\n")
        continue

    if not results:
        print("⚠️  Query returned no results.\n")
        continue

    # Call 2: explain the results
    answer = explain_results(user_input, sql, results, conversation_history)
    
    # Save to conversation history so follow-up questions work
    conversation_history.append({"role": "user", "content": user_input})
    conversation_history.append({"role": "assistant", "content": answer})

    print(f"Claude: {answer}\n")