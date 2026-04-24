from google.cloud import bigquery

# This connects to your project
client = bigquery.Client(project="netflix-pipeline-490116")

# A simple summary query — top 5 most watched titles
query = """
    SELECT 
        content_id,
        COUNT(*) as watch_count,
        ROUND(AVG(watch_duration_min), 1) as avg_watch_mins,
        ROUND(AVG(CASE WHEN completed THEN 1.0 ELSE 0.0 END) * 100, 1) as completion_rate_pct
    FROM `netflix-pipeline-490116.netflix_dataset.fact_watches`
    GROUP BY content_id
    ORDER BY watch_count DESC
    LIMIT 5
"""

print("Running query against BigQuery...")
results = client.query(query).result()

print("\nTop 5 most watched titles:\n")
for row in results:
    print(f"  {row.content_id}: {row.watch_count} watches, "
          f"{row.avg_watch_mins} avg mins, "
          f"{row.completion_rate_pct}% completed")