import base64
import json
import uuid 
from google.cloud import bigquery


# initialize the big query client
# client = bigquery.Client(location="US")
client = bigquery.Client(project="netflix-pipeline-490116")


PROJECT_ID = "netflix-pipeline-490116"
DATASET_ID = "netflix_dataset"

def process_watch_events(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic."""
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(f"Received event: {json.dumps(data, indent=2)}")

    #generate unique watch_id for each event
    watch_id = str(uuid.uuid4())
    content_id = f"{data['content_title'].replace(' ', '_').lower()}_{data.get('season', 'movie')}_{data.get('episode', '0')}"

    # Prepare the row to insert into BigQuery
    user_row = {
        "user_id": data["user_id"],
        "age_group": data.get("age_group"),
        "country": data.get("country"),
        "device": data.get("device")
    }
    insert_row(f"{PROJECT_ID}.{DATASET_ID}.dim_users", user_row)

    content_row = {
        "content_id": content_id,
        "content_title": data["content_title"],
        "content_type": data["content_type"],
        "genre": data["genre"], 
        "season": data.get("season"),
        "episode": data.get("episode")
    }
    insert_row(f"{PROJECT_ID}.{DATASET_ID}.dim_content", content_row)

    fact_row = {
        "watch_id": watch_id,
        "user_id": data["user_id"],
        "content_id": content_id,
        "watch_duration_min": data["watch_duration_min"],
        "total_duration_min": data["total_duration_min"],
        "completed": data["completed"],
        "rating": data["rating"],
        "is_rewatch": data["is_rewatch"],
        "timestamp": data["timestamp"]
    }
    insert_row(f"{PROJECT_ID}.{DATASET_ID}.fact_watches", fact_row)
    print(f"Printed watch id: {watch_id}")

def insert_row(table_id, row):
    table = client.get_table(table_id)
    errors = client.insert_rows_json(table, [row])
    if errors:
        print(f"Error inserting row into {table_id}: {errors}")
    else:
        print(f"Successfully inserted row into {table_id}")

