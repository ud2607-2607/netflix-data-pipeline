# 🎬 Netflix Streaming Data Pipeline

Engineered an end-to-end serverless data pipeline on Google Cloud Platform (GCP) to ingest, process, and analyze simulated real-time Netflix user engagement events (heartbeats).

# Architecture
- Data Generation -> publisher.py generates simulated data for user events.
- Ingestion: Used Cloud Pub/Sub for message streaming
- Processing: Used Cloud function triggered by Pub/Sub to clean JSON data and inject metadata into BigQuery
- Storage & Analytics: Architected a STAR schema for the database in BigQuery and wrote SQL scripts to analyze the simulated data.
- Observability: Used Cloud Logging for error handling and tracking. 

## How It Works
- **publisher.py** — generates 50 simulated Netflix watch events (movies + TV shows) and publishes them as JSON to a Pub/Sub topic
- **main.py** — Cloud Function that automatically triggers on each message, parses the JSON, and loads data into BigQuery
- **analytics/** — analytical SQL queries run on top of the BigQuery data

## Tech Stack
- Google Cloud Pub/Sub
- Google Cloud Functions (Python)
- Google BigQuery
- Python, SQL

## Data Model (Star Schema)
- fact_watches table -> watch_id (PK), user_id (FK), content_id (FK), watch_duration, total_duration, completed, is_rewatch, rating, timestamp
- dim_users table -> user_id (PK), age_group, country, device 
- dim_content table -> content_id(PK), content_title, content_type, genre, season, episode 

## Sample Queries
- Total watch events by country
- Average rating by content
- Top 10 most watched titles

## Setup
1. Clone the repo
2. Create GCP project and enable APIs (Pub/Sub, Cloud Functions, BigQuery)
3. Deploy Cloud Function: `gcloud functions deploy process_watch_events --runtime python311 --trigger-topic netflix-watch-events --entry-point process_watch_events --region us-central1`
4. Run publisher: `python3 publisher.py`
