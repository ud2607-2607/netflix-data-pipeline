from google.cloud import pubsub_v1
import json, random, datetime

project_id = "netflix-pipeline-490116"
topic_id = "netflix-watch-events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

TV_SHOWS = [
    {"title": "Stranger Things", "genre": "Sci-Fi", "type": "TV Show", "seasons": 4},
    {"title": "The Crown", "genre": "Drama", "type": "TV Show", "seasons": 6},
    {"title": "Squid Game", "genre": "Thriller", "type": "TV Show", "seasons": 2},
    {"title": "Ozark", "genre": "Crime", "type": "TV Show", "seasons": 4},
    {"title": "Wednesday", "genre": "Comedy", "type": "TV Show", "seasons": 1},
    {"title": "Bridgerton", "genre": "Romance", "type": "TV Show", "seasons": 3},
    {"title": "The Witcher", "genre": "Fantasy", "type": "TV Show", "seasons": 3},
    {"title": "Dark", "genre": "Sci-Fi", "type": "TV Show", "seasons": 3},
]

MOVIES = [
    {"title": "Bird Box", "genre": "Horror", "type": "Movie", "duration_min": 124},
    {"title": "The Irishman", "genre": "Crime", "type": "Movie", "duration_min": 209},
    {"title": "Extraction", "genre": "Action", "type": "Movie", "duration_min": 116},
    {"title": "The Gray Man", "genre": "Action", "type": "Movie", "duration_min": 122},
    {"title": "Don't Look Up", "genre": "Comedy", "type": "Movie", "duration_min": 138},
    {"title": "Hustle", "genre": "Drama", "type": "Movie", "duration_min": 117},
    {"title": "Glass Onion", "genre": "Mystery", "type": "Movie", "duration_min": 139},
    {"title": "All Quiet on the Western Front", "genre": "War", "type": "Movie", "duration_min": 148},
]

COUNTRIES = ["US", "UK", "Canada", "Germany", "Brazil", "India", "Australia", "France"]
DEVICES = ["Smart TV", "Mobile", "Laptop", "Tablet", "Desktop"]
AGE_GROUPS = ["13-17", "18-24", "25-34", "35-44", "45-54", "55+"]

def generate_event():
    content = random.choice(TV_SHOWS + MOVIES)
    is_movie = content["type"] == "Movie"

    if is_movie:
        total_duration = content["duration_min"]
    else:
        total_duration = random.randint(30, 60)

    watch_duration = random.randint(5, total_duration)
    completed = watch_duration >= total_duration * 0.85

    days_ago = random.randint(0, 30)
    hours_ago = random.randint(0, 23)
    timestamp = (datetime.datetime.utcnow()
                 - datetime.timedelta(days=days_ago, hours=hours_ago)).isoformat()

    event = {
        "user_id": f"user_{random.randint(1, 200)}",
        "country": random.choice(COUNTRIES),
        "age_group": random.choice(AGE_GROUPS),
        "device": random.choice(DEVICES),
        "content_title": content["title"],
        "content_type": content["type"],
        "genre": content["genre"],
        "watch_duration_min": watch_duration,
        "total_duration_min": total_duration,
        "completed": completed,
        "rating": random.choice([1, 2, 3, 4, 5, None]),
        "is_rewatch": random.random() < 0.2,
        "timestamp": timestamp
    }

    if not is_movie:
        event["season"] = random.randint(1, content["seasons"])
        event["episode"] = random.randint(1, 10)

    return event

NUM_EVENTS = 50

print(f"🚀 Publishing {NUM_EVENTS} events to {topic_id}...")

# Publish all messages first, collect futures
futures = []
events = []
for i in range(NUM_EVENTS):
    event = generate_event()
    data = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, data)
    futures.append(future)
    events.append(event)
    print(f"📤 [{i+1}/{NUM_EVENTS}] Queued: {event['content_title']} | {event['content_type']} | {event['user_id']} | {event['country']}")

# Now confirm all at once
print("\n⏳ Confirming delivery...")
for i, future in enumerate(futures):
    msg_id = future.result()
    print(f"✅ [{i+1}/{NUM_EVENTS}] Confirmed - Message ID: {msg_id}")

print(f"\n🎉 Done! {NUM_EVENTS} events published successfully to topic: {topic_id}")
