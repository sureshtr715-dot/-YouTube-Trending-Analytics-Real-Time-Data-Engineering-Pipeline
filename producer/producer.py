import json
import time
import os
import requests
from kafka import KafkaProducer

# ===============================
# Config (ENV-based)
# ===============================
API_KEY = os.getenv("YOUTUBE_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "youtube_trending")

if not API_KEY:
    raise RuntimeError("❌ YOUTUBE_API_KEY environment variable not set")

# ===============================
# Kafka Producer
# ===============================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

# ===============================
# YouTube API
# ===============================
def get_trending_videos():
    url = (
        "https://www.googleapis.com/youtube/v3/videos"
        "?part=snippet,statistics"
        "&chart=mostPopular"
        "&maxResults=10"
        "&regionCode=US"
        f"&key={API_KEY}"
    )

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json().get("items", [])

# ===============================
# Streaming Loop
# ===============================
print("🚀 YouTube Kafka Producer started")

while True:
    try:
        videos = get_trending_videos()

        for v in videos:
            payload = {
                "video_id": v.get("id"),
                "title": v["snippet"].get("title"),
                "channel": v["snippet"].get("channelTitle"),
                "published_at": v["snippet"].get("publishedAt"),
                "viewCount": int(v["statistics"].get("viewCount", 0)),
                "likeCount": int(v["statistics"].get("likeCount", 0)),
                "ingested_at": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            producer.send(TOPIC, payload)
            print(f"✅ Sent: {payload['video_id']}")

        producer.flush()
        time.sleep(10)

    except Exception as e:
        print("❌ Producer error:", e)
        time.sleep(5)
