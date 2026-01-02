from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "youtube_trending",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="yt-group"
)

for msg in consumer:
    print(msg.value)
