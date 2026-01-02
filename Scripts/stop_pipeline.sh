#!/bin/bash

echo "🛑 Stopping YouTube Streaming Pipeline..."

# Kill Spark Streaming
echo "➡️ Stopping Spark Streaming..."
pkill -f yt_stream_to_delta.py && echo "✓ Spark Streaming stopped" || echo "⚠️ Spark not running"

# Kill Kafka Producer
echo "➡️ Stopping Kafka Producer..."
pkill -f producer.py && echo "✓ Producer stopped" || echo "⚠️ Producer not running"

# Stop Kafka Broker
echo "➡️ Stopping Kafka Broker..."
pkill -f kafka.Kafka && echo "✓ Kafka stopped" || echo "⚠️ Kafka not running"

# Stop ZooKeeper
echo "➡️ Stopping ZooKeeper..."
pkill -f QuorumPeerMain && echo "✓ ZooKeeper stopped" || echo "⚠️ ZooKeeper not running"

echo "✅ PIPELINE STOPPED"
