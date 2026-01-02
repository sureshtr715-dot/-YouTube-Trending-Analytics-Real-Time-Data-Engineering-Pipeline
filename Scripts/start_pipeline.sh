#!/bin/bash

# Fail only on real errors, not wait loops
set -o pipefail

# ===============================
# Project Root (RELATIVE & SAFE)
# ===============================
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
KAFKA_DIR="$PROJECT_DIR/kafka"
LOG_DIR="$PROJECT_DIR/logs"
PYTHON_BIN="$PROJECT_DIR/yt-venv/bin/python"
SPARK_SUBMIT="$PROJECT_DIR/spark/bin/spark-submit"

mkdir -p "$LOG_DIR"

echo "🚀 Starting YouTube Streaming Pipeline"
echo "📁 Project Directory: $PROJECT_DIR"

# ===============================
# Dependency Checks
# ===============================
command -v jps >/dev/null || { echo "❌ jps not found (install JDK)"; exit 1; }
command -v nc >/dev/null || { echo "❌ nc not found (install netcat)"; exit 1; }

# ===============================
# 1. ZooKeeper
# ===============================
echo "➡️ Starting ZooKeeper..."

if jps | grep -q QuorumPeerMain; then
  echo "✅ ZooKeeper already running"
else
  nohup "$KAFKA_DIR/bin/zookeeper-server-start.sh" \
    "$KAFKA_DIR/config/zookeeper.properties" \
    > "$LOG_DIR/zookeeper.log" 2>&1 &

  echo "⏳ Waiting for ZooKeeper (2181)..."
  for _ in {1..30}; do
    nc -z localhost 2181 && break
    sleep 2
  done

  if ! nc -z localhost 2181; then
    echo "❌ ZooKeeper failed — check logs/zookeeper.log"
    exit 1
  fi

  echo "⏳ Verifying ZooKeeper health..."
  for _ in {1..15}; do
    echo ruok | nc localhost 2181 2>/dev/null | grep -q imok && break
    sleep 2
  done

  echo "✅ ZooKeeper is healthy"
fi

# ===============================
# 2. Kafka Broker
# ===============================
echo "➡️ Starting Kafka Broker..."

if jps | grep -q Kafka; then
  echo "✅ Kafka already running"
else
  nohup "$KAFKA_DIR/bin/kafka-server-start.sh" \
    "$KAFKA_DIR/config/server.properties" \
    > "$LOG_DIR/kafka.log" 2>&1 &

  echo "⏳ Waiting for Kafka (9092)..."
  for _ in {1..30}; do
    "$KAFKA_DIR/bin/kafka-topics.sh" \
      --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break
    sleep 2
  done

  echo "✅ Kafka is up"
fi

# ===============================
# 3. Kafka Producer
# ===============================
echo "➡️ Starting Kafka Producer..."

if pgrep -f producer.py >/dev/null; then
  echo "✅ Producer already running"
else
  nohup "$PYTHON_BIN" \
    "$PROJECT_DIR/app/producer.py" \
    > "$LOG_DIR/producer.log" 2>&1 &
  echo "✅ Producer started"
fi

# ===============================
# 4. Spark Streaming Job
# ===============================
echo "➡️ Starting Spark Streaming Job..."

if pgrep -f yt_stream_to_delta.py >/dev/null; then
  echo "✅ Spark streaming already running"
else
  nohup "$SPARK_SUBMIT" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,io.delta:delta-spark_2.12:3.2.0 \
    "$PROJECT_DIR/spark_jobs/yt_stream_to_delta.py" \
    > "$LOG_DIR/spark_stream.log" 2>&1 &
  echo "✅ Spark streaming started"
fi

echo ""
echo "🎯 PIPELINE IS LIVE"
echo "📂 Logs: $LOG_DIR"
