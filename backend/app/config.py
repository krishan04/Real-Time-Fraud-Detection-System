import os

# Kafka / SSE
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "fraud_alerts")
CONSUMER_GROUP = os.getenv("BACKEND_CONSUMER_GROUP", "fraud-dashboard-consumer")

# PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "fraud")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraud")
POSTGRES_DB = os.getenv("POSTGRES_DB", "frauddb")

# SSE behaviour
SSE_QUEUE_SIZE = int(os.getenv("SSE_QUEUE_SIZE", "1000"))
SSE_KEEPALIVE_S = float(os.getenv("SSE_KEEPALIVE_S", "15"))