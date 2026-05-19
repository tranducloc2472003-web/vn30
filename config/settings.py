import os

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP      = os.getenv("KAFKA_BOOTSTRAP",  "localhost:29092")  # host machine
KAFKA_BOOTSTRAP_INT  = os.getenv("KAFKA_BOOTSTRAP_INT", "kafka:9092")   # inside Docker
KAFKA_TOPIC_RAW      = "ssi.ticks.raw"
KAFKA_TOPIC_OHLCV    = "ssi.ohlcv.1m"

# ── Cassandra ─────────────────────────────────────────────────────────────────
CASSANDRA_HOSTS      = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_PORT       = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE   = "ssi"

# ── SSI iBroad ────────────────────────────────────────────────────────────────
SSI_URL              = "https://iboard-query.ssi.com.vn/stock/group/VN30"
SSI_DEVICE_ID        = os.getenv("SSI_DEVICE_ID", "6416C7C2-E423-443E-9725-C751AD691C50")
SSI_POLL_INTERVAL    = int(os.getenv("SSI_POLL_INTERVAL", "5"))   # giây

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_MASTER         = os.getenv("SPARK_MASTER", "spark://localhost:7077")
SPARK_APP_NAME       = "SSI-Pipeline"
SPARK_PACKAGES       = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
    "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0"
)

# Cửa sổ OHLCV
OHLCV_WINDOW         = "1 minute"
OHLCV_WATERMARK      = "10 seconds"

# ── Producer ──────────────────────────────────────────────────────────────────
KAFKA_PRODUCER_CONF = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "1",
    "compression.type": "lz4",
    "linger.ms": 200,
    "queue.buffering.max.messages": 100_000,
}