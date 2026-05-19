"""
spark_streaming.py
Đọc tick data từ Kafka → tính OHLCV theo window 1 phút → ghi vào Cassandra.

Chạy:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \
      processing/spark_streaming.py
"""
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    KAFKA_BOOTSTRAP_INT, KAFKA_TOPIC_RAW,
    CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE,
    OHLCV_WINDOW, OHLCV_WATERMARK,
    SPARK_APP_NAME,
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType,
)

# ── Schema của message JSON trong Kafka ───────────────────────────────────────
TICK_SCHEMA = StructType([
    StructField("symbol",        StringType(),    True),
    StructField("exchange",      StringType(),    True),
    StructField("trading_date",  StringType(),    True),
    StructField("crawled_at",    StringType(),    True),
    StructField("session",       StringType(),    True),
    StructField("trading_status",StringType(),    True),
    StructField("ref_price",     DoubleType(),    True),
    StructField("ceiling",       DoubleType(),    True),
    StructField("floor",         DoubleType(),    True),
    StructField("open_price",    DoubleType(),    True),
    StructField("matched_price", DoubleType(),    True),
    StructField("highest",       DoubleType(),    True),
    StructField("lowest",        DoubleType(),    True),
    StructField("avg_price",     DoubleType(),    True),
    StructField("prior_close",   DoubleType(),    True),
    StructField("price_change",  DoubleType(),    True),
    StructField("price_change_pct", DoubleType(), True),
    StructField("matched_volume",DoubleType(),    True),
    StructField("total_volume",  DoubleType(),    True),
    StructField("total_value",   DoubleType(),    True),
    StructField("foreign_buy_vol",  DoubleType(), True),
    StructField("foreign_sell_vol", DoubleType(), True),
    StructField("foreign_room",  DoubleType(),    True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
            .appName(f"{SPARK_APP_NAME}-Streaming")
            .config("spark.cassandra.connection.host", ",".join(CASSANDRA_HOSTS))
            .config("spark.cassandra.connection.port", str(CASSANDRA_PORT))
            .config("spark.sql.shuffle.partitions", "4")   # nhỏ gọn cho dev
            .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ── Đọc từ Kafka ──────────────────────────────────────────────────────────
    raw = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_INT)
            .option("subscribe", KAFKA_TOPIC_RAW)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
    )

    # ── Parse JSON ────────────────────────────────────────────────────────────
    ticks = (
        raw
        .select(F.from_json(F.col("value").cast("string"), TICK_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("ts", F.to_timestamp("crawled_at"))   # event time
        .withWatermark("ts", OHLCV_WATERMARK)
    )

    # ── OHLCV aggregation theo window 1 phút ──────────────────────────────────
    ohlcv = (
        ticks
        .groupBy(
            F.window("ts", OHLCV_WINDOW),
            F.col("symbol"),
            F.col("exchange"),
            F.col("trading_date"),
        )
        .agg(
            F.first("matched_price").alias("open"),
            F.max("matched_price").alias("high"),
            F.min("matched_price").alias("low"),
            F.last("matched_price").alias("close"),
            F.sum("matched_volume").alias("volume"),
            # VWAP = sum(price * vol) / sum(vol)
            (F.sum(F.col("matched_price") * F.col("matched_volume")) /
             F.sum("matched_volume")).alias("vwap"),
        )
        .select(
            F.col("symbol"),
            F.col("trading_date"),
            F.col("window.start").alias("window_ts"),
            F.col("exchange"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("volume").cast(LongType()),
            F.col("vwap"),
        )
    )

    # ── Ghi vào Cassandra ─────────────────────────────────────────────────────
    def write_to_cassandra(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        (
            batch_df.write
                .format("org.apache.spark.sql.cassandra")
                .mode("append")
                .option("keyspace", CASSANDRA_KEYSPACE)
                .option("table", "ohlcv_1m")
                .save()
        )
        print(f"[Batch {batch_id}] Wrote {batch_df.count()} OHLCV rows to Cassandra")

    query = (
        ohlcv.writeStream
            .outputMode("update")
            .foreachBatch(write_to_cassandra)
            .option("checkpointLocation", "/tmp/spark-checkpoint/ohlcv")
            .trigger(processingTime="30 seconds")
            .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()