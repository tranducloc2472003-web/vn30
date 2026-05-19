"""
spark_batch.py
Đọc OHLCV từ Cassandra → tính features kỹ thuật → ghi vào bảng features.

Chạy thủ công hoặc schedule bằng cron / Airflow:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \
      processing/spark_batch.py --date 2026-05-18
"""
import sys, os, argparse
from datetime import date
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE, SPARK_APP_NAME,
)

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType


# ── Spark session ─────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
            .appName(f"{SPARK_APP_NAME}-Batch")
            .config("spark.cassandra.connection.host", ",".join(CASSANDRA_HOSTS))
            .config("spark.cassandra.connection.port", str(CASSANDRA_PORT))
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
    )


# ── Đọc OHLCV từ Cassandra ────────────────────────────────────────────────────
def load_ohlcv(spark: SparkSession, trade_date: str):
    return (
        spark.read
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", CASSANDRA_KEYSPACE)
            .option("table", "ohlcv_1m")
            .load()
            .filter(F.col("trade_date") == trade_date)
    )


# ── Feature engineering ───────────────────────────────────────────────────────
def compute_features(df):
    """
    Tính các chỉ báo kỹ thuật phổ biến:
      - MA 20, MA 50
      - RSI 14
      - Bollinger Bands (20, 2σ)
      - VWAP (đã có từ streaming, tính lại cộng dồn trong ngày)
    """
    w_sym = Window.partitionBy("symbol", "trading_date").orderBy("window_ts")

    # ── Moving Averages ───────────────────────────────────────────────────────
    w_20 = w_sym.rowsBetween(-19, 0)
    w_50 = w_sym.rowsBetween(-49, 0)

    df = (
        df
        .withColumn("ma_20", F.avg("close").over(w_20))
        .withColumn("ma_50", F.avg("close").over(w_50))
    )

    # ── Bollinger Bands (20 chu kỳ, 2 độ lệch chuẩn) ─────────────────────────
    df = (
        df
        .withColumn("bb_std", F.stddev("close").over(w_20))
        .withColumn("bb_upper", F.col("ma_20") + 2 * F.col("bb_std"))
        .withColumn("bb_lower", F.col("ma_20") - 2 * F.col("bb_std"))
        .drop("bb_std")
    )

    # ── RSI 14 ────────────────────────────────────────────────────────────────
    # Bước 1: tính delta giá
    w_prev = w_sym.rowsBetween(-1, -1)
    df = df.withColumn("prev_close", F.first("close").over(w_prev))
    df = df.withColumn("delta", F.col("close") - F.col("prev_close"))

    # Bước 2: tách gain / loss
    df = (
        df
        .withColumn("gain", F.when(F.col("delta") > 0, F.col("delta")).otherwise(0.0))
        .withColumn("loss", F.when(F.col("delta") < 0, -F.col("delta")).otherwise(0.0))
    )

    # Bước 3: avg gain / loss qua 14 chu kỳ
    w_14 = w_sym.rowsBetween(-13, 0)
    df = (
        df
        .withColumn("avg_gain", F.avg("gain").over(w_14))
        .withColumn("avg_loss", F.avg("loss").over(w_14))
    )

    # Bước 4: RSI
    df = (
        df
        .withColumn(
            "rsi_14",
            F.when(
                F.col("avg_loss") == 0, 100.0
            ).otherwise(
                100.0 - (100.0 / (1.0 + F.col("avg_gain") / F.col("avg_loss")))
            )
        )
        .drop("prev_close", "delta", "gain", "loss", "avg_gain", "avg_loss")
    )

    # ── VWAP cộng dồn trong ngày ──────────────────────────────────────────────
    w_cum = w_sym.rowsBetween(Window.unboundedPreceding, 0)
    df = (
        df
        .withColumn(
            "vwap",
            F.sum(F.col("close") * F.col("volume")).over(w_cum) /
            F.sum("volume").over(w_cum)
        )
    )

    return df


# ── Ghi vào Cassandra features ────────────────────────────────────────────────
def write_features(df, trade_date: str):
    feature_cols = [
        "symbol", "trading_date", "window_ts",
        "rsi_14", "ma_20", "ma_50", "vwap",
        "bb_upper", "bb_lower",
    ]
    (
        df.select(*feature_cols)
            .write
            .format("org.apache.spark.sql.cassandra")
            .mode("append")
            .option("keyspace", CASSANDRA_KEYSPACE)
            .option("table", "features")
            .save()
    )
    print(f"Features written for {trade_date}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="Ngày xử lý, format YYYY-MM-DD (mặc định: hôm nay)")
    args = parser.parse_args()
    trade_date = args.date

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Batch] Đang tính features cho ngày: {trade_date}")
    ohlcv = load_ohlcv(spark, trade_date)
    row_count = ohlcv.count()
    print(f"[Batch] Loaded {row_count} OHLCV rows")

    if row_count == 0:
        print("[Batch] Không có dữ liệu — thoát.")
        spark.stop()
        return

    features = compute_features(ohlcv)
    write_features(features, trade_date)

    spark.stop()
    print("[Batch] Hoàn tất.")


if __name__ == "__main__":
    main()