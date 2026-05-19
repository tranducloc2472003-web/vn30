# SSI iBroad Realtime Data Pipeline

Hệ thống pipeline realtime crawl dữ liệu chứng khoán VN30 từ SSI iBroad, xử lý bằng Kafka + Spark Streaming, lưu trữ Cassandra và visualize trên Grafana.

## Architecture

```
SSI iBroad API
      │
      ▼
 Kafka Producer  ──►  Kafka (ssi.ticks.raw)
                              │
                              ▼
                    Spark Structured Streaming
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
             Cassandra               Cassandra
             ohlcv_1m                features
                    │                    │
                    └─────────┬──────────┘
                              ▼
                           Grafana
                        (localhost:3000)
```

## Stack

| Service | Image | Port |
|---|---|---|
| Zookeeper | confluentinc/cp-zookeeper:7.6.0 | 2181 |
| Kafka | confluentinc/cp-kafka:7.6.0 | 9092 / 29092 |
| Kafka UI | provectuslabs/kafka-ui | 8080 |
| Cassandra | cassandra:4.1 | 9042 |
| Spark Master | bde2020/spark-master:3.3.0-hadoop3.3 | 7077 / 8081 |
| Spark Worker | bde2020/spark-worker:3.3.0-hadoop3.3 | 8082 |
| Grafana | grafana/grafana:10.4.0 | 3000 |

## Cấu trúc project

```
ssi-pipeline/
├── docker-compose.yml
├── cassandra-init.cql
├── requirements.txt
├── config/
│   └── settings.py           # cấu hình tập trung
├── ingestion/
│   ├── producer.py           # crawl SSI iBroad thật
│   └── mock_producer.py      # sinh mock data để test
├── processing/
│   ├── spark_streaming.py    # Kafka → OHLCV 1m → Cassandra
│   └── spark_batch.py        # tính RSI, MA, VWAP, BB → features
└── grafana/
    └── provisioning/
        └── datasources/
            └── cassandra.yml
```

## Cài đặt & Chạy

### 1. Yêu cầu

- Docker Desktop
- Python 3.11
- Git

### 2. Clone & cài dependencies

```bash
git clone https://github.com/tranducloc2472003-web/vn30.git
pip install -r requirements.txt
```

### 3. Khởi động stack

```bash
docker-compose up -d
```

Chờ ~60 giây để Cassandra healthy, kiểm tra:

```bash
docker-compose ps
```

### 4. Tạo schema Cassandra

```bash
docker exec -it cassandra cqlsh
```

```sql
CREATE KEYSPACE IF NOT EXISTS ssi
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE ssi;

CREATE TABLE IF NOT EXISTS ohlcv_1m (
  symbol       text,
  trading_date text,
  window_ts    timestamp,
  exchange     text,
  open         double,
  high         double,
  low          double,
  close        double,
  volume       bigint,
  vwap         double,
  PRIMARY KEY ((symbol, trading_date), window_ts)
) WITH CLUSTERING ORDER BY (window_ts DESC);

CREATE TABLE IF NOT EXISTS features (
  symbol       text,
  trading_date text,
  window_ts    timestamp,
  rsi_14       double,
  ma_20        double,
  ma_50        double,
  vwap         double,
  bb_upper     double,
  bb_lower     double,
  PRIMARY KEY ((symbol, trading_date), window_ts)
) WITH CLUSTERING ORDER BY (window_ts DESC);
```

### 5. Chạy producer

```bash
# Test với mock data (không cần tài khoản SSI)
python ingestion/mock_producer.py

# Crawl SSI thật
python ingestion/producer.py
```

### 6. Chạy Spark Streaming

```bash
# Copy file vào container
docker exec spark-master mkdir -p /opt/config
docker cp processing/spark_streaming.py spark-master:/opt/spark_streaming.py
docker cp config/settings.py spark-master:/opt/config/settings.py

# Fix Cassandra host
docker exec spark-master bash -c "sed -i 's/localhost/cassandra/g' /opt/config/settings.py"

# Submit job
docker exec spark-master bash -c "/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/config/settings.py \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \
  /opt/spark_streaming.py"
```

### 7. Xem dashboard

| URL | Service | Login |
|---|---|---|
| http://localhost:3000 | Grafana | admin / admin |
| http://localhost:8080 | Kafka UI | — |
| http://localhost:8081 | Spark Master | — |

**Tạo dashboard Grafana:**
1. Connections → Data sources → Cassandra-SSI → Build a dashboard
2. Keyspace: `ssi` / Table: `ohlcv_1m` / Time Column: `window_ts` / Value Column: `close`
3. Set auto-refresh: 5s

### 8. Chạy Spark Batch (tính features)

```bash
docker exec spark-master bash -c "/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/config/settings.py \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \
  /opt/spark_batch.py --date $(date +%Y-%m-%d)"
```

## Dừng stack

```bash
# Dừng nhưng giữ data
docker-compose stop

# Dừng và xóa toàn bộ (kể cả data)
docker-compose down -v
```

## Cassandra Schema

| Table | Mô tả |
|---|---|
| `ohlcv_1m` | Nến 1 phút: open, high, low, close, volume, vwap |
| `features` | Chỉ báo kỹ thuật: RSI 14, MA 20/50, Bollinger Bands |
<img width="1099" height="340" alt="image" src="https://github.com/user-attachments/assets/bf564507-072c-4bdd-be0a-8dc4caee93cb" />

## Dữ liệu crawl

Endpoint SSI iBroad: `https://iboard-query.ssi.com.vn/stock/group/VN30`

Danh sách mã VN30 được fetch động từ SSI khi khởi động — tự động cập nhật khi HOSE review rổ (tháng 1 và tháng 7 hàng năm).

## Notes

- Spark Streaming trigger mỗi **30 giây**, tính OHLCV window **1 phút**
- Producer poll SSI mỗi **5 giây** (configurable qua `SSI_POLL_INTERVAL`)
- Data Cassandra tự xóa sau **30 ngày** (TTL)
- Kafka retention: **24 giờ**

# THÀNH QUẢ:

<img width="1919" height="943" alt="image" src="https://github.com/user-attachments/assets/4c8d307f-da86-4833-9135-f276f5aaeaee" />

