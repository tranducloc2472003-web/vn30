"""
mock_producer.py
Sinh random tick data cho 30 mã VN30 và push vào Kafka.
Dùng để test pipeline mà không cần tài khoản SSI.
"""
import json
import time
import random
import logging
from datetime import datetime, date
from confluent_kafka import Producer
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config.settings import KAFKA_TOPIC_RAW, KAFKA_PRODUCER_CONF

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

FALLBACK_VN30 = [
    "ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","HDB","HPG",
    "MBB","MSN","MWG","PLX","POW","SAB","SHB","SSB","SSI","STB",
    "TCB","TPB","VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE",
]


def fetch_vn30_symbols() -> list:
    """
    Lấy danh sách mã VN30 mới nhất từ SSI.
    Nếu không connect được thì dùng fallback hardcoded.
    """
    try:
        import requests
        from config.settings import SSI_URL, SSI_DEVICE_ID
        headers = {
            "accept": "application/json",
            "device-id": SSI_DEVICE_ID,
            "origin": "https://iboard.ssi.com.vn",
            "referer": "https://iboard.ssi.com.vn/",
            "user-agent": "Mozilla/5.0",
        }
        resp = requests.get(SSI_URL, headers=headers, timeout=10)
        resp.raise_for_status()
        symbols = [item["stockSymbol"] for item in resp.json()["data"]]
        log.info("Fetch VN30 tu SSI: %d ma — %s", len(symbols), symbols)
        return symbols
    except Exception as e:
        log.warning("Khong fetch duoc VN30 tu SSI (%s) → dung fallback %d ma", e, len(FALLBACK_VN30))
        return FALLBACK_VN30


# Lấy danh sách động khi khởi động
VN30 = fetch_vn30_symbols()

# Giá tham chiếu ban đầu (VND, giữ nguyên float không round)
BASE_PRICES = {sym: random.uniform(15_000, 120_000) for sym in VN30}


def make_tick(symbol: str) -> dict:
    ref   = BASE_PRICES[symbol]
    ceil  = ref * 1.07
    floor = ref * 0.93

    matched = random.uniform(floor, ceil)
    volume  = random.randint(100, 50_000) * 100

    # Drift nhẹ sau mỗi tick
    BASE_PRICES[symbol] = matched * random.uniform(0.999, 1.001)

    change     = round(matched - ref, 2)
    change_pct = round(change / ref * 100, 4) if ref else 0

    bid1 = matched - random.randint(1, 3) * 50
    ask1 = matched + random.randint(1, 3) * 50

    return {
        "symbol":           symbol,
        "exchange":         "HOSE",
        "trading_date":     date.today().isoformat(),
        "crawled_at":       datetime.utcnow().isoformat() + "Z",
        "session":          "LO",
        "trading_status":   "Trading",

        "ref_price":        ref,
        "ceiling":          ceil,
        "floor":            floor,
        "open_price":       ref * random.uniform(0.98, 1.02),
        "matched_price":    matched,
        "highest":          max(matched, ref) * random.uniform(1.0, 1.03),
        "lowest":           min(matched, ref) * random.uniform(0.97, 1.0),
        "avg_price":        (matched + ref) / 2,
        "prior_close":      ref,

        "price_change":     change,
        "price_change_pct": change_pct,

        "matched_volume":   volume,
        "total_volume":     volume * random.randint(5, 30),
        "total_value":      matched * volume * random.randint(5, 30),

        "foreign_buy_vol":  random.randint(0, 10_000) * 100,
        "foreign_sell_vol": random.randint(0, 10_000) * 100,
        "foreign_room":     random.randint(10_000, 5_000_000) * 100,

        "bid1_price": bid1,        "bid1_vol": random.randint(1, 500) * 100,
        "bid2_price": bid1 - 50,   "bid2_vol": random.randint(1, 500) * 100,
        "bid3_price": bid1 - 100,  "bid3_vol": random.randint(1, 500) * 100,
        "ask1_price": ask1,        "ask1_vol": random.randint(1, 500) * 100,
        "ask2_price": ask1 + 50,   "ask2_vol": random.randint(1, 500) * 100,
        "ask3_price": ask1 + 100,  "ask3_vol": random.randint(1, 500) * 100,
    }


def delivery_report(err, msg):
    if err:
        log.error("Kafka delivery failed: %s", err)


def main(interval: float = 1.0):
    producer = Producer(KAFKA_PRODUCER_CONF)
    log.info("Mock producer khởi động — %d mã VN30 | interval: %.1fs", len(VN30), interval)

    total = 0
    while True:
        for symbol in VN30:
            tick = make_tick(symbol)
            producer.produce(
                topic    = KAFKA_TOPIC_RAW,
                key      = symbol.encode(),
                value    = json.dumps(tick, ensure_ascii=False).encode(),
                callback = delivery_report,
            )
        producer.poll(0)
        total += len(VN30)
        log.info("Pushed %d ticks | Tổng: %d", len(VN30), total)
        time.sleep(interval)


if __name__ == "__main__":
    main(interval=1.0)