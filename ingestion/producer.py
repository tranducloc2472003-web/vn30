import json
import time
import logging
from datetime import datetime
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Kafka config ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:29092"   # đổi thành "kafka:9092" nếu chạy trong Docker
KAFKA_TOPIC     = "ssi.ticks.raw"
POLL_INTERVAL   = 5                   # giây giữa mỗi lần crawl

# ── SSI headers ───────────────────────────────────────────────────────────────
SSI_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi",
    "cache-control": "no-cache",
    "device-id": "6416C7C2-E423-443E-9725-C751AD691C50",
    "origin": "https://iboard.ssi.com.vn",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "referer": "https://iboard.ssi.com.vn/",
    "sec-ch-ua": '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36"
    ),
    "x-device-name": "Chrome",
    "x-os-name": "Windows",
}

SSI_URL = "https://iboard-query.ssi.com.vn/stock/group/VN30"


# ── Crawl ─────────────────────────────────────────────────────────────────────
def get_data() -> list:
    import requests
    resp = requests.get(SSI_URL, headers=SSI_HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()["data"]


# ── Format (code của bạn, giữ nguyên) ────────────────────────────────────────
def format_data(res: dict) -> dict:
    data = {}
    data["MaCK"]             = res["stockSymbol"]
    data["Ten cong ty"]      = res["companyNameVi"]
    data["Ten cong ty (EN)"] = res["companyNameEn"]
    data["San"]              = res["exchange"].upper()
    data["Thi truong"]       = res["market"]

    data["Gia tran"]          = res["ceiling"]
    data["Gia san"]           = res["floor"]
    data["Gia tham chieu"]    = res["refPrice"]
    data["Gia khop lenh"]     = res["matchedPrice"]
    data["Gia mo cua"]        = res["openPrice"]
    data["Gia cao nhat"]      = res["highest"]
    data["Gia thap nhat"]     = res["lowest"]
    data["Gia trung binh"]    = res["avgPrice"]
    data["Gia dong cua truoc"]= res["priorClosePrice"]

    data["Thay doi gia"]      = res["priceChange"]
    data["Thay doi gia (%)"]  = res["priceChangePercent"]

    data["KL khop lenh"]       = res["matchedVolume"]
    data["Tong KL giao dich"]  = res["nmTotalTradedQty"]
    data["Tong GT giao dich"]  = res["nmTotalTradedValue"]
    data["Lo giao dich"]       = res["tradingUnit"]

    data["KL nuoc ngoai mua"]  = res["buyForeignQtty"]
    data["GT nuoc ngoai mua"]  = res["buyForeignValue"]
    data["KL nuoc ngoai ban"]  = res["sellForeignQtty"]
    data["GT nuoc ngoai ban"]  = res["sellForeignValue"]
    data["Room con lai"]       = res["remainForeignQtty"]

    data["Mua 1 - Gia"]  = res["best1Bid"]
    data["Mua 1 - KL"]   = res["best1BidVol"]
    data["Mua 2 - Gia"]  = res["best2Bid"]
    data["Mua 2 - KL"]   = res["best2BidVol"]
    data["Mua 3 - Gia"]  = res["best3Bid"]
    data["Mua 3 - KL"]   = res["best3BidVol"]

    data["Ban 1 - Gia"]  = res["best1Offer"]
    data["Ban 1 - KL"]   = res["best1OfferVol"]
    data["Ban 2 - Gia"]  = res["best2Offer"]
    data["Ban 2 - KL"]   = res["best2OfferVol"]
    data["Ban 3 - Gia"]  = res["best3Offer"]
    data["Ban 3 - KL"]   = res["best3OfferVol"]

    data["Phien"]            = res["session"]
    data["Ngay GD"]          = res["tradingDate"]
    data["Trang thai GD"]    = res["tradingStatus"]
    data["Trang thai hanh chinh"] = res["adminStatus"]

    return data


# ── Làm sạch ──────────────────────────────────────────────────────────────────
def _to_float(val) -> float | None:
    """Chuyển giá trị về float, trả về None nếu không hợp lệ."""
    try:
        return float(val) if val not in (None, "", "0", 0) else None
    except (TypeError, ValueError):
        return None


def clean(record: dict) -> dict | None:
    """
    Validate và chuẩn hoá một record tick.
    Trả về None nếu record không đủ điều kiện push lên Kafka.
    """
    symbol = record.get("MaCK", "").strip().upper()
    if not symbol:
        log.warning("Bỏ record: thiếu mã CK")
        return None

    matched_price = _to_float(record.get("Gia khop lenh"))
    if matched_price is None or matched_price <= 0:
        log.debug("Bỏ %s: giá khớp lệnh không hợp lệ (%s)", symbol, record.get("Gia khop lenh"))
        return None

    ref_price = _to_float(record.get("Gia tham chieu"))
    ceiling   = _to_float(record.get("Gia tran"))
    floor_p   = _to_float(record.get("Gia san"))

    # Giá khớp phải nằm trong biên độ (nếu có đủ dữ liệu)
    if ceiling and floor_p:
        if not (floor_p <= matched_price <= ceiling):
            log.warning(
                "Bỏ %s: giá khớp %.1f ngoài biên độ [%.1f, %.1f]",
                symbol, matched_price, floor_p, ceiling
            )
            return None

    volume = _to_float(record.get("KL khop lenh"))

    # Parse ngày giao dịch "20260515" → ISO string
    raw_date = str(record.get("Ngay GD", "")).strip()
    try:
        trading_date = datetime.strptime(raw_date, "%Y%m%d").date().isoformat()
    except ValueError:
        trading_date = None

    return {
        "symbol":          symbol,
        "exchange":        record.get("San", "").upper(),
        "trading_date":    trading_date,
        "crawled_at":      datetime.utcnow().isoformat() + "Z",
        "session":         record.get("Phien"),
        "trading_status":  record.get("Trang thai GD"),

        # Giá
        "ref_price":       ref_price,
        "ceiling":         ceiling,
        "floor":           floor_p,
        "open_price":      _to_float(record.get("Gia mo cua")),
        "matched_price":   matched_price,
        "highest":         _to_float(record.get("Gia cao nhat")),
        "lowest":          _to_float(record.get("Gia thap nhat")),
        "avg_price":       _to_float(record.get("Gia trung binh")),
        "prior_close":     _to_float(record.get("Gia dong cua truoc")),

        # Thay đổi
        "price_change":    _to_float(record.get("Thay doi gia")),
        "price_change_pct":_to_float(record.get("Thay doi gia (%)")),

        # Khối lượng
        "matched_volume":  volume,
        "total_volume":    _to_float(record.get("Tong KL giao dich")),
        "total_value":     _to_float(record.get("Tong GT giao dich")),

        # Nước ngoài
        "foreign_buy_vol": _to_float(record.get("KL nuoc ngoai mua")),
        "foreign_sell_vol":_to_float(record.get("KL nuoc ngoai ban")),
        "foreign_room":    _to_float(record.get("Room con lai")),

        # Order book top 3
        "bid1_price": _to_float(record.get("Mua 1 - Gia")),
        "bid1_vol":   _to_float(record.get("Mua 1 - KL")),
        "bid2_price": _to_float(record.get("Mua 2 - Gia")),
        "bid2_vol":   _to_float(record.get("Mua 2 - KL")),
        "bid3_price": _to_float(record.get("Mua 3 - Gia")),
        "bid3_vol":   _to_float(record.get("Mua 3 - KL")),
        "ask1_price": _to_float(record.get("Ban 1 - Gia")),
        "ask1_vol":   _to_float(record.get("Ban 1 - KL")),
        "ask2_price": _to_float(record.get("Ban 2 - Gia")),
        "ask2_vol":   _to_float(record.get("Ban 2 - KL")),
        "ask3_price": _to_float(record.get("Ban 3 - Gia")),
        "ask3_vol":   _to_float(record.get("Ban 3 - KL")),
    }


# ── Kafka helpers ─────────────────────────────────────────────────────────────
def delivery_report(err, msg):
    if err:
        log.error("Kafka delivery failed: %s", err)
    else:
        log.debug("Delivered → %s [partition %d]", msg.topic(), msg.partition())


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "1",                  # leader ACK là đủ cho tick data
        "compression.type": "lz4",
        "linger.ms": 200,             # batch nhỏ trong 200ms để tăng throughput
        "queue.buffering.max.messages": 100_000,
    })


# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    producer = make_producer()
    log.info("Producer khởi động — topic: %s | interval: %ss", KAFKA_TOPIC, POLL_INTERVAL)

    pushed_total = 0

    while True:
        try:
            raw_list = get_data()
            batch_ok = 0

            for raw in raw_list:
                formatted = format_data(raw)
                cleaned   = clean(formatted)
                if cleaned is None:
                    continue

                producer.produce(
                    topic     = KAFKA_TOPIC,
                    key       = cleaned["symbol"].encode(),   # partition theo mã CK
                    value     = json.dumps(cleaned, ensure_ascii=False).encode(),
                    callback  = delivery_report,
                )
                batch_ok += 1

            producer.poll(0)   # flush callbacks không blocking
            pushed_total += batch_ok
            log.info("Batch: %d/%d records hợp lệ | Tổng: %d", batch_ok, len(raw_list), pushed_total)

        except Exception as exc:
            log.error("Lỗi crawl: %s — thử lại sau %ss", exc, POLL_INTERVAL)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()