# producer/producer.py
import os, json, random, time, sys
from datetime import datetime, timezone
from kafka import KafkaProducer, errors as kerrors

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "events")
MSGS_PER_SEC = int(os.getenv("MSGS_PER_SEC", "5"))

PAGES = ["/", "/home", "/search", "/product/sku-1", "/product/sku-2", "/cart", "/checkout", "/help"]
REFERRERS = ["direct", "google", "facebook", "twitter", "email", "partner"]
DEVICES = ["desktop","mobile","tablet"]
BROWSERS = ["Chrome","Safari","Firefox","Edge"]
COUNTRIES = ["US","IN","DE","GB","CA","AU","FR","BR","SG","AE"]
CAMPAIGNS = ["spring_sale","retargeting","launch","brand","none"]
EVENT_TYPES = ["page_view","click","add_to_cart","purchase"]

def make_event():
    now = datetime.now(timezone.utc).isoformat()
    event_type = random.choices(EVENT_TYPES, weights=[75, 15, 7, 3])[0]
    revenue = round(random.uniform(10, 200), 2) if event_type == "purchase" else None
    return {
        "event_time": now,
        "user_id": f"user_{random.randint(1, 5000)}",
        "session_id": f"sess_{random.randint(1, 500000)}" if random.random() < 0.7 else None,
        "event_type": event_type,
        "page": random.choice(PAGES),
        "referrer": random.choice(REFERRERS),
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "country": random.choice(COUNTRIES),
        "campaign": random.choice(CAMPAIGNS),
        "latency_ms": random.randint(50, 1200),
        "revenue": revenue
    }

def build_producer():
    # Send immediately (linger_ms=0), small batch, and wait for acks periodically
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        client_id="clickstream-producer",
        acks=1,                 # 0 | 1 | 'all'
        linger_ms=0,            # do not delay sends
        batch_size=16384,       # 16 KB
        buffer_memory=33554432, # 32 MB
        retries=10,
        retry_backoff_ms=500,
        max_in_flight_requests_per_connection=1,
        request_timeout_ms=30000,
        max_block_ms=120000,
        api_version_auto_timeout_ms=30000,
    )

def wait_for_topic(p: KafkaProducer, topic: str, attempts: int = 60, delay: float = 1.0):
    for i in range(1, attempts + 1):
        try:
            if p.bootstrap_connected():
                parts = p.partitions_for(topic)
                if parts:
                    print(f"[meta] topic '{topic}' partitions={sorted(list(parts))}", flush=True)
                    return
        except kerrors.KafkaError as e:
            print(f"[wait] metadata error: {e}", file=sys.stderr, flush=True)
        if i % 5 == 0:
            print(f"[wait] waiting for Kafka/topic metadata (attempt {i}/{attempts})...", flush=True)
        time.sleep(delay)
    raise RuntimeError("Kafka/topic metadata not available after waiting")

def main():
    prod = build_producer()
    wait_for_topic(prod, TOPIC)

    rate_hz = max(MSGS_PER_SEC, 1)
    interval = 1.0 / rate_hz
    print(f"Producing to {BOOTSTRAP} topic '{TOPIC}' at ~{MSGS_PER_SEC} msg/s (acks=1)", flush=True)

    counter = 0
    while True:
        try:
            fut = prod.send(TOPIC, make_event())
            counter += 1

            # Every 10th message, wait for the broker ack and flush buffers.
            if counter % 10 == 0:
                rec = fut.get(timeout=5)   # raises if not acked
                print(f"[ack] {rec.topic}:{rec.partition}@{rec.offset}", flush=True)
                prod.flush()
        except kerrors.KafkaTimeoutError as e:
            print(f"[warn] KafkaTimeoutError: {e}", file=sys.stderr, flush=True)
            time.sleep(1.0)
            continue
        except kerrors.KafkaError as e:
            print(f"[warn] KafkaError: {e}", file=sys.stderr, flush=True)
            time.sleep(1.0)
            continue

        time.sleep(interval)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[fatal] Producer exiting: {e}", file=sys.stderr, flush=True)
        raise
