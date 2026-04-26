"""
Misinformation Kafka Producer
──────────────────────────────
Simulates real-time misinformation article streams into Kafka topics.

Topics written to:
  • raw-articles   — all generated articles
  • viral-alerts   — articles exceeding viral velocity threshold

Run locally:
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python producer.py

Run in Docker:
  Handled by docker-compose (env vars injected automatically)
"""

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from threading import Event, Thread

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

from sample_data import (
    BURST_CAMPAIGNS,
    CREDIBILITY_RANGES,
    GEO_ORIGINS,
    HEADLINE_TEMPLATES,
    KEYWORD_POOLS,
    PLATFORMS,
    SENTIMENT_RANGES,
    SOURCE_DOMAINS,
    TEMPLATE_VARS,
    VELOCITY_RANGES,
)

# ─── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Config from environment ───────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PRODUCER_RATE = int(os.getenv("PRODUCER_RATE", "10"))           # msgs/sec (default 10 for local)
BURST_INTERVAL = int(os.getenv("BURST_INTERVAL", "60"))         # secs between burst events
BURST_MULTIPLIER = int(os.getenv("BURST_MULTIPLIER", "10"))
VIRAL_VELOCITY_THRESHOLD = int(os.getenv("VIRAL_VELOCITY_THRESHOLD", "5000"))

TOPIC_RAW = "raw-articles"
TOPIC_VIRAL = "viral-alerts"

fake = Faker()
shutdown_event = Event()


# ─── Article Generator ─────────────────────────────────────────────────────────

def pick_geo() -> str:
    """Weighted random country selection."""
    countries = list(GEO_ORIGINS.keys())
    weights = list(GEO_ORIGINS.values())
    return random.choices(countries, weights=weights, k=1)[0]


def fill_template(template: str) -> str:
    """Replace {placeholders} in headline templates with random values."""
    result = template
    for key, values in TEMPLATE_VARS.items():
        placeholder = f"{{{key}}}"
        if placeholder in result:
            result = result.replace(placeholder, random.choice(values), 1)
    return result


def generate_article(
    category: str = None,
    geo_override: str = None,
    platform_override: str = None,
    velocity_boost: float = 1.0,
) -> dict:
    """Generate a single misinformation article event."""

    if category is None:
        category = random.choice(["health", "politics", "climate", "finance"])

    # Headline
    template = random.choice(HEADLINE_TEMPLATES[category])
    headline = fill_template(template)

    # Source domain
    source_domain = random.choice(SOURCE_DOMAINS[category])

    # Credibility (low — this is misinfo)
    cred_min, cred_max = CREDIBILITY_RANGES[category]
    credibility_score = round(random.uniform(cred_min, cred_max), 3)

    # Geo origin
    geo_origin = geo_override or pick_geo()

    # Platform
    platform = platform_override or random.choice(PLATFORMS)

    # Share velocity
    vel_min, vel_max = VELOCITY_RANGES["normal"]
    if velocity_boost > 5:
        vel_min, vel_max = VELOCITY_RANGES["viral"]
    elif velocity_boost > 2:
        vel_min, vel_max = VELOCITY_RANGES["trending"]
    share_velocity = int(random.uniform(vel_min, vel_max) * velocity_boost)
    share_velocity = min(share_velocity, 100_000)  # cap at 100k

    # Sentiment
    sent_min, sent_max = SENTIMENT_RANGES[category]
    sentiment_score = round(random.uniform(sent_min, sent_max), 3)

    # Keywords
    pool = KEYWORD_POOLS[category]
    keyword_flags = random.sample(pool, k=min(random.randint(2, 5), len(pool)))

    return {
        "article_id": str(uuid.uuid4()),
        "headline": headline,
        "source_domain": source_domain,
        "category": category,
        "credibility_score": credibility_score,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "geo_origin": geo_origin,
        "platform": platform,
        "share_velocity": share_velocity,
        "sentiment_score": sentiment_score,
        "keyword_flags": keyword_flags,
    }


# ─── Kafka Producer Setup ──────────────────────────────────────────────────────

def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Create Kafka producer with retry logic for startup timing."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_block_ms=10000,
            )
            log.info(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except KafkaError as e:
            log.warning(f"Attempt {attempt}/{retries}: Kafka not ready — {e}. Retrying in {delay}s...")
            time.sleep(delay)

    log.error("❌ Could not connect to Kafka after all retries. Exiting.")
    sys.exit(1)


def on_send_error(exc):
    log.error(f"Failed to send message: {exc}")


def publish_article(producer: KafkaProducer, article: dict):
    """Publish article to raw-articles topic, and viral-alerts if velocity threshold crossed."""
    key = article["category"]

    # Always publish to raw-articles
    producer.send(
        TOPIC_RAW,
        key=key,
        value=article,
    ).add_errback(on_send_error)

    # Also publish to viral-alerts if share_velocity exceeds threshold
    if article["share_velocity"] >= VIRAL_VELOCITY_THRESHOLD:
        producer.send(
            TOPIC_VIRAL,
            key=key,
            value=article,
        ).add_errback(on_send_error)


# ─── Burst Campaign Simulator ──────────────────────────────────────────────────

def run_burst_campaign(producer: KafkaProducer, campaign: dict):
    """Simulate a coordinated viral misinformation campaign burst."""
    log.warning(f"🚨 BURST CAMPAIGN STARTING: {campaign['name']}")
    log.info(f"   Category: {campaign['category']} | "
             f"Duration: {campaign['duration_secs']}s | "
             f"Rate multiplier: {campaign['rate_multiplier']}x")

    end_time = time.time() + campaign["duration_secs"]
    burst_rate = PRODUCER_RATE * campaign["rate_multiplier"]
    sleep_interval = 1.0 / max(burst_rate, 1)
    count = 0

    while time.time() < end_time and not shutdown_event.is_set():
        geo = random.choice(campaign["geo_focus"])
        platform = random.choice(campaign["platform_focus"])

        article = generate_article(
            category=campaign["category"],
            geo_override=geo,
            platform_override=platform,
            velocity_boost=campaign["rate_multiplier"] * 0.8,
        )
        publish_article(producer, article)
        count += 1
        time.sleep(sleep_interval)

    log.info(f"✅ Burst campaign complete. Sent {count} articles.")


# ─── Stats Reporter ────────────────────────────────────────────────────────────

class Stats:
    def __init__(self):
        self.total = 0
        self.viral = 0
        self.by_category = {"health": 0, "politics": 0, "climate": 0, "finance": 0}
        self.start_time = time.time()

    def record(self, article: dict):
        self.total += 1
        self.by_category[article["category"]] += 1
        if article["share_velocity"] >= VIRAL_VELOCITY_THRESHOLD:
            self.viral += 1

    def report(self):
        elapsed = time.time() - self.start_time
        rate = self.total / elapsed if elapsed > 0 else 0
        log.info(
            f"📊 Stats | Total: {self.total} | Rate: {rate:.1f}/s | "
            f"Viral: {self.viral} | "
            f"Health: {self.by_category['health']} | "
            f"Politics: {self.by_category['politics']} | "
            f"Climate: {self.by_category['climate']} | "
            f"Finance: {self.by_category['finance']}"
        )


# ─── Main Producer Loop ────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("  Misinformation Analytics — Kafka Producer")
    log.info("=" * 60)
    log.info(f"  Bootstrap servers : {KAFKA_BOOTSTRAP_SERVERS}")
    log.info(f"  Publish rate      : {PRODUCER_RATE} msgs/sec")
    log.info(f"  Burst interval    : every {BURST_INTERVAL}s")
    log.info(f"  Viral threshold   : {VIRAL_VELOCITY_THRESHOLD} shares/min")
    log.info("=" * 60)

    producer = create_producer()
    stats = Stats()

    sleep_interval = 1.0 / max(PRODUCER_RATE, 1)
    last_burst_time = time.time()
    last_stats_time = time.time()
    in_burst = False

    # Graceful shutdown
    def handle_signal(sig, frame):
        log.info("🛑 Shutdown signal received. Flushing producer...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    log.info("🚀 Producer started. Streaming articles into Kafka...")

    try:
        while not shutdown_event.is_set():
            now = time.time()

            # ── Trigger a burst campaign periodically ────────────────────────
            if (now - last_burst_time) >= BURST_INTERVAL and not in_burst:
                campaign = random.choice(BURST_CAMPAIGNS)
                in_burst = True

                def make_burst_runner(c):
                    def run_burst():
                        run_burst_campaign(producer, c)
                    return run_burst

                burst_thread = Thread(target=make_burst_runner(campaign), daemon=True)
                burst_thread.start()
                last_burst_time = now

            # ── Normal article generation ────────────────────────────────────
            article = generate_article()
            publish_article(producer, article)
            stats.record(article)

            # ── Stats every 10 seconds ───────────────────────────────────────
            if (now - last_stats_time) >= 10:
                stats.report()
                last_stats_time = now

            time.sleep(sleep_interval)

    finally:
        log.info("Flushing remaining messages...")
        producer.flush(timeout=10)
        producer.close()
        stats.report()
        log.info("✅ Producer shut down cleanly.")


if __name__ == "__main__":
    main()