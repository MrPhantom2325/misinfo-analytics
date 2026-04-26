"""
run_all.py — Spark Streaming Orchestrator
──────────────────────────────────────────
Launches all 5 analytics jobs in a single SparkSession.
All jobs share the same Spark context but run independent streaming queries.

Jobs:
  3.1  credibility_filter   — flags low-credibility → flagged-misinfo topic
  3.2  velocity_analyzer    — sliding window viral detection + domain risk DB
  3.3  geo_aggregator       — country-level heatmap → geo-spread topic + DB
  3.4  category_trends      — rolling category counts → misinfo_trends DB
  3.5  viral_tracker        — high-velocity articles → viral_articles DB

Run inside Docker:
  CMD ["python", "run_all.py"]  (see spark/Dockerfile)

Run locally:
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \\
  DB_PATH=./data/misinfo.db \\
  DATA_DIR=./data \\
  python run_all.py
"""

import logging
import os
import signal
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("run_all")

# ─── PySpark setup ─────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession

from shared import (
    CHECKPOINT_DIR,
    DATA_DIR,
    DB_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    init_db,
)
from credibility_filter import run as run_credibility
from velocity_analyzer   import run as run_velocity
from geo_aggregator      import run as run_geo
from category_trends     import run_category_trends, run_viral_tracker


def build_spark() -> SparkSession:
    """Create a SparkSession with Kafka connector packages."""
    kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

    spark = (
        SparkSession.builder
        .appName("MisinformationAnalytics")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", kafka_pkg)
        .config("spark.sql.streaming.pollingDelay", "1000")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    log.info("=" * 60)
    log.info("  Misinformation Analytics — Spark Streaming Engine")
    log.info("=" * 60)
    log.info(f"  Kafka    : {KAFKA_BOOTSTRAP_SERVERS}")
    log.info(f"  DB path  : {DB_PATH}")
    log.info(f"  Data dir : {DATA_DIR}")
    log.info(f"  Checkpts : {CHECKPOINT_DIR}")
    log.info("=" * 60)

    # ── Phase 4: Initialise storage ───────────────────────────────────────────
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    init_db(DB_PATH)

    # ── Build Spark session ───────────────────────────────────────────────────
    log.info("Building SparkSession (downloading Kafka JARs on first run)...")
    spark = build_spark()
    log.info(f"✅ Spark version: {spark.version}")

    # ── Launch all streaming jobs ─────────────────────────────────────────────
    all_queries = []

    try:
        log.info("Starting streaming jobs...")

        queries_31 = run_credibility(spark)
        all_queries.extend(queries_31)
        log.info(f"  3.1 Credibility Filter   — {len(queries_31)} query/queries")

        queries_32 = run_velocity(spark)
        all_queries.extend(queries_32)
        log.info(f"  3.2 Velocity Analyzer    — {len(queries_32)} query/queries")

        queries_33 = run_geo(spark)
        all_queries.extend(queries_33)
        log.info(f"  3.3 Geo Aggregator       — {len(queries_33)} query/queries")

        queries_34 = run_category_trends(spark)
        all_queries.extend(queries_34)
        log.info(f"  3.4 Category Trends      — {len(queries_34)} query/queries")

        queries_35 = run_viral_tracker(spark)
        all_queries.extend(queries_35)
        log.info(f"  3.5 Viral Tracker        — {len(queries_35)} query/queries")

        log.info(f"\n✅ All {len(all_queries)} streaming queries active. Engine running.")
        log.info("   Press Ctrl+C to stop.\n")

    except Exception as e:
        log.error(f"❌ Failed to start streaming jobs: {e}", exc_info=True)
        spark.stop()
        sys.exit(1)

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    def shutdown(sig, frame):
        log.info("🛑 Shutdown signal received — stopping all queries...")
        for q in all_queries:
            try:
                q.stop()
            except Exception as ex:
                log.warning(f"Error stopping query {q.name}: {ex}")
        spark.stop()
        log.info("✅ Spark stopped cleanly.")
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── Health monitor loop ───────────────────────────────────────────────────
    while True:
        time.sleep(30)
        active = []
        dead   = []
        for q in all_queries:
            (active if q.isActive else dead).append(q.name)

        log.info(f"📡 Active queries ({len(active)}): {active}")
        if dead:
            log.warning(f"⚠️  Dead queries ({len(dead)}): {dead}")
            for q in all_queries:
                if not q.isActive and q.exception():
                    log.error(f"   {q.name} failed: {q.exception()}")


if __name__ == "__main__":
    main()