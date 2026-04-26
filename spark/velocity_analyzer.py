"""
velocity_analyzer.py — Spark Streaming Job 3.2
───────────────────────────────────────────────
Consumes: raw-articles
Produces: viral-alerts (Kafka) + domain_risk table (SQLite)

Logic:
  • Sliding window: 5-minute window, 1-minute slide
  • Count share events per article cluster (source_domain × category)
  • Detect viral threshold breaches → publish to viral-alerts topic
  • Rank top-10 most dangerous source domains → write to domain_risk DB table
"""

import json
import logging
import os
import sqlite3

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from shared import (
    ARTICLE_SCHEMA,
    CHECKPOINT_DIR,
    DATA_DIR,
    DB_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_RAW,
    TOPIC_VIRAL,
    VIRAL_VELOCITY_THRESHOLD,
    get_conn,
)

log = logging.getLogger(__name__)

WINDOW_DURATION = "5 minutes"
SLIDE_DURATION  = "1 minute"
WATERMARK       = "2 minutes"


def upsert_domain_risk(batch_df: DataFrame, batch_id: int) -> None:
    """Write domain risk rankings from each micro-batch to SQLite."""
    if batch_df.isEmpty():
        return

    # Compute a composite risk score:
    #   risk = (1 - avg_credibility) * log10(total_velocity + 1) * article_count
    risk_df = (
        batch_df
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION).alias("w"),
            "source_domain",
        )
        .agg(
            F.count("*").alias("article_count"),
            F.avg("credibility_score").alias("avg_credibility"),
            F.min("credibility_score").alias("min_credibility"),
            F.sum("share_velocity").alias("total_velocity"),
        )
        .withColumn(
            "risk_score",
            F.round(
                (1 - F.col("avg_credibility"))
                * F.log10(F.col("total_velocity") + 1)
                * F.col("article_count"),
                3,
            ),
        )
        .withColumn("window_start", F.col("w.start").cast(StringType()))
        .withColumn("window_end",   F.col("w.end").cast(StringType()))
        .drop("w")
    )

    rows = risk_df.collect()
    if not rows:
        return

    con = get_conn(DB_PATH)
    cur = con.cursor()
    for row in rows:
        cur.execute("""
            INSERT INTO domain_risk
                (window_start, source_domain, article_count,
                 avg_credibility, min_credibility, total_velocity, risk_score)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(window_start, source_domain) DO UPDATE SET
                article_count   = excluded.article_count,
                avg_credibility = excluded.avg_credibility,
                min_credibility = excluded.min_credibility,
                total_velocity  = excluded.total_velocity,
                risk_score      = excluded.risk_score
        """, (
            row["window_start"],
            row["source_domain"],
            int(row["article_count"]),
            float(row["avg_credibility"]) if row["avg_credibility"] else None,
            float(row["min_credibility"]) if row["min_credibility"] else None,
            int(row["total_velocity"]) if row["total_velocity"] else 0,
            float(row["risk_score"]) if row["risk_score"] else 0.0,
        ))
    con.commit()
    con.close()
    log.info(f"[3.2] Domain risk: upserted {len(rows)} rows (batch {batch_id})")


def run(spark: SparkSession) -> list:
    log.info("▶  Job 3.2 — Velocity Analyzer starting")

    checkpoint = os.path.join(CHECKPOINT_DIR, "velocity_analyzer")

    # ── Read + parse raw-articles ─────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast(StringType()),
                ARTICLE_SCHEMA,
            ).alias("data"),
        )
        .select("data.*")
        .filter(F.col("article_id").isNotNull())
        # Parse ISO timestamp string → proper timestamp for windowing
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withWatermark("event_time", WATERMARK)
    )

    # ── Sliding window aggregation ────────────────────────────────────────────
    windowed = (
        parsed
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION).alias("w"),
            "source_domain",
            "category",
        )
        .agg(
            F.count("*").alias("article_count"),
            F.sum("share_velocity").alias("total_velocity"),
            F.avg("share_velocity").alias("avg_velocity"),
            F.max("share_velocity").alias("max_velocity"),
            F.avg("credibility_score").alias("avg_credibility"),
        )
        .withColumn("window_start", F.col("w.start").cast(StringType()))
        .withColumn("window_end",   F.col("w.end").cast(StringType()))
        .drop("w")
    )

    # ── Detect viral breaches → publish to viral-alerts topic ─────────────────
    viral_breaches = windowed.filter(
        F.col("avg_velocity") >= VIRAL_VELOCITY_THRESHOLD
    )

    viral_for_kafka = viral_breaches.select(
        F.col("category").alias("key"),
        F.to_json(
            F.struct(
                "window_start", "window_end",
                "source_domain", "category",
                "article_count", "total_velocity", "avg_velocity", "max_velocity",
            )
        ).alias("value"),
    )

    kafka_query = (
        viral_for_kafka.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", TOPIC_VIRAL)
        .option("checkpointLocation", os.path.join(checkpoint, "kafka"))
        .outputMode("append")
        .queryName("velocity_viral_kafka")
        .start()
    )

    # ── Write domain risk to SQLite (uses parsed, not windowed) ───────────────
    db_query = (
        parsed.writeStream
        .foreachBatch(upsert_domain_risk)
        .option("checkpointLocation", os.path.join(checkpoint, "db"))
        .outputMode("append")
        .queryName("velocity_domain_risk_db")
        .start()
    )

    log.info("✅ Job 3.2 — Velocity Analyzer running")
    return [kafka_query, db_query]