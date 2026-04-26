"""
geo_aggregator.py — Spark Streaming Job 3.3
────────────────────────────────────────────
Consumes: raw-articles
Produces: geo-spread (Kafka) + geo_heatmap table (SQLite)

Logic:
  • 5-min tumbling window, group by geo_origin + category
  • Count misinfo volume per country per window
  • Publish aggregates to geo-spread Kafka topic
  • Persist to geo_heatmap SQLite table for dashboard choropleth
"""

import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from shared import (
    ARTICLE_SCHEMA,
    CHECKPOINT_DIR,
    DB_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_GEO,
    TOPIC_RAW,
    get_conn,
)

log = logging.getLogger(__name__)

WINDOW_DURATION = "5 minutes"
WATERMARK       = "2 minutes"


def upsert_geo_heatmap(batch_df: DataFrame, batch_id: int) -> None:
    """Persist geo aggregates to SQLite geo_heatmap table."""
    if batch_df.isEmpty():
        return

    geo_df = (
        batch_df
        .groupBy(
            F.window("event_time", WINDOW_DURATION).alias("w"),
            "geo_origin",
            "category",
        )
        .agg(
            F.count("*").alias("article_count"),
            F.avg("share_velocity").alias("avg_velocity"),
            F.avg("credibility_score").alias("avg_credibility"),
            F.avg("sentiment_score").alias("avg_sentiment"),
        )
        .withColumn("window_start", F.col("w.start").cast(StringType()))
        .withColumn("window_end",   F.col("w.end").cast(StringType()))
        .drop("w")
    )

    rows = geo_df.collect()
    if not rows:
        return

    con = get_conn(DB_PATH)
    cur = con.cursor()
    for row in rows:
        cur.execute("""
            INSERT INTO geo_heatmap
                (window_start, window_end, geo_origin, category,
                 article_count, avg_velocity)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(window_start, geo_origin, category) DO UPDATE SET
                article_count = excluded.article_count,
                avg_velocity  = excluded.avg_velocity
        """, (
            row["window_start"],
            row["window_end"],
            row["geo_origin"],
            row["category"],
            int(row["article_count"]),
            float(row["avg_velocity"]) if row["avg_velocity"] else 0.0,
        ))
    con.commit()
    con.close()
    log.info(f"[3.3] Geo heatmap: upserted {len(rows)} rows (batch {batch_id})")


def run(spark: SparkSession) -> list:
    log.info("▶  Job 3.3 — Geo Aggregator starting")

    checkpoint = os.path.join(CHECKPOINT_DIR, "geo_aggregator")

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
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withWatermark("event_time", WATERMARK)
    )

    # ── Windowed geo aggregation ──────────────────────────────────────────────
    geo_agg = (
        parsed
        .groupBy(
            F.window("event_time", WINDOW_DURATION).alias("w"),
            "geo_origin",
            "category",
        )
        .agg(
            F.count("*").alias("article_count"),
            F.avg("share_velocity").alias("avg_velocity"),
        )
        .withColumn("window_start", F.col("w.start").cast(StringType()))
        .withColumn("window_end",   F.col("w.end").cast(StringType()))
        .drop("w")
    )

    # ── Publish geo aggregates to Kafka geo-spread topic ──────────────────────
    geo_for_kafka = geo_agg.select(
        F.col("geo_origin").alias("key"),
        F.to_json(
            F.struct(
                "window_start", "window_end",
                "geo_origin", "category",
                "article_count", "avg_velocity",
            )
        ).alias("value"),
    )

    kafka_query = (
        geo_for_kafka.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", TOPIC_GEO)
        .option("checkpointLocation", os.path.join(checkpoint, "kafka"))
        .outputMode("append")
        .queryName("geo_kafka")
        .start()
    )

    # ── Persist to SQLite via foreachBatch ────────────────────────────────────
    db_query = (
        parsed.writeStream
        .foreachBatch(upsert_geo_heatmap)
        .option("checkpointLocation", os.path.join(checkpoint, "db"))
        .outputMode("append")
        .queryName("geo_db")
        .start()
    )

    log.info("✅ Job 3.3 — Geo Aggregator running")
    return [kafka_query, db_query]