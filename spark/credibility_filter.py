"""
credibility_filter.py — Spark Streaming Job 3.1
────────────────────────────────────────────────
Consumes: raw-articles
Produces: flagged-misinfo  (credibility_score < CREDIBILITY_THRESHOLD)

Logic:
  • Parse JSON from Kafka
  • Filter articles where credibility_score < 0.4
  • Re-publish full article JSON to flagged-misinfo topic
  • Write a running count CSV to data/ for dashboard fallback polling
"""

import json
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from shared import (
    ARTICLE_SCHEMA,
    CHECKPOINT_DIR,
    CREDIBILITY_THRESHOLD,
    DATA_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_FLAGGED,
    TOPIC_RAW,
)

log = logging.getLogger(__name__)


def run(spark: SparkSession) -> None:
    log.info("▶  Job 3.1 — Credibility Filter starting")

    checkpoint = os.path.join(CHECKPOINT_DIR, "credibility_filter")

    # ── Read raw-articles from Kafka ──────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON payload ────────────────────────────────────────────────────
    parsed = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast(StringType()),
                ARTICLE_SCHEMA,
            ).alias("data"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("data.*", "kafka_ts")
        .filter(F.col("article_id").isNotNull())
    )

    # ── Filter: credibility below threshold ───────────────────────────────────
    flagged = parsed.filter(
        F.col("credibility_score") < CREDIBILITY_THRESHOLD
    )

    # ── Serialize back to JSON for Kafka output ───────────────────────────────
    flagged_for_kafka = flagged.select(
        F.col("category").alias("key"),
        F.to_json(
            F.struct(
                "article_id", "headline", "source_domain", "category",
                "credibility_score", "timestamp", "geo_origin", "platform",
                "share_velocity", "sentiment_score", "keyword_flags",
            )
        ).alias("value"),
    )

    # ── Write to flagged-misinfo Kafka topic ──────────────────────────────────
    kafka_query = (
        flagged_for_kafka.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", TOPIC_FLAGGED)
        .option("checkpointLocation", os.path.join(checkpoint, "kafka"))
        .outputMode("append")
        .queryName("credibility_filter_kafka")
        .start()
    )

    # ── Also write running stats to CSV for dashboard fallback ────────────────
    # Aggregate per micro-batch: count flagged articles by category
    def write_flagged_stats(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        stats = (
            batch_df
            .groupBy("category")
            .agg(
                F.count("*").alias("flagged_count"),
                F.avg("credibility_score").alias("avg_credibility"),
                F.avg("sentiment_score").alias("avg_sentiment"),
                F.avg("share_velocity").alias("avg_velocity"),
            )
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("captured_at", F.lit(str(F.current_timestamp())))
        )
        out_path = os.path.join(DATA_DIR, "flagged_stats.csv")
        pdf = stats.toPandas()
        header = not os.path.exists(out_path)
        pdf.to_csv(out_path, mode="a", header=header, index=False)

    csv_query = (
        flagged.writeStream
        .foreachBatch(write_flagged_stats)
        .option("checkpointLocation", os.path.join(checkpoint, "csv"))
        .outputMode("append")
        .queryName("credibility_filter_csv")
        .start()
    )

    log.info("✅ Job 3.1 — Credibility Filter running")
    return [kafka_query, csv_query]