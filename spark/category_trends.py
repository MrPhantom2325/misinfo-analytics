"""
category_trends.py — Spark Streaming Jobs 3.4 + 3.5
─────────────────────────────────────────────────────
Job 3.4 — Category Trend Analyzer
  Consumes: raw-articles
  Produces: misinfo_trends table (SQLite)
  Logic: 5-min sliding window counts by category, avg credibility + sentiment

Job 3.5 — Viral Article Tracker
  Consumes: flagged-misinfo
  Produces: viral_articles table (SQLite)
  Logic: Insert any article with share_velocity > threshold into DB, keep last 1000
"""

import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from shared import (
    ARTICLE_SCHEMA,
    CHECKPOINT_DIR,
    CREDIBILITY_THRESHOLD,
    DB_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_FLAGGED,
    TOPIC_RAW,
    VIRAL_VELOCITY_THRESHOLD,
    get_conn,
)

log = logging.getLogger(__name__)

WINDOW_DURATION = "5 minutes"
SLIDE_DURATION  = "1 minute"
WATERMARK       = "2 minutes"


# ══════════════════════════════════════════════════════════════════════════════
# Job 3.4 — Category Trends
# ══════════════════════════════════════════════════════════════════════════════

def upsert_category_trends(batch_df: DataFrame, batch_id: int) -> None:
    """Write sliding-window category counts to misinfo_trends."""
    if batch_df.isEmpty():
        return

    trend_df = (
        batch_df
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION).alias("w"),
            "category",
        )
        .agg(
            F.count("*").alias("article_count"),
            F.avg("credibility_score").alias("avg_credibility"),
            F.avg("sentiment_score").alias("avg_sentiment"),
        )
        .withColumn("window_start", F.col("w.start").cast(StringType()))
        .withColumn("window_end",   F.col("w.end").cast(StringType()))
        .drop("w")
    )

    rows = trend_df.collect()
    if not rows:
        return

    con = get_conn(DB_PATH)
    cur = con.cursor()
    for row in rows:
        cur.execute("""
            INSERT INTO misinfo_trends
                (window_start, window_end, category, article_count,
                 avg_credibility, avg_sentiment)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(window_start, category) DO UPDATE SET
                article_count   = excluded.article_count,
                avg_credibility = excluded.avg_credibility,
                avg_sentiment   = excluded.avg_sentiment
        """, (
            row["window_start"],
            row["window_end"],
            row["category"],
            int(row["article_count"]),
            float(row["avg_credibility"]) if row["avg_credibility"] else None,
            float(row["avg_sentiment"])   if row["avg_sentiment"]   else None,
        ))
    con.commit()
    con.close()
    log.info(f"[3.4] Category trends: upserted {len(rows)} rows (batch {batch_id})")


def run_category_trends(spark: SparkSession) -> list:
    log.info("▶  Job 3.4 — Category Trend Analyzer starting")

    checkpoint = os.path.join(CHECKPOINT_DIR, "category_trends")

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

    db_query = (
        parsed.writeStream
        .foreachBatch(upsert_category_trends)
        .option("checkpointLocation", os.path.join(checkpoint, "db"))
        .outputMode("append")
        .queryName("category_trends_db")
        .start()
    )

    log.info("✅ Job 3.4 — Category Trend Analyzer running")
    return [db_query]


# ══════════════════════════════════════════════════════════════════════════════
# Job 3.5 — Viral Article Tracker (Source Domain Risk)
# ══════════════════════════════════════════════════════════════════════════════

def insert_viral_articles(batch_df: DataFrame, batch_id: int) -> None:
    """
    Insert high-velocity flagged articles into viral_articles table.
    Keeps rolling window of last 1000 rows.
    """
    if batch_df.isEmpty():
        return

    viral_df = batch_df.filter(
        F.col("share_velocity") >= VIRAL_VELOCITY_THRESHOLD
    )

    rows = viral_df.collect()
    if not rows:
        return

    con = get_conn(DB_PATH)
    cur = con.cursor()

    for row in rows:
        cur.execute("""
            INSERT OR REPLACE INTO viral_articles
                (article_id, headline, source_domain, category,
                 credibility_score, geo_origin, platform,
                 share_velocity, sentiment_score, event_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["article_id"],
            row["headline"],
            row["source_domain"],
            row["category"],
            float(row["credibility_score"]) if row["credibility_score"] else None,
            row["geo_origin"],
            row["platform"],
            int(row["share_velocity"])       if row["share_velocity"]    else 0,
            float(row["sentiment_score"])    if row["sentiment_score"]   else None,
            row["timestamp"],
        ))

    # Keep only last 1000 viral articles (rolling buffer)
    cur.execute("""
        DELETE FROM viral_articles
        WHERE article_id NOT IN (
            SELECT article_id FROM viral_articles
            ORDER BY inserted_at DESC
            LIMIT 1000
        )
    """)

    con.commit()
    con.close()
    log.info(f"[3.5] Viral articles: inserted {len(rows)} rows (batch {batch_id})")


def run_viral_tracker(spark: SparkSession) -> list:
    log.info("▶  Job 3.5 — Viral Article Tracker starting")

    checkpoint = os.path.join(CHECKPOINT_DIR, "viral_tracker")

    # Read from flagged-misinfo (already filtered by credibility_filter job)
    flagged_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_FLAGGED)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        flagged_stream
        .select(
            F.from_json(
                F.col("value").cast(StringType()),
                ARTICLE_SCHEMA,
            ).alias("data"),
        )
        .select("data.*")
        .filter(F.col("article_id").isNotNull())
    )

    db_query = (
        parsed.writeStream
        .foreachBatch(insert_viral_articles)
        .option("checkpointLocation", os.path.join(checkpoint, "db"))
        .outputMode("append")
        .queryName("viral_tracker_db")
        .start()
    )

    log.info("✅ Job 3.5 — Viral Article Tracker running")
    return [db_query]