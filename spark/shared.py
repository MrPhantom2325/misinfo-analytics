"""
shared.py — Common schema, DB setup, and utilities for all Spark jobs.
Imported by every analyzer module.
"""

import os
import sqlite3
import logging
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType, ArrayType, TimestampType,
)

log = logging.getLogger(__name__)

# ─── Environment ───────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_PATH = os.getenv("DB_PATH", "/app/data/misinfo.db")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/app/data/checkpoints")

VIRAL_VELOCITY_THRESHOLD = int(os.getenv("VIRAL_VELOCITY_THRESHOLD", "5000"))
CREDIBILITY_THRESHOLD    = float(os.getenv("CREDIBILITY_THRESHOLD", "0.4"))

# ─── Kafka topic names ─────────────────────────────────────────────────────────

TOPIC_RAW        = "raw-articles"
TOPIC_FLAGGED    = "flagged-misinfo"
TOPIC_GEO        = "geo-spread"
TOPIC_VIRAL      = "viral-alerts"

# ─── Article JSON schema (must match producer.py output exactly) ───────────────

ARTICLE_SCHEMA = StructType([
    StructField("article_id",        StringType(),              True),
    StructField("headline",          StringType(),              True),
    StructField("source_domain",     StringType(),              True),
    StructField("category",          StringType(),              True),
    StructField("credibility_score", FloatType(),               True),
    StructField("timestamp",         StringType(),              True),   # parsed later
    StructField("geo_origin",        StringType(),              True),
    StructField("platform",          StringType(),              True),
    StructField("share_velocity",    IntegerType(),             True),
    StructField("sentiment_score",   FloatType(),               True),
    StructField("keyword_flags",     ArrayType(StringType()),   True),
])

# ─── SQLite DB initialiser ─────────────────────────────────────────────────────

def init_db(db_path: str = DB_PATH) -> None:
    """Create all storage tables if they don't already exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.executescript("""
        -- Category trend counts per time window
        CREATE TABLE IF NOT EXISTS misinfo_trends (
            window_start    TEXT NOT NULL,
            window_end      TEXT NOT NULL,
            category        TEXT NOT NULL,
            article_count   INTEGER NOT NULL DEFAULT 0,
            avg_credibility REAL,
            avg_sentiment   REAL,
            inserted_at     TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (window_start, category)
        );

        -- Country-level spread data per window
        CREATE TABLE IF NOT EXISTS geo_heatmap (
            window_start  TEXT NOT NULL,
            window_end    TEXT NOT NULL,
            geo_origin    TEXT NOT NULL,
            category      TEXT NOT NULL,
            article_count INTEGER NOT NULL DEFAULT 0,
            avg_velocity  REAL,
            inserted_at   TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (window_start, geo_origin, category)
        );

        -- Flagged high-velocity viral articles (rolling, last 1000)
        CREATE TABLE IF NOT EXISTS viral_articles (
            article_id       TEXT PRIMARY KEY,
            headline         TEXT,
            source_domain    TEXT,
            category         TEXT,
            credibility_score REAL,
            geo_origin       TEXT,
            platform         TEXT,
            share_velocity   INTEGER,
            sentiment_score  REAL,
            event_time       TEXT,
            inserted_at      TEXT DEFAULT (datetime('now'))
        );

        -- Source domain risk rankings (upserted per window)
        CREATE TABLE IF NOT EXISTS domain_risk (
            window_start     TEXT NOT NULL,
            source_domain    TEXT NOT NULL,
            article_count    INTEGER NOT NULL DEFAULT 0,
            avg_credibility  REAL,
            min_credibility  REAL,
            total_velocity   INTEGER,
            risk_score       REAL,
            inserted_at      TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (window_start, source_domain)
        );

        -- Indexes for dashboard queries
        CREATE INDEX IF NOT EXISTS idx_trends_window   ON misinfo_trends  (window_start);
        CREATE INDEX IF NOT EXISTS idx_geo_window      ON geo_heatmap     (window_start);
        CREATE INDEX IF NOT EXISTS idx_viral_time      ON viral_articles  (event_time);
        CREATE INDEX IF NOT EXISTS idx_domain_window   ON domain_risk     (window_start);
        CREATE INDEX IF NOT EXISTS idx_viral_ins       ON viral_articles  (inserted_at);
    """)

    con.commit()
    con.close()
    log.info(f"✅ SQLite DB initialised at {db_path}")


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Return a new SQLite connection (thread-local — do NOT share across threads)."""
    return sqlite3.connect(db_path, check_same_thread=False)