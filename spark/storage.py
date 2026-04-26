"""
storage.py — Phase 4: Storage Query Layer
──────────────────────────────────────────
Provides clean read functions over the SQLite DB written by Spark jobs.
Imported by the Dash dashboard (Phase 5) and any ad-hoc analysis scripts.

Tables available:
  • misinfo_trends   — category counts per time window
  • geo_heatmap      — country-level spread data
  • viral_articles   — flagged high-velocity articles (last 1000)
  • domain_risk      — source domain risk rankings

Usage:
  from storage import get_category_trends, get_geo_heatmap, ...
"""

import os
import sqlite3
import logging
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

DB_PATH  = os.getenv("DB_PATH",  "/app/data/misinfo.db")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")


def _conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    return sqlite3.connect(db_path, check_same_thread=False)


# ─── misinfo_trends ────────────────────────────────────────────────────────────

def get_category_trends(
    last_n_windows: int = 20,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Rolling category breakdown for pie/bar charts.
    Returns: window_start, category, article_count, avg_credibility, avg_sentiment
    """
    sql = """
        SELECT window_start, window_end, category,
               article_count, avg_credibility, avg_sentiment
        FROM misinfo_trends
        ORDER BY window_start DESC
        LIMIT ?
    """
    try:
        con = _conn(db_path)
        df = pd.read_sql_query(sql, con, params=(last_n_windows * 4,))  # 4 categories
        con.close()
        return df
    except Exception as e:
        log.warning(f"get_category_trends error: {e}")
        return pd.DataFrame(columns=["window_start", "window_end", "category",
                                     "article_count", "avg_credibility", "avg_sentiment"])


def get_category_totals(db_path: str = DB_PATH) -> pd.DataFrame:
    """Aggregate total article counts by category (for pie chart)."""
    sql = """
        SELECT category,
               SUM(article_count) AS total_count,
               AVG(avg_credibility) AS avg_credibility,
               AVG(avg_sentiment) AS avg_sentiment
        FROM misinfo_trends
        GROUP BY category
        ORDER BY total_count DESC
    """
    try:
        con = _conn(db_path)
        df = pd.read_sql_query(sql, con)
        con.close()
        return df
    except Exception as e:
        log.warning(f"get_category_totals error: {e}")
        return pd.DataFrame(columns=["category", "total_count", "avg_credibility", "avg_sentiment"])


def get_trend_timeseries(
    category: Optional[str] = None,
    last_n: int = 50,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Time-series of article_count per window for line/area chart.
    Optionally filtered by category.
    """
    if category:
        sql = """
            SELECT window_start, category, article_count, avg_sentiment
            FROM misinfo_trends
            WHERE category = ?
            ORDER BY window_start DESC
            LIMIT ?
        """
        params = (category, last_n)
    else:
        sql = """
            SELECT window_start, category, article_count, avg_sentiment
            FROM misinfo_trends
            ORDER BY window_start DESC
            LIMIT ?
        """
        params = (last_n,)

    try:
        con = _conn(db_path)
        df = pd.read_sql_query(sql, con, params=params)
        con.close()
        return df.sort_values("window_start")
    except Exception as e:
        log.warning(f"get_trend_timeseries error: {e}")
        return pd.DataFrame(columns=["window_start", "category", "article_count", "avg_sentiment"])


# ─── geo_heatmap ───────────────────────────────────────────────────────────────

def get_geo_heatmap(
    last_n_windows: int = 5,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Country-level spread totals for choropleth map.
    Returns: geo_origin, total_articles, avg_velocity (aggregated across recent windows)
    """
    sql = """
        SELECT geo_origin,
               SUM(article_count) AS total_articles,
               AVG(avg_velocity)  AS avg_velocity,
               GROUP_CONCAT(DISTINCT category) AS categories
        FROM geo_heatmap
        WHERE window_start IN (
            SELECT DISTINCT window_start
            FROM geo_heatmap
            ORDER BY window_start DESC
            LIMIT ?
        )
        GROUP BY geo_origin
        ORDER BY total_articles DESC
    """
    try:
        con = _conn(db_path)
        df = pd.read_sql_query(sql, con, params=(last_n_windows,))
        con.close()
        return df
    except Exception as e:
        log.warning(f"get_geo_heatmap error: {e}")
        return pd.DataFrame(columns=["geo_origin", "total_articles", "avg_velocity", "categories"])


def get_geo_by_category(
    category: str,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """Geo heatmap filtered to a single category."""
    sql = """
        SELECT geo_origin, SUM(article_count) AS total_articles
        FROM geo_heatmap
        WHERE category = ?
        GROUP BY geo_origin
        ORDER BY total_articles DESC
    """
    try:
        con = _conn(db_path)
        df = pd.read_sql_query(sql, con, params=(category,))
        con.close()
        return df
    except Exception as e:
        log.warning(f"get_geo_by_category error: {e}")
        return pd.DataFrame(columns=["geo_origin", "total_articles"])


# ─── viral_articles ────────────────────────────────────────────────────────────

def get_viral_articles(
    limit: int = 50,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Latest high-velocity articles for the live feed table.
    Returns newest first.
    """
    sql = """
        SELECT article_id, headline, source_domain, category,
               credibility_score, geo_origin, platform,
               share_velocity, sentiment_score, event_time, inserted_at
        FROM viral_articles
        ORDER BY inserted_at DESC
        LIMIT ?
    """
    try:
        con = _conn(db_path)
        df = pd.read_sql_query(sql, con, params=(limit,))
        con.close()
        return df
    except Exception as e:
        log.warning(f"get_viral_articles error: {e}")
        return pd.DataFrame(columns=[
            "article_id", "headline", "source_domain", "category",
            "credibility_score", "geo_origin", "platform",
            "share_velocity", "sentiment_score", "event_time", "inserted_at",
        ])


def get_viral_velocity_series(
    last_n: int = 60,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Share velocity over time for the live velocity meter line chart.
    Bins articles into 1-minute buckets.
    """
    sql = """
        SELECT strftime('%Y-%m-%dT%H:%M:00', inserted_at) AS minute_bucket,
               AVG(share_velocity) AS avg_velocity,
               MAX(share_velocity) AS max_velocity,
               COUNT(*) AS article_count
        FROM viral_articles
        GROUP BY minute_bucket
        ORDER BY minute_bucket DESC
        LIMIT ?
    """
    try:
        con = _conn(db_path)
        df = pd.read_sql_query(sql, con, params=(last_n,))
        con.close()
        return df.sort_values("minute_bucket")
    except Exception as e:
        log.warning(f"get_viral_velocity_series error: {e}")
        return pd.DataFrame(columns=["minute_bucket", "avg_velocity", "max_velocity", "article_count"])


# ─── domain_risk ───────────────────────────────────────────────────────────────

def get_top_risky_domains(
    top_n: int = 10,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Top N most dangerous source domains for horizontal bar chart.
    Aggregated across all windows, ranked by composite risk_score.
    """
    sql = """
        SELECT source_domain,
               SUM(article_count)    AS total_articles,
               AVG(avg_credibility)  AS avg_credibility,
               MIN(min_credibility)  AS min_credibility,
               SUM(total_velocity)   AS total_velocity,
               MAX(risk_score)       AS peak_risk_score,
               AVG(risk_score)       AS avg_risk_score
        FROM domain_risk
        GROUP BY source_domain
        ORDER BY avg_risk_score DESC
        LIMIT ?
    """
    try:
        con = _conn(db_path)
        df = pd.read_sql_query(sql, con, params=(top_n,))
        con.close()
        return df
    except Exception as e:
        log.warning(f"get_top_risky_domains error: {e}")
        return pd.DataFrame(columns=[
            "source_domain", "total_articles", "avg_credibility",
            "min_credibility", "total_velocity", "peak_risk_score", "avg_risk_score",
        ])


def get_domain_risk_history(
    domain: str,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """Risk score history for a specific domain over time."""
    sql = """
        SELECT window_start, risk_score, article_count, avg_credibility
        FROM domain_risk
        WHERE source_domain = ?
        ORDER BY window_start ASC
    """
    try:
        con = _conn(db_path)
        df = pd.read_sql_query(sql, con, params=(domain,))
        con.close()
        return df
    except Exception as e:
        log.warning(f"get_domain_risk_history error: {e}")
        return pd.DataFrame(columns=["window_start", "risk_score", "article_count", "avg_credibility"])


# ─── Sentiment timeline ────────────────────────────────────────────────────────

def get_sentiment_timeline(
    last_n: int = 50,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Rolling average sentiment per category for area chart.
    """
    sql = """
        SELECT window_start, category, avg_sentiment, article_count
        FROM misinfo_trends
        WHERE avg_sentiment IS NOT NULL
        ORDER BY window_start DESC
        LIMIT ?
    """
    try:
        con = _conn(db_path)
        df = pd.read_sql_query(sql, con, params=(last_n * 4,))
        con.close()
        return df.sort_values("window_start")
    except Exception as e:
        log.warning(f"get_sentiment_timeline error: {e}")
        return pd.DataFrame(columns=["window_start", "category", "avg_sentiment", "article_count"])


# ─── Summary stats (for KPI header cards) ─────────────────────────────────────

def get_summary_stats(db_path: str = DB_PATH) -> dict:
    """
    Single-call summary for dashboard KPI cards.
    Returns dict with: total_articles, total_viral, active_domains,
                       countries_affected, avg_credibility, most_active_category
    """
    defaults = {
        "total_articles": 0,
        "total_viral": 0,
        "active_domains": 0,
        "countries_affected": 0,
        "avg_credibility": 0.0,
        "most_active_category": "—",
    }
    try:
        con = _conn(db_path)
        cur = con.cursor()

        cur.execute("SELECT COALESCE(SUM(article_count), 0) FROM misinfo_trends")
        defaults["total_articles"] = int(cur.fetchone()[0])

        cur.execute("SELECT COUNT(*) FROM viral_articles")
        defaults["total_viral"] = int(cur.fetchone()[0])

        cur.execute("SELECT COUNT(DISTINCT source_domain) FROM domain_risk")
        defaults["active_domains"] = int(cur.fetchone()[0])

        cur.execute("SELECT COUNT(DISTINCT geo_origin) FROM geo_heatmap")
        defaults["countries_affected"] = int(cur.fetchone()[0])

        cur.execute("SELECT AVG(avg_credibility) FROM misinfo_trends WHERE avg_credibility IS NOT NULL")
        val = cur.fetchone()[0]
        defaults["avg_credibility"] = round(float(val), 3) if val else 0.0

        cur.execute("""
            SELECT category FROM misinfo_trends
            GROUP BY category ORDER BY SUM(article_count) DESC LIMIT 1
        """)
        row = cur.fetchone()
        defaults["most_active_category"] = row[0] if row else "—"

        con.close()
    except Exception as e:
        log.warning(f"get_summary_stats error: {e}")

    return defaults