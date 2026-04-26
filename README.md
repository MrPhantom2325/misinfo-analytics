# 🛡️ Global Misinformation Propagation Analytics

Real-time misinformation detection and propagation analytics pipeline.

---

## Architecture

```
[Producer] → [Kafka] → [Spark Streaming] → [SQLite/CSV] → [Dash Dashboard]
```

| Service      | Port | Purpose                   |
| ------------ | ---- | ------------------------- |
| Kafka Broker | 9092 | Message broker            |
| Kafka UI     | 8080 | Topic/message inspector   |
| Spark Master | 8090 | Spark web UI              |
| Dashboard    | 8050 | Plotly Dash visualization |

---

## Quick Start

### Prerequisites

- Docker + Docker Compose installed
- At least 4GB RAM available for Docker

### Phase 1 & 2: Start Kafka + Producer

```bash
# 1. Clone / enter project directory
cd misinfo-analytics

# 2. Start all services
docker-compose up -d

# 3. Watch producer logs
docker-compose logs -f producer

# 4. Inspect Kafka topics
open http://localhost:8080
```

### Run Producer Locally (without Docker)

```bash
cd producer
pip install -r requirements.txt

# Start Kafka first (via docker-compose), then:
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
PRODUCER_RATE=5 \
BURST_INTERVAL=30 \
python producer.py
```

---

## Kafka Topics

| Topic             | Description                          | Key      |
| ----------------- | ------------------------------------ | -------- |
| `raw-articles`    | All incoming misinformation articles | category |
| `flagged-misinfo` | credibility_score < 0.4 (Phase 3)    | category |
| `geo-spread`      | Country-level propagation (Phase 3)  | country  |
| `viral-alerts`    | share_velocity > 5000                | category |

---

## Article Schema

```json
{
  "article_id": "uuid",
  "headline": "BREAKING: Vaccines cause autism — suppressed by CDC",
  "source_domain": "naturalcures247.net",
  "category": "health | politics | climate | finance",
  "credibility_score": 0.08,
  "timestamp": "2026-04-26T10:30:00Z",
  "geo_origin": "US",
  "platform": "twitter | facebook | telegram | tiktok | ...",
  "share_velocity": 12500,
  "sentiment_score": -0.92,
  "keyword_flags": ["vaccine", "autism", "cover-up"]
}
```

---

## Producer Environment Variables

| Variable                   | Default        | Description                           |
| -------------------------- | -------------- | ------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS`  | localhost:9092 | Kafka broker address                  |
| `PRODUCER_RATE`            | 10             | Messages per second (normal)          |
| `BURST_INTERVAL`           | 60             | Seconds between burst campaigns       |
| `BURST_MULTIPLIER`         | 10             | Rate multiplier during burst          |
| `VIRAL_VELOCITY_THRESHOLD` | 5000           | share_velocity to trigger viral alert |

---

## Burst Campaign Simulation

The producer automatically triggers coordinated "campaign bursts" at the configured interval.
Campaigns simulate:

- **Health Disinformation Wave** — floods US/GB/AU on Twitter + Facebook
- **Election Fraud Narrative** — floods US on Truth Social + Gab
- **Russian Geo-Targeted Campaign** — floods RU/UA on Telegram
- **Financial Panic Campaign** — floods US/EU on YouTube + Rumble

---

## Project Structure

```
misinfo-analytics/
├── docker-compose.yml          ← Phase 1: All services
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── producer.py             ← Phase 2: Kafka producer
│   └── sample_data.py          ← Headline templates & data pools
├── spark/
│   ├── Dockerfile
│   ├── credibility_filter.py   ← Phase 3 (coming)
│   ├── velocity_analyzer.py    ← Phase 3 (coming)
│   ├── geo_aggregator.py       ← Phase 3 (coming)
│   └── category_trends.py      ← Phase 3 (coming)
├── dashboard/
│   └── app.py                  ← Phase 5 (coming)
├── data/
│   └── sample_articles.json    ← Static sample data
└── README.md
```

---

## Stopping Everything

```bash
docker-compose down           # stop containers
docker-compose down -v        # stop + remove volumes
```
