# PulseLog — Automated Observability & Log Aggregation Engine

A high-throughput, write-optimized log ingestion, structuring, alerting, and analytics pipeline inspired by **Datadog**, **Vector/Logstash**, **Apache Kafka**, and **ClickHouse**.

[![GitHub Pages Demo](https://img.shields.io/badge/GitHub%20Pages-Live%20Interactive%20Demo-brightgreen?style=for-the-badge&logo=github)](https://sarthakguria20-debug.github.io/log-observability-engine/)
[![Integration Tests](https://img.shields.io/badge/Test%20Suite-6%2F6%20Passed-blue?style=for-the-badge)](tests/e2e_test.py)

---

## 🌐 Live Interactive Demo (GitHub Pages)

Experience the full PulseLog engine directly in your browser with zero installation:

👉 **[https://sarthakguria20-debug.github.io/log-observability-engine/](https://sarthakguria20-debug.github.io/log-observability-engine/)**

- **Live Log Ingestion & Tailing**: Multi-microservice streaming (payment-service, auth-service, api-gateway, order-worker).
- **Interactive Chaos Controls**: Inject `CRITICAL_ERROR` bursts, test watermark backpressure, and trigger ClickHouse MergeTree compactions.
- **Alert Notifications**: Inspect real Slack Block Kit JSON and RFC 5322 Email payloads generated when incident rules trigger.
- **ClickHouse Skip Index**: Full-text and facet search demonstrating 512-bit Bloom filter partition pruning.

### Enabling GitHub Pages in GitHub Settings:
1. Navigate to your repository on GitHub: **Settings** → **Pages**.
2. Under **Build and deployment**:
   - **Source**: Select **GitHub Actions** (the automated workflow in `.github/workflows/deploy-pages.yml` handles deployment on every push).
   - *Or* select **Deploy from a branch** → Branch: `main` → Folder: `/docs` → **Save**.
3. Your live dashboard is immediately hosted at `https://<username>.github.io/<repo>/`!

---

## Architecture Overview

```
+-------------------------------------------------------------------------+
| Edge Servers / Microservices                                            |
|   payment-service | auth-service | api-gateway | order-worker           |
|                                  |                                      |
|                                  v                                      |
|  [PulseLog CLI Log Agent] (cmd/log-agent & bin/log-agent)               |
|    - Inode-aware file tailer + stdin streamer + traffic simulator       |
|    - Bounded Ring Buffer (85% edge DEBUG shedding watermark)            |
|    - Micro-batching (linger_ms / batch_size) + GZIP payload compression |
|    - HTTP 429 Circuit Breaker awareness + Full-Jitter Exponential Backoff|
+-------------------------------------------------------------------------+
                                   |
                        HTTP POST /api/v1/ingest
                                   v
+-------------------------------------------------------------------------+
| Central Ingestion Queue (internal/broker & Apache Kafka KRaft)          |
|    - 4 Topic Partitions consistently hashed by fnv1a(service_name) % 4  |
|    - Per-partition Producer/Consumer Offset & Lag telemetry             |
|    - Multi-Stage Watermark Backpressure Controller:                     |
|        * < 65%  (NORMAL)         : Zero delay, full admission           |
|        * 65-85% (SOFT_THROTTLE)  : Proportional producer delay          |
|        * 85-97% (SHEDDING_DEBUG) : Drops DEBUG/TRACE, keeps WARN/ERR    |
|        * >= 97% (CIRCUIT_OPEN)   : Returns HTTP 429 + Spills CRITICAL   |
+-------------------------------------------------------------------------+
                                   |
                    4 Concurrent Partition Workers
                                   v
+-----------------------------------+-------------------------------------+
| Unstructured Log Parser           | Real-Time Alerting Engine           |
| (internal/parser)                 | (internal/alerting)                 |
|  - Auto-detects JSON, Bracketed,  |  - Keyword Rules (CRITICAL_ERROR)   |
|    Nginx/Apache, Syslog RFC3164   |  - Sliding-Window Error Thresholds  |
|  - Inline key=value & latency_ms  |  - P99 Latency SLA Breach Rules     |
|  - Tokenizes for Inverted/Bloom   |  - Slack Block Kit + RFC5322 Email  |
+-----------------------------------+-------------------------------------+
                                   |
                                   v
+-------------------------------------------------------------------------+
| Write-Optimized Columnar Storage (clickhouse/schema.sql & internal/storage)|
|  - Append-only in-memory MemTable -> sorted immutable ColumnarParts     |
|  - Ordered byPRIMARY KEY (service, severity, toStartOfMinute(timestamp))|
|  - LowCardinality dictionary encoding + ZLIB/ZSTD columnar compression  |
|  - 512-bit Token Bloom Filter & Min/Max Skip Index (prunes granules)    |
|  - Background MergeTree Compaction + 1-Minute Rollup Materialized Views |
+-------------------------------------------------------------------------+
                                   |
                                   v
+-------------------------------------------------------------------------+
| React Analytics & Observability Dashboard (dashboard/)                  |
|  - Live log tailing, full-text & facet search, Bloom skip telemetry     |
|  - Stacked severity histogram & per-microservice volume breakdown       |
|  - Interactive Chaos/Spike controls & Slack/Email payload modal viewer  |
+-------------------------------------------------------------------------+
```

---

## Quick Start

### 1. Run the Self-Contained Stack Locally (Zero External Dependencies)

Start the central ingestion pipeline and React Analytics Dashboard on `http://localhost:8080`:

```bash
make run
```

In a second terminal, use the **CLI Log Agent** (`./bin/log-agent`) to stream logs, inject critical errors, or test backpressure spikes:

```bash
# Inject a CRITICAL_ERROR burst and trigger Slack + Email alert notifications
make inject-critical

# Send a 950-log burst to test Soft/Hard Watermark Backpressure & DEBUG shedding
make spike

# Stream arbitrary unstructured logs over stdin with GZIP batching
echo '2026-10-13T10:15:22Z [ERROR] [payment-service] CRITICAL_ERROR: Ledger deadlock trace_id=tr-991 latency=1420ms' | \
  ./bin/log-agent --stdin --gzip --service=payment-service
```

### 2. Run the Automated End-to-End Verification Suite

```bash
make test
```

### 3. Run with Docker Compose (Apache Kafka KRaft + ClickHouse Server + Go Services)

```bash
make docker-up
```

