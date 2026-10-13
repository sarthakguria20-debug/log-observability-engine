-- =============================================================================
-- PulseLog Observability Engine — ClickHouse Write-Optimized Storage Schema
-- =============================================================================
-- Optimized for high-throughput append-only log ingestion (100k+ events/sec/node)
-- Key optimizations:
--   1. LowCardinality dictionary encoding for repetitive metadata columns
--   2. Specialized column codecs (DoubleDelta for monotonic timestamps, ZSTD(3) for raw text)
--   3. Sparse primary index ordered by (service, severity, toStartOfMinute(timestamp))
--   4. Data-skipping Bloom Filter & 4-gram indices to bypass irrelevant granules
--   5. Incremental Materialized Views for zero-scan real-time analytics & alerting
-- =============================================================================

CREATE DATABASE IF NOT EXISTS pulselog;

CREATE TABLE IF NOT EXISTS pulselog.logs_raw
(
    id              String                          CODEC(ZSTD(1)),
    timestamp       DateTime64(3, 'UTC')            CODEC(DoubleDelta, ZSTD(1)),
    ingested_at     DateTime64(3, 'UTC')            CODEC(DoubleDelta, ZSTD(1)),
    service         LowCardinality(String)          CODEC(ZSTD(1)),
    host            LowCardinality(String)          CODEC(ZSTD(1)),
    environment     LowCardinality(String)          CODEC(ZSTD(1)),
    severity        LowCardinality(String)          CODEC(ZSTD(1)),
    format          LowCardinality(String)          CODEC(ZSTD(1)),
    message         String                          CODEC(ZSTD(3)),
    trace_id        String                          CODEC(ZSTD(1)),
    span_id         String                          CODEC(ZSTD(1)),
    status_code     UInt16                          CODEC(T64, ZSTD(1)),
    latency_ms      Float64                         CODEC(Gorilla, ZSTD(1)),
    attributes      Map(LowCardinality(String), String) CODEC(ZSTD(2)),
    partition_id    UInt8                           CODEC(T64, ZSTD(1)),
    kafka_offset    UInt64                          CODEC(DoubleDelta, ZSTD(1)),
    raw_line        String                          CODEC(ZSTD(3)),

    -- Data Skipping Indices for fast needle-in-a-haystack searches (e.g. CRITICAL_ERROR or trace_id)
    INDEX idx_message_ngram message TYPE ngrambf_v1(4, 32768, 3, 0) GRANULARITY 2,
    INDEX idx_raw_token raw_line TYPE tokenbf_v1(16384, 3, 0) GRANULARITY 2,
    INDEX idx_trace_bloom trace_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
PRIMARY KEY (service, severity, toStartOfMinute(timestamp))
ORDER BY (service, severity, toStartOfMinute(timestamp), timestamp, id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 3600;

-- =============================================================================
-- 1-Minute Pre-Aggregated Rollup Table & Materialized View
-- Powers the React Analytics Histogram and Error-Rate Sliding Windows in <2ms
-- =============================================================================

CREATE TABLE IF NOT EXISTS pulselog.logs_1m_rollup
(
    bucket_minute   DateTime('UTC')                 CODEC(DoubleDelta, ZSTD(1)),
    service         LowCardinality(String),
    severity        LowCardinality(String),
    event_count     UInt64                          CODEC(T64, ZSTD(1)),
    critical_count  UInt64                          CODEC(T64, ZSTD(1)),
    latency_sum_ms  Float64,
    latency_max_ms  SimpleAggregateFunction(max, Float64)
)
ENGINE = SummingMergeTree((event_count, critical_count, latency_sum_ms))
PARTITION BY toYYYYMM(bucket_minute)
ORDER BY (bucket_minute, service, severity);

CREATE MATERIALIZED VIEW IF NOT EXISTS pulselog.logs_1m_rollup_mv
TO pulselog.logs_1m_rollup
AS SELECT
    toStartOfMinute(timestamp) AS bucket_minute,
    service,
    severity,
    count() AS event_count,
    countIf(positionCaseInsensitive(raw_line, 'CRITICAL_ERROR') > 0 OR severity = 'CRITICAL') AS critical_count,
    sum(latency_ms) AS latency_sum_ms,
    max(latency_ms) AS latency_max_ms
FROM pulselog.logs_raw
GROUP BY bucket_minute, service, severity;

