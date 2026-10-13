package model

import "time"

// Severity represents normalized log severity levels ordered by criticality.
type Severity string

const (
	SeverityDebug    Severity = "DEBUG"
	SeverityInfo     Severity = "INFO"
	SeverityWarn     Severity = "WARN"
	SeverityError    Severity = "ERROR"
	SeverityCritical Severity = "CRITICAL"
)

// SeverityWeight returns numeric priority so backpressure load-shedding can drop low-priority logs first.
func SeverityWeight(s Severity) int {
	switch s {
	case SeverityCritical:
		return 50
	case SeverityError:
		return 40
	case SeverityWarn:
		return 30
	case SeverityInfo:
		return 20
	case SeverityDebug:
		return 10
	default:
		return 20
	}
}

// RawLogEvent is emitted by the lightweight CLI log agent before central parsing.
type RawLogEvent struct {
	AgentID      string            `json:"agent_id"`
	Host         string            `json:"host"`
	ServiceHint  string            `json:"service_hint,omitempty"`
	SourcePath   string            `json:"source_path"`
	Offset       int64             `json:"offset"`
	Timestamp    time.Time         `json:"timestamp"`
	RawLine      string            `json:"raw_line"`
	Labels       map[string]string `json:"labels,omitempty"`
}

// LogBatch is the wire payload shipped from the CLI agent to the ingestion broker.
type LogBatch struct {
	BatchID           string        `json:"batch_id"`
	AgentID           string        `json:"agent_id"`
	SentAt            time.Time     `json:"sent_at"`
	Compressed        bool          `json:"compressed"`
	DroppedDebugCount int64         `json:"dropped_debug_count"`
	QueueOccupancyPct float64       `json:"queue_occupancy_pct"`
	Events            []RawLogEvent `json:"events"`
}

// StructuredLogRecord is the canonical indexed document stored in ClickHouse / Columnar Engine.
type StructuredLogRecord struct {
	ID          string            `json:"id"`
	Timestamp   time.Time         `json:"timestamp"`
	IngestedAt  time.Time         `json:"ingested_at"`
	Service     string            `json:"service"`
	Host        string            `json:"host"`
	Environment string            `json:"environment"`
	Severity    Severity          `json:"severity"`
	Format      string            `json:"format"` // json, syslog, nginx, bracketed, unstructured
	Message     string            `json:"message"`
	TraceID     string            `json:"trace_id,omitempty"`
	SpanID      string            `json:"span_id,omitempty"`
	StatusCode  int               `json:"status_code,omitempty"`
	LatencyMs   float64           `json:"latency_ms,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	Tokens      []string          `json:"tokens,omitempty"`
	Partition   int               `json:"partition"`
	Offset      int64             `json:"offset"`
	RawLine     string            `json:"raw_line"`
}

// BackpressureState describes the current flow-control state of the pipeline.
type BackpressureState string

const (
	BackpressureNormal        BackpressureState = "NORMAL"
	BackpressureSoftThrottle  BackpressureState = "SOFT_THROTTLE"
	BackpressureSheddingDebug BackpressureState = "SHEDDING_DEBUG"
	BackpressureCircuitOpen   BackpressureState = "CIRCUIT_OPEN"
)

// BackpressureMetrics captures real-time queue depth, watermarks, and load shedding telemetry.
type BackpressureMetrics struct {
	State                 BackpressureState `json:"state"`
	QueueDepth            int               `json:"queue_depth"`
	QueueCapacity         int               `json:"queue_capacity"`
	OccupancyPct          float64           `json:"occupancy_pct"`
	SoftWatermarkPct      float64           `json:"soft_watermark_pct"`
	HardWatermarkPct      float64           `json:"hard_watermark_pct"`
	DroppedDebugLogs      int64             `json:"dropped_debug_logs"`
	SpilledToDiskLogs     int64             `json:"spilled_to_disk_logs"`
	ThrottleDelayMs       int64             `json:"throttle_delay_ms"`
	PartitionLag          map[int]int64     `json:"partition_lag"`
	TotalLag              int64             `json:"total_lag"`
	CircuitBreakerTrips   int64             `json:"circuit_breaker_trips"`
	SimulatedSlowdownMs   int64             `json:"simulated_slowdown_ms"`
}

// StorageStats exposes ClickHouse / Columnar MergeTree write-optimization metrics.
type StorageStats struct {
	EngineType            string  `json:"engine_type"`
	TotalRows             int64   `json:"total_rows"`
	MemTableRows          int64   `json:"memtable_rows"`
	MemTableFlushLimit    int64   `json:"memtable_flush_limit"`
	ActiveColumnarParts   int     `json:"active_columnar_parts"`
	CompactionsCompleted  int64   `json:"compactions_completed"`
	UncompressedBytes     int64   `json:"uncompressed_bytes"`
	CompressedBytes       int64   `json:"compressed_bytes"`
	CompressionRatio      float64 `json:"compression_ratio"`
	BloomFilterQueries    int64   `json:"bloom_filter_queries"`
	PartsSkippedByIndex   int64   `json:"parts_skipped_by_index"`
	PartsScanned          int64   `json:"parts_scanned"`
	WriteThroughputPerSec float64 `json:"write_throughput_per_sec"`
}

// AlertRuleType distinguishes keyword triggers from sliding-window error threshold triggers.
type AlertRuleType string

const (
	AlertRuleKeyword   AlertRuleType = "KEYWORD"
	AlertRuleThreshold AlertRuleType = "THRESHOLD"
	AlertRuleLatency   AlertRuleType = "LATENCY_P99"
)

// AlertRule defines when an observability alert should fire.
type AlertRule struct {
	ID              string        `json:"id"`
	Name            string        `json:"name"`
	Type            AlertRuleType `json:"type"`
	Enabled         bool          `json:"enabled"`
	ServiceFilter   string        `json:"service_filter,omitempty"` // "" or "*" means all services
	Keyword         string        `json:"keyword,omitempty"`        // e.g. "CRITICAL_ERROR"
	SeverityFilter  Severity      `json:"severity_filter,omitempty"`
	ThresholdCount  int           `json:"threshold_count,omitempty"`
	WindowSeconds   int           `json:"window_seconds,omitempty"`
	CooldownSeconds int           `json:"cooldown_seconds"`
	Channels        []string      `json:"channels"` // ["slack", "email", "webhook"]
}

// AlertIncident represents a triggered alert event and its dispatched notification payloads.
type AlertIncident struct {
	ID             string            `json:"id"`
	RuleID         string            `json:"rule_id"`
	RuleName       string            `json:"rule_name"`
	RuleType       AlertRuleType     `json:"rule_type"`
	Severity       Severity          `json:"severity"`
	Status         string            `json:"status"` // FIRING, ACKNOWLEDGED, RESOLVED
	Service        string            `json:"service"`
	Host           string            `json:"host"`
	Summary        string            `json:"summary"`
	MatchedKeyword string            `json:"matched_keyword,omitempty"`
	ObservedValue  float64           `json:"observed_value"`
	ThresholdValue float64           `json:"threshold_value"`
	WindowSeconds  int               `json:"window_seconds"`
	SampleRawLog   string            `json:"sample_raw_log"`
	TriggeredAt    time.Time         `json:"triggered_at"`
	ChannelsSent   []string          `json:"channels_sent"`
	SlackPayload   string            `json:"slack_payload"`
	EmailPayload   string            `json:"email_payload"`
}

