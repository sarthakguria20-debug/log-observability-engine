package parser

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pulselog/log-observability-engine/internal/model"
)

var (
	// Bracketed format: 2026-10-13T10:15:22.419Z [ERROR] [payment-service] message...
	reBracketed = regexp.MustCompile(`^(?P<ts>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\s+\[(?P<level>[A-Z]+)\]\s+\[(?P<service>[a-zA-Z0-9_.-]+)\]\s+(?P<msg>.*)$`)

	// Nginx / Apache combined log format
	reNginx = regexp.MustCompile(`^(?P<ip>\d+\.\d+\.\d+\.\d+)\s+\S+\s+\S+\s+\[(?P<ts>[^\]]+)\]\s+"(?P<method>[A-Z]+)\s+(?P<path>\S+)\s+[^"]+"\s+(?P<status>\d{3})\s+(?P<bytes>\d+)(?:\s+"[^"]*"\s+"(?P<ua>[^"]*)")?(?P<rest>.*)$`)

	// Syslog RFC3164/5424: <34>Oct 13 10:15:22 host app[123]: message
	reSyslog = regexp.MustCompile(`^(?:<(?P<pri>\d+)>)?(?P<ts>[A-Z][a-z]{2}\s+\d+\s+\d{2}:\d{2}:\d{2}|\d{4}-\d{2}-\d{2}T\S+)\s+(?P<host>\S+)\s+(?P<service>[a-zA-Z0-9_.-]+)(?:\[(?P<pid>\d+)\])?:\s*(?P<msg>.*)$`)

	// Key=Value attribute extractor
	reKeyValue = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_.-]*)=("[^"]*"|\S+)`)

	// Latency extractor (e.g., latency=432ms, duration=12.4ms, took 850ms)
	reLatency = regexp.MustCompile(`(?i)(?:latency|duration|elapsed|took)[=:\s]+(\d+(?:\.\d+)?)\s*(ms|s|us)`)

	// Non-alphanumeric tokenizer for inverted index & bloom filter
	reTokenSplit = regexp.MustCompile(`[^a-zA-Z0-9_]+`)
)

// LogParser transforms unstructured RawLogEvents into normalized, indexed StructuredLogRecords.
type LogParser struct {
	DefaultEnvironment string
}

func NewLogParser(defaultEnv string) *LogParser {
	if defaultEnv == "" {
		defaultEnv = "production"
	}
	return &LogParser{DefaultEnvironment: defaultEnv}
}

// Parse inspects a RawLogEvent, detects its format, extracts structured fields, attributes, and search tokens.
func (p *LogParser) Parse(raw model.RawLogEvent, partition int, offset int64) model.StructuredLogRecord {
	now := time.Now().UTC()
	ts := raw.Timestamp
	if ts.IsZero() {
		ts = now
	}

	line := strings.TrimSpace(raw.RawLine)
	rec := model.StructuredLogRecord{
		ID:          generateID(),
		Timestamp:   ts,
		IngestedAt:  now,
		Service:      fallbackString(raw.ServiceHint, "unknown-service"),
		Host:        fallbackString(raw.Host, "localhost"),
		Environment: p.DefaultEnvironment,
		Severity:    model.SeverityInfo,
		Format:      "unstructured",
		Message:     line,
		Attributes:  make(map[string]string),
		Partition:   partition,
		Offset:      offset,
		RawLine:     line,
	}

	for k, v := range raw.Labels {
		rec.Attributes[k] = v
	}

	// 1. Try JSON structured log
	if strings.HasPrefix(line, "{") && strings.HasSuffix(line, "}") {
		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(line), &payload); err == nil {
			rec.Format = "json"
			p.populateFromJSON(&rec, payload)
			p.finalizeRecord(&rec)
			return rec
		}
	}

	// 2. Try Bracketed Microservice Log
	if m := reBracketed.FindStringSubmatch(line); m != nil {
		rec.Format = "bracketed"
		if parsedTS, err := parseFlexibleTime(m[1]); err == nil {
			rec.Timestamp = parsedTS
		}
		rec.Severity = NormalizeSeverity(m[2])
		rec.Service = m[3]
		rec.Message = m[4]
		p.extractInlineMetadata(&rec, m[4])
		p.finalizeRecord(&rec)
		return rec
	}

	// 3. Try Nginx / Apache HTTP Access Log
	if m := reNginx.FindStringSubmatch(line); m != nil {
		rec.Format = "nginx"
		rec.Attributes["client_ip"] = m[1]
		rec.Attributes["http_method"] = m[3]
		rec.Attributes["http_path"] = m[4]
		if status, err := strconv.Atoi(m[5]); err == nil {
			rec.StatusCode = status
			if status >= 500 {
				rec.Severity = model.SeverityError
			} else if status >= 400 {
				rec.Severity = model.SeverityWarn
			}
		}
		rec.Attributes["response_bytes"] = m[6]
		if m[7] != "" {
			rec.Attributes["user_agent"] = m[7]
		}
		if rec.Service == "unknown-service" {
			rec.Service = "api-gateway"
		}
		rec.Message = fmt.Sprintf("%s %s -> HTTP %d (%s bytes)%s", m[3], m[4], rec.StatusCode, m[6], m[8])
		p.extractInlineMetadata(&rec, m[8])
		p.finalizeRecord(&rec)
		return rec
	}

	// 4. Try Syslog RFC3164 / RFC5424
	if m := reSyslog.FindStringSubmatch(line); m != nil {
		rec.Format = "syslog"
		if m[1] != "" {
			if pri, err := strconv.Atoi(m[1]); err == nil {
				sevCode := pri & 0x07
				switch {
				case sevCode <= 2:
					rec.Severity = model.SeverityCritical
				case sevCode == 3:
					rec.Severity = model.SeverityError
				case sevCode == 4:
					rec.Severity = model.SeverityWarn
				case sevCode <= 6:
					rec.Severity = model.SeverityInfo
				default:
					rec.Severity = model.SeverityDebug
				}
			}
		}
		rec.Host = m[3]
		rec.Service = m[4]
		if m[5] != "" {
			rec.Attributes["pid"] = m[5]
		}
		rec.Message = m[6]
		p.extractInlineMetadata(&rec, m[6])
		p.finalizeRecord(&rec)
		return rec
	}

	// 5. Fallback: Unstructured log analysis
	p.extractInlineMetadata(&rec, line)
	p.finalizeRecord(&rec)
	return rec
}

func (p *LogParser) populateFromJSON(rec *model.StructuredLogRecord, payload map[string]interface{}) {
	for k, v := range payload {
		strVal := fmt.Sprintf("%v", v)
		switch strings.ToLower(k) {
		case "level", "severity", "lvl":
			rec.Severity = NormalizeSeverity(strVal)
		case "msg", "message", "log":
			rec.Message = strVal
		case "service", "app", "component":
			rec.Service = strVal
		case "host", "hostname", "node":
			rec.Host = strVal
		case "env", "environment":
			rec.Environment = strVal
		case "trace_id", "traceid":
			rec.TraceID = strVal
		case "span_id", "spanid":
			rec.SpanID = strVal
		case "status", "status_code", "http_status":
			if code, err := strconv.Atoi(strVal); err == nil {
				rec.StatusCode = code
			}
		case "latency_ms", "duration_ms", "latency":
			if val, err := strconv.ParseFloat(strings.TrimSuffix(strVal, "ms"), 64); err == nil {
				rec.LatencyMs = val
			}
		case "ts", "timestamp", "time", "@timestamp":
			if ts, err := parseFlexibleTime(strVal); err == nil {
				rec.Timestamp = ts
			}
		default:
			rec.Attributes[k] = strVal
		}
	}
}

func (p *LogParser) extractInlineMetadata(rec *model.StructuredLogRecord, text string) {
	// Extract key=value pairs
	matches := reKeyValue.FindAllStringSubmatch(text, -1)
	for _, kv := range matches {
		key := strings.ToLower(kv[1])
		val := strings.Trim(kv[2], `"[]`)
		switch key {
		case "trace_id", "traceid":
			rec.TraceID = val
		case "span_id", "spanid":
			rec.SpanID = val
		case "service", "svc":
			rec.Service = val
		case "host", "node":
			rec.Host = val
		case "env":
			rec.Environment = val
		case "status", "status_code":
			if code, err := strconv.Atoi(val); err == nil {
				rec.StatusCode = code
			}
		case "latency", "latency_ms", "duration":
			clean := strings.TrimSuffix(val, "ms")
			if f, err := strconv.ParseFloat(clean, 64); err == nil {
				rec.LatencyMs = f
			}
		default:
			rec.Attributes[key] = val
		}
	}

	// Extract latency if not already set
	if rec.LatencyMs == 0 {
		if lm := reLatency.FindStringSubmatch(text); lm != nil {
			if val, err := strconv.ParseFloat(lm[1], 64); err == nil {
				unit := strings.ToLower(lm[2])
				switch unit {
				case "s":
					rec.LatencyMs = val * 1000.0
				case "us":
					rec.LatencyMs = val / 1000.0
				default:
					rec.LatencyMs = val
				}
			}
		}
	}
}

func (p *LogParser) finalizeRecord(rec *model.StructuredLogRecord) {
	upperMsg := strings.ToUpper(rec.RawLine)
	if strings.Contains(upperMsg, "CRITICAL_ERROR") || strings.Contains(upperMsg, "FATAL") || strings.Contains(upperMsg, "PANIC") || strings.Contains(upperMsg, "OOM_KILLED") {
		rec.Severity = model.SeverityCritical
	} else if rec.Severity == model.SeverityInfo {
		switch {
		case strings.Contains(upperMsg, "ERROR") || strings.Contains(upperMsg, "EXCEPTION") || strings.Contains(upperMsg, "FAIL"):
			rec.Severity = model.SeverityError
		case strings.Contains(upperMsg, "WARN") || strings.Contains(upperMsg, "TIMEOUT") || strings.Contains(upperMsg, "SLOW"):
			rec.Severity = model.SeverityWarn
		case strings.Contains(upperMsg, "DEBUG") || strings.Contains(upperMsg, "TRACE"):
			rec.Severity = model.SeverityDebug
		}
	}

	// Build full-text search tokens (normalized lowercase terms >= 2 chars)
	rawTokens := reTokenSplit.Split(strings.ToLower(rec.RawLine), -1)
	seen := make(map[string]struct{}, len(rawTokens))
	tokens := make([]string, 0, len(rawTokens))
	for _, tok := range rawTokens {
		if len(tok) < 2 {
			continue
		}
		if _, exists := seen[tok]; !exists {
			seen[tok] = struct{}{}
			tokens = append(tokens, tok)
		}
	}
	rec.Tokens = tokens
}

// NormalizeSeverity maps arbitrary log level strings to canonical model.Severity values.
func NormalizeSeverity(lvl string) model.Severity {
	switch strings.ToUpper(strings.TrimSpace(lvl)) {
	case "CRITICAL", "CRIT", "FATAL", "PANIC", "EMERG", "ALERT", "CRITICAL_ERROR":
		return model.SeverityCritical
	case "ERROR", "ERR", "SEVERE":
		return model.SeverityError
	case "WARN", "WARNING":
		return model.SeverityWarn
	case "DEBUG", "DBG", "TRACE", "VERBOSE":
		return model.SeverityDebug
	default:
		return model.SeverityInfo
	}
}

func parseFlexibleTime(s string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		"02/Jan/2006:15:04:05 -0700",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp: %s", s)
}

func fallbackString(val, def string) string {
	if strings.TrimSpace(val) == "" {
		return def
	}
	return val
}

func generateID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

