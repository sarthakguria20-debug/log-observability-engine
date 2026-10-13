package parser

import (
	"testing"
	"time"

	"github.com/pulselog/log-observability-engine/internal/model"
)

func TestParseMultiFormatLogs(t *testing.T) {
	p := NewLogParser("production")

	// 1. JSON log with CRITICAL_ERROR
	recJSON := p.Parse(model.RawLogEvent{
		Host:      "prod-node-01",
		Timestamp: time.Now().UTC(),
		RawLine:   `{"level":"error","service":"payment-service","trace_id":"tr-12345","latency_ms":912.5,"msg":"CRITICAL_ERROR: database deadlock detected"}`,
	}, 0, 1)

	if recJSON.Format != "json" {
		t.Fatalf("expected format json, got %s", recJSON.Format)
	}
	if recJSON.Severity != model.SeverityCritical {
		t.Fatalf("expected CRITICAL severity promotion for CRITICAL_ERROR, got %s", recJSON.Severity)
	}
	if recJSON.Service != "payment-service" || recJSON.TraceID != "tr-12345" || recJSON.LatencyMs != 912.5 {
		t.Fatalf("unexpected parsed fields: %+v", recJSON)
	}

	// 2. Nginx combined access log
	recNginx := p.Parse(model.RawLogEvent{
		Host:    "edge-gw-01",
		RawLine: `10.0.1.9 - - [13/Oct/2026:10:15:22 +0000] "POST /api/v1/checkout HTTP/1.1" 502 1024 "-" "Go-http/2.0" trace_id=tr-999 latency=640ms`,
	}, 1, 2)

	if recNginx.Format != "nginx" || recNginx.StatusCode != 502 || recNginx.Severity != model.SeverityError {
		t.Fatalf("unexpected nginx parse result: %+v", recNginx)
	}
	if recNginx.LatencyMs != 640 {
		t.Fatalf("expected latency 640ms, got %v", recNginx.LatencyMs)
	}

	// 3. Bracketed log
	recBrack := p.Parse(model.RawLogEvent{
		Host:    "worker-02",
		RawLine: `2026-10-13T10:15:22.419Z [WARN] [order-worker] Slow consumer lag detected trace_id=tr-abc latency=315ms`,
	}, 2, 3)

	if recBrack.Format != "bracketed" || recBrack.Service != "order-worker" || recBrack.Severity != model.SeverityWarn {
		t.Fatalf("unexpected bracketed parse result: %+v", recBrack)
	}
}

