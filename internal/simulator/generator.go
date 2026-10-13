package simulator

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pulselog/log-observability-engine/internal/model"
)

var (
	services = []string{"payment-service", "auth-service", "api-gateway", "order-worker", "search-indexer", "inventory-db"}
	hosts    = []string{"prod-us-east-01", "prod-us-east-02", "prod-eu-west-01", "prod-ap-south-01"}
	paths    = []string{"/api/v1/checkout", "/api/v1/auth/token", "/api/v2/orders", "/api/v1/search/query", "/api/v1/inventory/reserve"}
	methods  = []string{"GET", "POST", "PUT"}
)

// GenerateRealisticBatch produces a mixed-format batch of realistic microservice logs.
func GenerateRealisticBatch(count int, mode string) model.LogBatch {
	now := time.Now().UTC()
	events := make([]model.RawLogEvent, 0, count)

	for i := 0; i < count; i++ {
		svc := services[rand.Intn(len(services))]
		host := hosts[rand.Intn(len(hosts))]
		traceID := fmt.Sprintf("tr-%08x%04x", rand.Uint32(), rand.Intn(65535))
		var rawLine string

		switch mode {
		case "critical":
			rawLine = generateCriticalLine(now, svc, host, traceID, i)
		case "error_burst":
			rawLine = generateErrorBurstLine(now, svc, host, traceID, i)
		case "backpressure_spike":
			// 60% DEBUG logs so backpressure load-shedding visibly drops DEBUG while preserving WARN/ERROR/CRITICAL
			if i%5 < 3 {
				rawLine = fmt.Sprintf("%s [DEBUG] [%s] trace_id=%s span_id=sp-%04x Connection pool heartbeat check ok latency=%dms host=%s",
					now.Format(time.RFC3339Nano), svc, traceID, rand.Intn(9999), 2+rand.Intn(8), host)
			} else {
				rawLine = generateStandardLine(now, svc, host, traceID, i)
			}
		default:
			rawLine = generateStandardLine(now, svc, host, traceID, i)
		}

		events = append(events, model.RawLogEvent{
			AgentID:     "agent-" + host,
			Host:        host,
			ServiceHint: svc,
			SourcePath:  "/var/log/services/" + svc + ".log",
			Offset:      int64(i + 1),
			Timestamp:   now,
			RawLine:     rawLine,
		})
	}

	return model.LogBatch{
		BatchID: fmt.Sprintf("batch-%d", now.UnixNano()),
		AgentID: "pulselog-sim-agent",
		SentAt:  now,
		Events:  events,
	}
}

func generateStandardLine(now time.Time, svc, host, traceID string, idx int) string {
	roll := rand.Intn(100)
	switch {
	case roll < 30:
		// JSON structured log
		lvl := "info"
		msg := "Processed gRPC request successfully"
		lat := 12 + rand.Intn(85)
		if roll < 4 {
			lvl = "error"
			msg = "Downstream RPC deadline exceeded while querying shard"
			lat = 450 + rand.Intn(600)
		} else if roll < 10 {
			lvl = "warn"
			msg = "Retry attempt #2 triggered due to transient socket reset"
			lat = 180 + rand.Intn(220)
		}
		return fmt.Sprintf(`{"ts":"%s","level":"%s","service":"%s","host":"%s","trace_id":"%s","latency_ms":%d,"msg":"%s"}`,
			now.Format(time.RFC3339Nano), lvl, svc, host, traceID, lat, msg)

	case roll < 60:
		// Bracketed microservice format
		lvl := "INFO"
		lat := 8 + rand.Intn(60)
		msg := fmt.Sprintf("Worker transaction committed trace_id=%s latency=%dms shard=%d", traceID, lat, idx%8)
		if roll < 35 {
			lvl = "DEBUG"
			msg = fmt.Sprintf("Cache lookup hit key=session:%x trace_id=%s latency=2ms", rand.Intn(99999), traceID)
		} else if roll == 59 {
			lvl = "ERROR"
			lat = 920 + rand.Intn(400)
			msg = fmt.Sprintf("Database connection pool exhausted trace_id=%s latency=%dms active_conns=128", traceID, lat)
		}
		return fmt.Sprintf("%s [%s] [%s] %s", now.Format(time.RFC3339Nano), lvl, svc, msg)

	case roll < 85:
		// Nginx / API Gateway Combined Log format
		method := methods[rand.Intn(len(methods))]
		path := paths[rand.Intn(len(paths))]
		status := 200
		lat := 15 + rand.Intn(90)
		if roll == 84 {
			status = 502
			lat = 780 + rand.Intn(500)
		} else if roll >= 80 {
			status = 429
			lat = 45 + rand.Intn(50)
		}
		return fmt.Sprintf(`10.42.%d.%d - - [%s] "%s %s HTTP/1.1" %d %d "-" "PulseClient/2.4" trace_id=%s latency=%dms service=%s`,
			1+rand.Intn(250), 1+rand.Intn(250),
			now.Format("02/Jan/2006:15:04:05 -0700"),
			method, path, status, 256+rand.Intn(4096), traceID, lat, svc)

	default:
		// Syslog RFC3164 format
		pri := 30 // daemon.info
		msg := fmt.Sprintf("Healthcheck probe passed trace_id=%s latency=%dms", traceID, 5+rand.Intn(25))
		if roll == 99 {
			pri = 27 // daemon.err
			msg = fmt.Sprintf("TLS handshake failed from upstream peer trace_id=%s latency=310ms", traceID)
		}
		return fmt.Sprintf("<%d>%s %s %s[%d]: %s",
			pri, now.Format("Jan  2 15:04:05"), host, svc, 1000+rand.Intn(8000), msg)
	}
}

func generateCriticalLine(now time.Time, svc, host, traceID string, idx int) string {
	scenarios := []string{
		fmt.Sprintf(`%s [CRITICAL] [payment-service] CRITICAL_ERROR: Stripe settlement ledger checksum mismatch on shard-03 trace_id=%s latency=1840ms host=%s`,
			now.Format(time.RFC3339Nano), traceID, host),
		fmt.Sprintf(`{"ts":"%s","level":"critical","service":"inventory-db","host":"%s","trace_id":"%s","latency_ms":2410,"msg":"CRITICAL_ERROR: Primary WAL disk write failure (ENOSPC) — replica promotion initiated"}`,
			now.Format(time.RFC3339Nano), host, traceID),
		fmt.Sprintf(`<26>%s %s auth-service[4412]: CRITICAL_ERROR: HSM signing key vault unreachable after 3 retries trace_id=%s latency=1550ms`,
			now.Format("Jan  2 15:04:05"), host, traceID),
	}
	return scenarios[idx%len(scenarios)]
}

func generateErrorBurstLine(now time.Time, svc, host, traceID string, idx int) string {
	return fmt.Sprintf(`%s [ERROR] [%s] Upstream connection reset by peer during transaction commit trace_id=%s latency=%dms status=503 host=%s`,
		now.Format(time.RFC3339Nano), svc, traceID, 650+rand.Intn(700), host)
}

