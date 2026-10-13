package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pulselog/log-observability-engine/internal/alerting"
	"github.com/pulselog/log-observability-engine/internal/broker"
	"github.com/pulselog/log-observability-engine/internal/model"
	"github.com/pulselog/log-observability-engine/internal/parser"
	"github.com/pulselog/log-observability-engine/internal/simulator"
	"github.com/pulselog/log-observability-engine/internal/storage"
)

type SSEHub struct {
	mu      sync.Mutex
	clients map[chan string]struct{}
}

func NewSSEHub() *SSEHub {
	return &SSEHub{clients: make(map[chan string]struct{})}
}

func (h *SSEHub) Broadcast(eventType string, payload interface{}) {
	b, err := json.Marshal(map[string]interface{}{
		"type":      eventType,
		"timestamp": time.Now().UTC(),
		"data":      payload,
	})
	if err != nil {
		return
	}
	msg := string(b)
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address for ingestion API and React Analytics Dashboard")
	dashboardDir := flag.String("dashboard", "./dashboard", "Path to React dashboard static assets")
	autoSim := flag.Bool("auto-simulate", true, "Run continuous background microservice log simulator")
	flag.Parse()

	sseHub := NewSSEHub()
	logBroker := broker.NewBroker(4, 1600, 65.0, 85.0)
	logParser := parser.NewLogParser("production")
	colStore := storage.NewColumnarEngine(200)
	alertEngine := alerting.NewEngine(
		os.Getenv("SLACK_WEBHOOK_URL"),
		os.Getenv("ALERT_EMAIL_TO"),
		func(inc model.AlertIncident) {
			sseHub.Broadcast("alert", inc)
		},
	)

	var simulatorEnabled atomic.Bool
	simulatorEnabled.Store(*autoSim)

	// Seed initial historical + live data so the dashboard is rich immediately on load
	seedBatch := simulator.GenerateRealisticBatch(420, "standard")
	for idx, ev := range seedBatch.Events {
		rec := logParser.Parse(ev, idx%4, int64(idx+1))
		colStore.InsertBatch([]model.StructuredLogRecord{rec})
	}
	colStore.Flush()

	// Start 1 consumer worker goroutine per Kafka-style partition
	for _, part := range logBroker.Partitions() {
		go func(pq *broker.PartitionQueue) {
			batchBuf := make([]model.StructuredLogRecord, 0, 32)
			ticker := time.NewTicker(120 * time.Millisecond)
			defer ticker.Stop()

			flushBatch := func() {
				if len(batchBuf) == 0 {
					return
				}
				colStore.InsertBatch(batchBuf)
				sseHub.Broadcast("logs", batchBuf[len(batchBuf)-1])
				batchBuf = batchBuf[:0]
			}

			for {
				select {
				case item := <-pq.Ch:
					if delayMs := logBroker.GetSimulatedSlowdownMs(); delayMs > 0 {
						time.Sleep(time.Duration(delayMs) * time.Millisecond)
					}
					parsed := logParser.Parse(item.Event, item.Partition, item.Offset)
					logBroker.MarkConsumed(item.Partition, item.Offset)
					alertEngine.Evaluate(parsed)
					batchBuf = append(batchBuf, parsed)
					if len(batchBuf) >= 32 {
						flushBatch()
					}
				case <-ticker.C:
					flushBatch()
				}
			}
		}(part)
	}

	// Background simulator goroutine
	go func() {
		ticker := time.NewTicker(600 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if !simulatorEnabled.Load() {
				continue
			}
			b := simulator.GenerateRealisticBatch(4, "standard")
			_, _, _, _ = logBroker.PublishBatch(b)
		}
	}()

	mux := http.NewServeMux()

	// 1. Ingestion Endpoint (called by CLI log-agent)
	mux.HandleFunc("/api/v1/ingest", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var reader io.Reader = r.Body
		if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, "invalid gzip body", http.StatusBadRequest)
				return
			}
			defer gz.Close()
			reader = gz
		}

		var batch model.LogBatch
		if err := json.NewDecoder(reader).Decode(&batch); err != nil {
			http.Error(w, "invalid JSON batch: "+err.Error(), http.StatusBadRequest)
			return
		}

		accepted, shed, bpState, err := logBroker.PublishBatch(batch)
		w.Header().Set("X-Backpressure-State", string(bpState))
		if err == broker.ErrBrokerCircuitOpen {
			w.WriteHeader(http.StatusTooManyRequests)
		} else {
			w.WriteHeader(http.StatusAccepted)
		}
		writeJSON(w, map[string]interface{}{
			"status":             "ok",
			"accepted":           accepted,
			"shed_debug":         shed,
			"backpressure_state": bpState,
		})
	})

	// 2. Query & Search Structured Logs Endpoint
	mux.HandleFunc("/api/v1/logs", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		q := r.URL.Query()
		limit, _ := strconv.Atoi(q.Get("limit"))
		minLat, _ := strconv.ParseFloat(q.Get("min_latency_ms"), 64)
		res := colStore.Query(storage.QueryFilter{
			Query:        q.Get("q"),
			Service:      q.Get("service"),
			Severity:     model.Severity(strings.ToUpper(q.Get("severity"))),
			Host:         q.Get("host"),
			TraceID:      q.Get("trace_id"),
			MinLatencyMs: minLat,
			Limit:        limit,
		})
		writeJSON(w, res)
	})

	// 3. Unified Pipeline Telemetry, Histograms & Backpressure Metrics Endpoint
	mux.HandleFunc("/api/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		buckets, byService, bySeverity := colStore.Aggregations()
		writeJSON(w, map[string]interface{}{
			"backpressure":      logBroker.Metrics(),
			"storage":           colStore.Stats(),
			"histogram":         buckets,
			"by_service":        byService,
			"by_severity":       bySeverity,
			"simulator_enabled": simulatorEnabled.Load(),
		})
	})

	// 4. Alerts & Incidents Endpoint
	mux.HandleFunc("/api/v1/alerts", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodPost {
			var body struct {
				IncidentID string `json:"incident_id"`
				Status     string `json:"status"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			alertEngine.UpdateIncidentStatus(body.IncidentID, body.Status)
		}
		writeJSON(w, map[string]interface{}{
			"incidents": alertEngine.Incidents(),
			"rules":     alertEngine.Rules(),
		})
	})

	// 5. Alert Rules Management Endpoint
	mux.HandleFunc("/api/v1/rules", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodPost {
			var rule model.AlertRule
			if err := json.NewDecoder(r.Body).Decode(&rule); err == nil {
				created := alertEngine.AddRule(rule)
				writeJSON(w, created)
				return
			}
		} else if r.Method == http.MethodPatch {
			var body struct {
				RuleID string `json:"rule_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			alertEngine.ToggleRule(body.RuleID)
		}
		writeJSON(w, alertEngine.Rules())
	})

	// 6. Interactive Chaos, Spike & CRITICAL_ERROR Simulator Endpoint
	mux.HandleFunc("/api/v1/simulate", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		var req struct {
			Action    string `json:"action"` // "critical", "error_burst", "backpressure_spike", "toggle_sim", "custom_log"
			Count     int    `json:"count"`
			RawLine   string `json:"raw_line"`
			Service   string `json:"service"`
			Host      string `json:"host"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		switch req.Action {
		case "toggle_sim":
			next := !simulatorEnabled.Load()
			simulatorEnabled.Store(next)
			writeJSON(w, map[string]interface{}{"simulator_enabled": next})
			return

		case "custom_log":
			if req.RawLine != "" {
				b := model.LogBatch{
					BatchID: fmt.Sprintf("custom-%d", time.Now().UnixNano()),
					AgentID: "dashboard-injector",
					SentAt:  time.Now().UTC(),
					Events: []model.RawLogEvent{{
						AgentID:     "dashboard-injector",
						Host:        fallback(req.Host, "prod-us-east-01"),
						ServiceHint: fallback(req.Service, "payment-service"),
						Timestamp:   time.Now().UTC(),
						RawLine:     req.RawLine,
					}},
				}
				acc, shed, state, _ := logBroker.PublishBatch(b)
				writeJSON(w, map[string]interface{}{"accepted": acc, "shed": shed, "state": state})
				return
			}

		case "backpressure_spike":
			count := req.Count
			if count <= 0 {
				count = 950
			}
			// Temporarily apply consumer slowdown so queue occupancy visibly climbs into SOFT_THROTTLE / SHEDDING_DEBUG
			logBroker.SetSimulatedSlowdownMs(8)
			go func() {
				time.Sleep(6 * time.Second)
				logBroker.SetSimulatedSlowdownMs(0)
			}()
			b := simulator.GenerateRealisticBatch(count, "backpressure_spike")
			acc, shed, state, _ := logBroker.PublishBatch(b)
			writeJSON(w, map[string]interface{}{
				"action":   req.Action,
				"accepted": acc,
				"shed":     shed,
				"state":    state,
			})
			return

		default:
			count := req.Count
			if count <= 0 {
				if req.Action == "critical" {
					count = 3
				} else {
					count = 15
				}
			}
			b := simulator.GenerateRealisticBatch(count, req.Action)
			acc, shed, state, _ := logBroker.PublishBatch(b)
			writeJSON(w, map[string]interface{}{
				"action":   req.Action,
				"accepted": acc,
				"shed":     shed,
				"state":    state,
			})
		}
	})

	// 7. Columnar Storage Flush & MergeTree Compaction Endpoint
	mux.HandleFunc("/api/v1/storage/compact", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		compacted := colStore.Compact()
		writeJSON(w, map[string]interface{}{
			"compacted": compacted,
			"storage":   colStore.Stats(),
		})
	})

	// 8. Backpressure Slowdown Simulation Endpoint
	mux.HandleFunc("/api/v1/backpressure/slowdown", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		var req struct {
			DelayMs int64 `json:"delay_ms"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		logBroker.SetSimulatedSlowdownMs(req.DelayMs)
		writeJSON(w, logBroker.Metrics())
	})

	// 9. Server-Sent Events (SSE) Live Telemetry Stream
	mux.HandleFunc("/api/v1/stream", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		ch := make(chan string, 32)
		sseHub.mu.Lock()
		sseHub.clients[ch] = struct{}{}
		sseHub.mu.Unlock()

		defer func() {
			sseHub.mu.Lock()
			delete(sseHub.clients, ch)
			sseHub.mu.Unlock()
		}()

		for {
			select {
			case <-r.Context().Done():
				return
			case msg := <-ch:
				_, _ = fmt.Fprintf(w, "data: %s\n\n", msg)
				flusher.Flush()
			}
		}
	})

	// Serve React Analytics Dashboard
	fs := http.FileServer(http.Dir(*dashboardDir))
	mux.Handle("/", fs)

	log.Printf("[PulseLog Engine] Listening on http://localhost%s (Dashboard: %s)", *addr, *dashboardDir)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Encoding")
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func fallback(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

