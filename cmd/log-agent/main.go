package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pulselog/log-observability-engine/internal/model"
	"github.com/pulselog/log-observability-engine/internal/simulator"
)

// AgentRingBuffer is a bounded lock-free/channel ring buffer on the edge server with watermark load shedding.
type AgentRingBuffer struct {
	ch           chan model.RawLogEvent
	capacity     int
	droppedDebug atomic.Int64
}

func NewAgentRingBuffer(capacity int) *AgentRingBuffer {
	return &AgentRingBuffer{
		ch:       make(chan model.RawLogEvent, capacity),
		capacity: capacity,
	}
}

func (rb *AgentRingBuffer) Enqueue(ev model.RawLogEvent) bool {
	occ := float64(len(rb.ch)) / float64(rb.capacity) * 100.0
	// Edge load-shedding: drop DEBUG logs if local agent buffer is >85% full
	if occ >= 85.0 && (strings.Contains(ev.RawLine, "[DEBUG]") || strings.Contains(ev.RawLine, `"level":"debug"`)) {
		rb.droppedDebug.Add(1)
		return false
	}
	select {
	case rb.ch <- ev:
		return true
	default:
		rb.droppedDebug.Add(1)
		return false
	}
}

func main() {
	endpoint := flag.String("endpoint", "http://localhost:8080/api/v1/ingest", "Central ingestion pipeline HTTP endpoint")
	agentID := flag.String("agent-id", "edge-agent-01", "Unique identifier for this log agent")
	host := flag.String("host", "prod-node-01", "Server hostname attached to emitted logs")
	service := flag.String("service", "payment-service", "Default service name hint")
	filePath := flag.String("file", "", "Path to log file to tail continuously")
	useStdin := flag.Bool("stdin", false, "Stream log lines from standard input")
	simulate := flag.Bool("simulate", false, "Run continuous multi-service log generator")
	injectCritical := flag.Bool("inject-critical", false, "Send an immediate CRITICAL_ERROR burst and exit")
	burstCount := flag.Int("burst", 0, "Send a one-shot high-volume burst of N logs and exit")
	ratePerSec := flag.Int("rate", 25, "Events per second when running in --simulate mode")
	batchSize := flag.Int("batch-size", 50, "Maximum events per micro-batch")
	lingerMs := flag.Int("linger-ms", 250, "Maximum time (ms) to wait before flushing a partial batch")
	bufferCap := flag.Int("buffer-capacity", 4096, "Local ring buffer capacity before backpressure shedding")
	useGzip := flag.Bool("gzip", false, "Compress batch payloads using gzip")
	flag.Parse()

	client := &http.Client{Timeout: 5 * time.Second}

	// One-shot CRITICAL_ERROR injection mode
	if *injectCritical {
		batch := simulator.GenerateRealisticBatch(5, "critical")
		batch.AgentID = *agentID
		sendWithBackoff(client, *endpoint, batch, *useGzip)
		fmt.Printf("[PulseLog Agent] Dispatched %d CRITICAL_ERROR events to %s\n", len(batch.Events), *endpoint)
		return
	}

	// One-shot high-volume backpressure burst mode
	if *burstCount > 0 {
		batch := simulator.GenerateRealisticBatch(*burstCount, "backpressure_spike")
		batch.AgentID = *agentID
		sendWithBackoff(client, *endpoint, batch, *useGzip)
		fmt.Printf("[PulseLog Agent] Dispatched burst of %d events to %s\n", len(batch.Events), *endpoint)
		return
	}

	ring := NewAgentRingBuffer(*bufferCap)
	stopCh := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start background batch shipper goroutine with adaptive backpressure
	go func() {
		ticker := time.NewTicker(time.Duration(*lingerMs) * time.Millisecond)
		defer ticker.Stop()
		batchEvents := make([]model.RawLogEvent, 0, *batchSize)

		flush := func() {
			if len(batchEvents) == 0 {
				return
			}
			occPct := float64(len(ring.ch)) / float64(ring.capacity) * 100.0
			batch := model.LogBatch{
				BatchID:           fmt.Sprintf("%s-%d", *agentID, time.Now().UnixNano()),
				AgentID:           *agentID,
				SentAt:            time.Now().UTC(),
				Compressed:        *useGzip || occPct >= 70.0,
				DroppedDebugCount: ring.droppedDebug.Swap(0),
				QueueOccupancyPct: occPct,
				Events:            batchEvents,
			}
			backpressureDelay := sendWithBackoff(client, *endpoint, batch, batch.Compressed)
			batchEvents = make([]model.RawLogEvent, 0, *batchSize)
			if backpressureDelay > 0 {
				time.Sleep(backpressureDelay)
			}
		}

		for {
			select {
			case <-stopCh:
				flush()
				return
			case ev := <-ring.ch:
				batchEvents = append(batchEvents, ev)
				if len(batchEvents) >= *batchSize {
					flush()
				}
			case <-ticker.C:
				flush()
			}
		}
	}()

	log.Printf("[PulseLog Agent] Started agent=%s host=%s endpoint=%s buffer=%d batch=%d linger=%dms",
		*agentID, *host, *endpoint, *bufferCap, *batchSize, *lingerMs)

	switch {
	case *useStdin:
		scanner := bufio.NewScanner(os.Stdin)
		var offset int64
		for scanner.Scan() {
			offset++
			ring.Enqueue(model.RawLogEvent{
				AgentID:     *agentID,
				Host:        *host,
				ServiceHint: *service,
				SourcePath:  "stdin",
				Offset:      offset,
				Timestamp:   time.Now().UTC(),
				RawLine:     scanner.Text(),
			})
		}
		time.Sleep(350 * time.Millisecond)
		close(stopCh)

	case *filePath != "":
		go tailFile(*filePath, *agentID, *host, *service, ring, stopCh)
		<-sigCh
		close(stopCh)

	case *simulate:
		go func() {
			interval := time.Second / time.Duration(maxInt(*ratePerSec, 1))
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					b := simulator.GenerateRealisticBatch(1, "standard")
					for _, ev := range b.Events {
						ring.Enqueue(ev)
					}
				}
			}
		}()
		<-sigCh
		close(stopCh)

	default:
		fmt.Println("Usage: log-agent [--file /path/to.log | --stdin | --simulate | --inject-critical | --burst N]")
		close(stopCh)
	}
}

func tailFile(path, agentID, host, service string, ring *AgentRingBuffer, stopCh <-chan struct{}) {
	var offset int64
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		f, err := os.Open(path)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		info, err := f.Stat()
		if err == nil && info.Size() < offset {
			// File was truncated or rotated
			offset = 0
		}
		_, _ = f.Seek(offset, io.SeekStart)
		reader := bufio.NewReader(f)

		for {
			line, err := reader.ReadString('\n')
			if len(line) > 0 {
				offset += int64(len(line))
				clean := strings.TrimRight(line, "\r\n")
				if clean != "" {
					ring.Enqueue(model.RawLogEvent{
						AgentID:     agentID,
						Host:        host,
						ServiceHint: service,
						SourcePath:  path,
						Offset:      offset,
						Timestamp:   time.Now().UTC(),
						RawLine:     clean,
					})
				}
			}
			if err != nil {
				break
			}
		}
		_ = f.Close()
		time.Sleep(200 * time.Millisecond)
	}
}

func sendWithBackoff(client *http.Client, endpoint string, batch model.LogBatch, compress bool) time.Duration {
	rawJSON, err := json.Marshal(batch)
	if err != nil {
		return 0
	}

	var payload []byte
	contentEncoding := ""
	if compress {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, _ = gz.Write(rawJSON)
		_ = gz.Close()
		payload = buf.Bytes()
		contentEncoding = "gzip"
	} else {
		payload = rawJSON
	}

	maxRetries := 4
	baseBackoff := 80 * time.Millisecond

	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return 0
		}
		req.Header.Set("Content-Type", "application/json")
		if contentEncoding != "" {
			req.Header.Set("Content-Encoding", contentEncoding)
		}

		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			bpState := resp.Header.Get("X-Backpressure-State")
			if resp.StatusCode == http.StatusTooManyRequests || bpState == string(model.BackpressureCircuitOpen) {
				// Exponential backoff with full jitter on HTTP 429 / Circuit Open
				jitter := time.Duration(rand.Intn(60)) * time.Millisecond
				sleepDur := baseBackoff*(1<<attempt) + jitter
				log.Printf("[PulseLog Agent] Downstream backpressure 429 (%s) — backing off for %v", bpState, sleepDur)
				time.Sleep(sleepDur)
				continue
			}
			if bpState == string(model.BackpressureSoftThrottle) || bpState == string(model.BackpressureSheddingDebug) {
				return 50 * time.Millisecond
			}
			return 0
		}

		time.Sleep(baseBackoff * (1 << attempt))
	}
	return 200 * time.Millisecond
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

