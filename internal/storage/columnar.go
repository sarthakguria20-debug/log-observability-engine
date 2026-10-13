package storage

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pulselog/log-observability-engine/internal/model"
)

// ColumnarPart represents an immutable sorted MergeTree part with dictionary encoding and a Bloom filter skip index.
type ColumnarPart struct {
	PartID            int64
	CreatedAt         time.Time
	RowCount          int
	MinTimestamp      time.Time
	MaxTimestamp      time.Time
	ServiceDict       map[string]struct{}
	SeverityDict      map[model.Severity]struct{}
	BloomFilter       [256]uint64 // 16,384-bit token bloom filter matching ClickHouse tokenbf_v1(16384, 3, 0)
	UncompressedBytes int64
	CompressedBytes   int64
	Records           []model.StructuredLogRecord
}

// QueryFilter specifies search, facet, and analytical predicates for querying logs.
type QueryFilter struct {
	Query        string         `json:"query"`
	Service      string         `json:"service"`
	Severity     model.Severity `json:"severity"`
	Host         string         `json:"host"`
	TraceID      string         `json:"trace_id"`
	MinLatencyMs float64        `json:"min_latency_ms"`
	Limit        int            `json:"limit"`
}

// QueryResult returns matched logs along with ClickHouse-style execution & skip-index statistics.
type QueryResult struct {
	Records             []model.StructuredLogRecord `json:"records"`
	TotalMatched        int                         `json:"total_matched"`
	ExecutionTimeMs     float64                     `json:"execution_time_ms"`
	PartsScanned        int                         `json:"parts_scanned"`
	PartsSkippedByIndex int                         `json:"parts_skipped_by_index"`
}

// TimeBucket represents a 10-second or 1-minute histogram bar for the React dashboard.
type TimeBucket struct {
	Timestamp string `json:"timestamp"`
	Debug     int    `json:"debug"`
	Info      int    `json:"info"`
	Warn      int    `json:"warn"`
	Error     int    `json:"error"`
	Critical  int    `json:"critical"`
	Total     int    `json:"total"`
}

// ColumnarEngine implements a ClickHouse-inspired MergeTree columnar log store.
type ColumnarEngine struct {
	mu                   sync.RWMutex
	memTable             []model.StructuredLogRecord
	memTableFlushLimit   int
	parts                []*ColumnarPart
	nextPartID           atomic.Int64
	totalRows            atomic.Int64
	compactionsCompleted atomic.Int64
	uncompressedBytes    atomic.Int64
	compressedBytes      atomic.Int64
	bloomQueries         atomic.Int64
	partsSkipped         atomic.Int64
	partsScanned         atomic.Int64
	startedAt            time.Time
}

func NewColumnarEngine(flushLimit int) *ColumnarEngine {
	if flushLimit <= 0 {
		flushLimit = 250
	}
	return &ColumnarEngine{
		memTable:           make([]model.StructuredLogRecord, 0, flushLimit),
		memTableFlushLimit: flushLimit,
		parts:              make([]*ColumnarPart, 0, 64),
		startedAt:          time.Now(),
	}
}

// InsertBatch appends parsed structured logs into the MemTable and flushes an immutable ColumnarPart when full.
func (e *ColumnarEngine) InsertBatch(records []model.StructuredLogRecord) {
	if len(records) == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	e.memTable = append(e.memTable, records...)
	e.totalRows.Add(int64(len(records)))

	if len(e.memTable) >= e.memTableFlushLimit {
		e.flushMemTableLocked()
	}

	// Trigger background-style MergeTree compaction when active parts exceed threshold
	if len(e.parts) >= 10 {
		e.compactOldestPartsLocked(4)
	}
}

// Flush forces the current MemTable to flush into an immutable ColumnarPart.
func (e *ColumnarEngine) Flush() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.memTable) > 0 {
		e.flushMemTableLocked()
	}
}

// Compact merges up to N oldest immutable parts into a single sorted, higher-compression part.
func (e *ColumnarEngine) Compact() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.memTable) > 0 {
		e.flushMemTableLocked()
	}
	if len(e.parts) < 2 {
		return false
	}
	e.compactOldestPartsLocked(len(e.parts))
	return true
}

func (e *ColumnarEngine) flushMemTableLocked() {
	rows := make([]model.StructuredLogRecord, len(e.memTable))
	copy(rows, e.memTable)
	e.memTable = e.memTable[:0]

	part := e.buildColumnarPart(rows)
	e.parts = append(e.parts, part)
	e.uncompressedBytes.Add(part.UncompressedBytes)
	e.compressedBytes.Add(part.CompressedBytes)
}

func (e *ColumnarEngine) compactOldestPartsLocked(count int) {
	if count > len(e.parts) {
		count = len(e.parts)
	}
	if count < 2 {
		return
	}

	var mergedRows []model.StructuredLogRecord
	var oldUncompressed, oldCompressed int64
	for i := 0; i < count; i++ {
		mergedRows = append(mergedRows, e.parts[i].Records...)
		oldUncompressed += e.parts[i].UncompressedBytes
		oldCompressed += e.parts[i].CompressedBytes
	}

	compactedPart := e.buildColumnarPart(mergedRows)
	remaining := make([]*ColumnarPart, 0, len(e.parts)-count+1)
	remaining = append(remaining, compactedPart)
	remaining = append(remaining, e.parts[count:]...)
	e.parts = remaining

	e.uncompressedBytes.Add(compactedPart.UncompressedBytes - oldUncompressed)
	e.compressedBytes.Add(compactedPart.CompressedBytes - oldCompressed)
	e.compactionsCompleted.Add(1)
}

func (e *ColumnarEngine) buildColumnarPart(rows []model.StructuredLogRecord) *ColumnarPart {
	// Sort by (Service, Severity, Timestamp) just like ClickHousePRIMARY KEY (service, severity, timestamp)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Service != rows[j].Service {
			return rows[i].Service < rows[j].Service
		}
		if rows[i].Severity != rows[j].Severity {
			return rows[i].Severity < rows[j].Severity
		}
		return rows[i].Timestamp.Before(rows[j].Timestamp)
	})

	part := &ColumnarPart{
		PartID:       e.nextPartID.Add(1),
		CreatedAt:    time.Now().UTC(),
		RowCount:     len(rows),
		ServiceDict:  make(map[string]struct{}),
		SeverityDict: make(map[model.Severity]struct{}),
		Records:      rows,
	}

	var rawBuf bytes.Buffer
	for i, r := range rows {
		if i == 0 || r.Timestamp.Before(part.MinTimestamp) {
			part.MinTimestamp = r.Timestamp
		}
		if i == 0 || r.Timestamp.After(part.MaxTimestamp) {
			part.MaxTimestamp = r.Timestamp
		}
		part.ServiceDict[r.Service] = struct{}{}
		part.SeverityDict[r.Severity] = struct{}{}

		// Populate 512-bit Bloom filter with search tokens and trace_id
		for _, tok := range r.Tokens {
			bloomAdd(&part.BloomFilter, tok)
		}
		if r.TraceID != "" {
			bloomAdd(&part.BloomFilter, strings.ToLower(r.TraceID))
		}
		rawBuf.WriteString(r.RawLine)
		rawBuf.WriteByte('\n')
	}

	part.UncompressedBytes = int64(rawBuf.Len())
	var compBuf bytes.Buffer
	zw, _ := zlib.NewWriterLevel(&compBuf, zlib.BestCompression)
	_, _ = zw.Write(rawBuf.Bytes())
	_ = zw.Close()
	part.CompressedBytes = int64(compBuf.Len())

	return part
}

// Query executes a filtered search using dictionary & Bloom filter skip indices before scanning rows.
func (e *ColumnarEngine) Query(f QueryFilter) QueryResult {
	start := time.Now()
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}

	qLower := strings.ToLower(strings.TrimSpace(f.Query))
	queryTokens := extractQueryTokens(qLower)

	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(queryTokens) > 0 || f.Service != "" || f.Severity != "" {
		e.bloomQueries.Add(1)
	}

	var matched []model.StructuredLogRecord
	skipped := 0
	scanned := 0

	// 1. Scan in-memory MemTable (newest logs)
	for i := len(e.memTable) - 1; i >= 0; i-- {
		if matchesFilter(e.memTable[i], f, qLower) {
			matched = append(matched, e.memTable[i])
		}
	}

	// 2. Iterate through immutable ColumnarParts in reverse chronological order, applying Skip Indices
	for pIdx := len(e.parts) - 1; pIdx >= 0; pIdx-- {
		part := e.parts[pIdx]
		if canSkipPart(part, f, queryTokens) {
			skipped++
			continue
		}
		scanned++
		for i := len(part.Records) - 1; i >= 0; i-- {
			if matchesFilter(part.Records[i], f, qLower) {
				matched = append(matched, part.Records[i])
			}
		}
	}

	e.partsSkipped.Add(int64(skipped))
	e.partsScanned.Add(int64(scanned))

	// Sort matched records newest-first
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].Timestamp.After(matched[j].Timestamp)
	})

	totalMatched := len(matched)
	if len(matched) > f.Limit {
		matched = matched[:f.Limit]
	}

	elapsedMs := float64(time.Since(start).Microseconds()) / 1000.0
	return QueryResult{
		Records:             matched,
		TotalMatched:        totalMatched,
		ExecutionTimeMs:     elapsedMs,
		PartsScanned:        scanned,
		PartsSkippedByIndex: skipped,
	}
}

// Aggregations returns time-series histogram buckets and top-service breakdowns.
func (e *ColumnarEngine) Aggregations() (buckets []TimeBucket, byService map[string]int, bySeverity map[string]int) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	byService = make(map[string]int)
	bySeverity = map[string]int{
		"DEBUG":    0,
		"INFO":     0,
		"WARN":     0,
		"ERROR":    0,
		"CRITICAL": 0,
	}
	bucketMap := make(map[string]*TimeBucket)

	processRec := func(r model.StructuredLogRecord) {
		byService[r.Service]++
		bySeverity[string(r.Severity)]++

		// Group into 10-second time buckets for smooth real-time visualization
		sec := (r.Timestamp.Second() / 10) * 10
		bKey := r.Timestamp.Truncate(time.Minute).Add(time.Duration(sec) * time.Second).Format("15:04:05")
		tb, ok := bucketMap[bKey]
		if !ok {
			tb = &TimeBucket{Timestamp: bKey}
			bucketMap[bKey] = tb
		}
		switch r.Severity {
		case model.SeverityDebug:
			tb.Debug++
		case model.SeverityInfo:
			tb.Info++
		case model.SeverityWarn:
			tb.Warn++
		case model.SeverityError:
			tb.Error++
		case model.SeverityCritical:
			tb.Critical++
		}
		tb.Total++
	}

	for _, part := range e.parts {
		for _, r := range part.Records {
			processRec(r)
		}
	}
	for _, r := range e.memTable {
		processRec(r)
	}

	for _, tb := range bucketMap {
		buckets = append(buckets, *tb)
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Timestamp < buckets[j].Timestamp
	})
	if len(buckets) > 24 {
		buckets = buckets[len(buckets)-24:]
	}
	return buckets, byService, bySeverity
}

// Stats returns live MergeTree storage telemetry.
func (e *ColumnarEngine) Stats() model.StorageStats {
	e.mu.RLock()
	memRows := int64(len(e.memTable))
	activeParts := len(e.parts)
	e.mu.RUnlock()

	uncomp := e.uncompressedBytes.Load()
	comp := e.compressedBytes.Load()
	ratio := 1.0
	if comp > 0 {
		ratio = float64(uncomp) / float64(comp)
	}
	elapsedSec := time.Since(e.startedAt).Seconds()
	throughput := 0.0
	if elapsedSec > 0 {
		throughput = float64(e.totalRows.Load()) / elapsedSec
	}

	return model.StorageStats{
		EngineType:            "ClickHouse MergeTree (Embedded Columnar + ZLIB/Bloom)",
		TotalRows:             e.totalRows.Load(),
		MemTableRows:          memRows,
		MemTableFlushLimit:    int64(e.memTableFlushLimit),
		ActiveColumnarParts:   activeParts,
		CompactionsCompleted:  e.compactionsCompleted.Load(),
		UncompressedBytes:     uncomp,
		CompressedBytes:       comp,
		CompressionRatio:      ratio,
		BloomFilterQueries:    e.bloomQueries.Load(),
		PartsSkippedByIndex:   e.partsSkipped.Load(),
		PartsScanned:          e.partsScanned.Load(),
		WriteThroughputPerSec: throughput,
	}
}

func canSkipPart(part *ColumnarPart, f QueryFilter, queryTokens []string) bool {
	if f.Service != "" && f.Service != "*" {
		if _, ok := part.ServiceDict[f.Service]; !ok {
			return true
		}
	}
	if f.Severity != "" && f.Severity != "*" {
		if _, ok := part.SeverityDict[f.Severity]; !ok {
			return true
		}
	}
	if f.TraceID != "" {
		if !bloomTest(&part.BloomFilter, strings.ToLower(f.TraceID)) {
			return true
		}
	}
	for _, tok := range queryTokens {
		if !bloomTest(&part.BloomFilter, tok) {
			return true
		}
	}
	return false
}

func matchesFilter(r model.StructuredLogRecord, f QueryFilter, qLower string) bool {
	if f.Service != "" && f.Service != "*" && r.Service != f.Service {
		return false
	}
	if f.Severity != "" && f.Severity != "*" && r.Severity != f.Severity {
		return false
	}
	if f.Host != "" && f.Host != "*" && r.Host != f.Host {
		return false
	}
	if f.TraceID != "" && !strings.EqualFold(r.TraceID, f.TraceID) {
		return false
	}
	if f.MinLatencyMs > 0 && r.LatencyMs < f.MinLatencyMs {
		return false
	}
	if qLower != "" {
		if !strings.Contains(strings.ToLower(r.RawLine), qLower) &&
			!strings.Contains(strings.ToLower(r.Service), qLower) &&
			!strings.Contains(strings.ToLower(r.TraceID), qLower) {
			return false
		}
	}
	return true
}

func bloomAdd(bf *[256]uint64, token string) {
	h1, h2 := bloomHashes(token)
	for i := uint32(0); i < 3; i++ {
		bit := (h1 + i*h2) % 16384
		bf[bit/64] |= 1 << (bit % 64)
	}
}

func bloomTest(bf *[256]uint64, token string) bool {
	h1, h2 := bloomHashes(token)
	for i := uint32(0); i < 3; i++ {
		bit := (h1 + i*h2) % 16384
		if (bf[bit/64] & (1 << (bit % 64))) == 0 {
			return false
		}
	}
	return true
}

func bloomHashes(s string) (uint32, uint32) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	sum := h.Sum64()
	return uint32(sum), uint32(sum>>32) | 1
}

func extractQueryTokens(q string) []string {
	var out []string
	for _, part := range strings.FieldsFunc(q, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_')
	}) {
		if len(part) >= 2 {
			out = append(out, part)
		}
	}
	return out
}

// FormatClickHouseJSONEachRow serializes a batch of records into ClickHouse JSONEachRow format.
func FormatClickHouseJSONEachRow(records []model.StructuredLogRecord) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range records {
		_ = enc.Encode(r)
	}
	return buf.Bytes()
}
