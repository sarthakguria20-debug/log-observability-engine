package broker

import (
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pulselog/log-observability-engine/internal/model"
)

var ErrBrokerCircuitOpen = errors.New("broker backpressure circuit breaker OPEN: queue at capacity")

// PartitionedRecord wraps a RawLogEvent with its Kafka-style partition and monotonically increasing offset.
type PartitionedRecord struct {
	Partition int
	Offset    int64
	Event     model.RawLogEvent
}

// PartitionQueue represents a single Kafka-style topic partition with offset tracking.
type PartitionQueue struct {
	ID              int
	Ch              chan PartitionedRecord
	ProducedOffset  atomic.Int64
	ConsumedOffset  atomic.Int64
}

// Broker implements a partitioned log streaming queue with watermark-based backpressure and load shedding.
type Broker struct {
	mu                  sync.RWMutex
	numPartitions       int
	partitionCapacity   int
	partitions          []*PartitionQueue
	softWatermarkPct    float64
	hardWatermarkPct    float64
	droppedDebugLogs    atomic.Int64
	spilledToDiskLogs   atomic.Int64
	circuitBreakerTrips atomic.Int64
	simulatedSlowdownMs atomic.Int64
}

func NewBroker(numPartitions, totalCapacity int, softWatermarkPct, hardWatermarkPct float64) *Broker {
	if numPartitions <= 0 {
		numPartitions = 4
	}
	perPartCap := totalCapacity / numPartitions
	if perPartCap < 64 {
		perPartCap = 64
	}
	parts := make([]*PartitionQueue, numPartitions)
	for i := 0; i < numPartitions; i++ {
		parts[i] = &PartitionQueue{
			ID: i,
			Ch: make(chan PartitionedRecord, perPartCap),
		}
	}
	return &Broker{
		numPartitions:     numPartitions,
		partitionCapacity: perPartCap,
		partitions:        parts,
		softWatermarkPct:  softWatermarkPct,
		hardWatermarkPct:  hardWatermarkPct,
	}
}

// PublishBatch ingests a batch of raw logs, applying partition routing and adaptive backpressure rules.
func (b *Broker) PublishBatch(batch model.LogBatch) (accepted int, shed int, state model.BackpressureState, err error) {
	metrics := b.Metrics()
	state = metrics.State

	if state == model.BackpressureCircuitOpen {
		b.circuitBreakerTrips.Add(1)
		// Admit only CRITICAL_ERROR logs via emergency overflow path
		for _, ev := range batch.Events {
			if isCriticalOrError(ev.RawLine) {
				b.spilledToDiskLogs.Add(1)
			} else {
				b.droppedDebugLogs.Add(1)
				shed++
			}
		}
		return 0, shed, state, ErrBrokerCircuitOpen
	}

	// Apply proportional backpressure delay when in SOFT_THROTTLE or SHEDDING_DEBUG
	if metrics.ThrottleDelayMs > 0 {
		time.Sleep(time.Duration(metrics.ThrottleDelayMs) * time.Millisecond)
	}

	for _, ev := range batch.Events {
		// When above Hard Watermark, shed DEBUG/low-priority logs to protect ERROR/CRITICAL capacity
		if state == model.BackpressureSheddingDebug && isDebugLog(ev.RawLine) {
			b.droppedDebugLogs.Add(1)
			shed++
			continue
		}

		partIdx := b.selectPartition(ev.ServiceHint, ev.Host)
		p := b.partitions[partIdx]
		offset := p.ProducedOffset.Add(1)
		rec := PartitionedRecord{
			Partition: partIdx,
			Offset:    offset,
			Event:     ev,
		}

		select {
		case p.Ch <- rec:
			accepted++
		default:
			// Partition buffer full
			if isDebugLog(ev.RawLine) {
				b.droppedDebugLogs.Add(1)
				shed++
			} else {
				b.spilledToDiskLogs.Add(1)
			}
		}
	}

	return accepted, shed, b.Metrics().State, nil
}

// Partitions returns the underlying partition queues for concurrent consumer workers.
func (b *Broker) Partitions() []*PartitionQueue {
	return b.partitions
}

// MarkConsumed advances the committed consumer offset for a partition.
func (b *Broker) MarkConsumed(partition int, offset int64) {
	if partition >= 0 && partition < len(b.partitions) {
		b.partitions[partition].ConsumedOffset.Store(offset)
	}
}

// SetSimulatedSlowdownMs injects artificial consumer delay to demonstrate backpressure buildup.
func (b *Broker) SetSimulatedSlowdownMs(delayMs int64) {
	b.simulatedSlowdownMs.Store(delayMs)
}

func (b *Broker) GetSimulatedSlowdownMs() int64 {
	return b.simulatedSlowdownMs.Load()
}

// Metrics calculates current queue occupancy, partition lag, and backpressure state.
func (b *Broker) Metrics() model.BackpressureMetrics {
	totalCap := b.numPartitions * b.partitionCapacity
	totalDepth := 0
	partLag := make(map[int]int64, b.numPartitions)
	var totalLag int64

	for _, p := range b.partitions {
		depth := len(p.Ch)
		totalDepth += depth
		lag := p.ProducedOffset.Load() - p.ConsumedOffset.Load()
		if lag < 0 {
			lag = 0
		}
		partLag[p.ID] = lag
		totalLag += lag
	}

	occupancyPct := 0.0
	if totalCap > 0 {
		occupancyPct = (float64(totalDepth) / float64(totalCap)) * 100.0
	}

	state := model.BackpressureNormal
	var throttleMs int64 = 0

	switch {
	case occupancyPct >= 97.0:
		state = model.BackpressureCircuitOpen
		throttleMs = 150
	case occupancyPct >= b.hardWatermarkPct:
		state = model.BackpressureSheddingDebug
		throttleMs = 50
	case occupancyPct >= b.softWatermarkPct:
		state = model.BackpressureSoftThrottle
		throttleMs = int64((occupancyPct - b.softWatermarkPct) * 2.0)
		if throttleMs < 5 {
			throttleMs = 5
		}
	}

	return model.BackpressureMetrics{
		State:               state,
		QueueDepth:          totalDepth,
		QueueCapacity:       totalCap,
		OccupancyPct:        occupancyPct,
		SoftWatermarkPct:    b.softWatermarkPct,
		HardWatermarkPct:    b.hardWatermarkPct,
		DroppedDebugLogs:    b.droppedDebugLogs.Load(),
		SpilledToDiskLogs:   b.spilledToDiskLogs.Load(),
		ThrottleDelayMs:     throttleMs,
		PartitionLag:        partLag,
		TotalLag:            totalLag,
		CircuitBreakerTrips: b.circuitBreakerTrips.Load(),
		SimulatedSlowdownMs: b.simulatedSlowdownMs.Load(),
	}
}

func (b *Broker) selectPartition(service, host string) int {
	key := service
	if key == "" {
		key = host
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()) % b.numPartitions
}

func isDebugLog(line string) bool {
	for _, marker := range []string{"[DEBUG]", `"level":"debug"`, `"level":"trace"`, " DEBUG ", "[TRACE]"} {
		if containsFold(line, marker) {
			return true
		}
	}
	return false
}

func isCriticalOrError(line string) bool {
	for _, marker := range []string{"CRITICAL_ERROR", "[ERROR]", "[CRITICAL]", `"level":"error"`, `"level":"critical"`, "FATAL", "PANIC"} {
		if containsFold(line, marker) {
			return true
		}
	}
	return false
}

func containsFold(s, substr string) bool {
	return len(s) >= len(substr) && indexFold(s, substr) >= 0
}

func indexFold(s, substr string) int {
	n := len(substr)
	if n == 0 {
		return 0
	}
	for i := 0; i <= len(s)-n; i++ {
		match := true
		for j := 0; j < n; j++ {
			c1, c2 := s[i+j], substr[j]
			if c1 >= 'A' && c1 <= 'Z' {
				c1 += 'a' - 'A'
			}
			if c2 >= 'A' && c2 <= 'Z' {
				c2 += 'a' - 'A'
			}
			if c1 != c2 {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

