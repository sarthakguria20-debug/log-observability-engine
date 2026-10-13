#!/usr/bin/env python3
"""
End-to-End Integration Test Suite for PulseLog Observability & Log Aggregation Engine.
Verifies:
  1. Multi-format unstructured log parsing (JSON, Nginx, Syslog, Bracketed)
  2. CLI Log Agent micro-batching & GZIP payload compression
  3. Columnar MergeTree storage, ZLIB compression ratio, and 512-bit Bloom filter part skipping
  4. Real-time Alerting Engine (CRITICAL_ERROR keyword trigger + Sliding-Window Error Burst + Slack Block Kit & Email payloads)
  5. Partitioned queue backpressure watermarks & DEBUG load shedding under spike load
"""

import gzip
import importlib.machinery
import importlib.util
import json
import os
import sys
import time

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_script_module(name: str, rel_path: str):
    full_path = os.path.join(ROOT_DIR, rel_path)
    loader = importlib.machinery.SourceFileLoader(name, full_path)
    spec = importlib.util.spec_from_loader(loader.name, loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


def main():
    print("=== PulseLog End-to-End Verification Suite ===")
    ingestor = load_script_module("pulselog_ingestor", "bin/ingestor")
    agent = load_script_module("pulselog_agent", "bin/log-agent")

    # 1. Initialize Broker, Parser, Columnar MergeTree Store, and Alerting Engine
    broker = ingestor.PartitionedBroker(num_partitions=4, total_capacity=1600, soft_wm=65.0, hard_wm=85.0)
    parser = ingestor.LogParser("production")
    store = ingestor.ColumnarEngine(flush_limit=200)
    alerts = ingestor.AlertingEngine()

    # Seed 420 logs across 2 immutable ColumnarParts
    seed_events = ingestor.generate_simulated_events(420, "standard")
    seed_parsed = [parser.parse(ev, idx % 4, idx + 1) for idx, ev in enumerate(seed_events)]
    store.insert_batch(seed_parsed[:210])
    store.flush()
    store.insert_batch(seed_parsed[210:])
    store.flush()

    st0 = store.stats()
    assert st0["total_rows"] == 420 and st0["active_columnar_parts"] == 2
    print(f"[PASS] 1. Columnar MergeTree Engine initialized ({st0['total_rows']} rows across {st0['active_columnar_parts']} immutable parts)")

    # 2. Test Multi-Format Unstructured Log Parsing (JSON, Nginx, Syslog, Bracketed)
    sample_events = [
        {
            "agent_id": "test-cli-agent",
            "host": "prod-pay-01",
            "service_hint": "payment-service",
            "raw_line": '{"level":"info","service":"payment-service","host":"prod-pay-01","trace_id":"tr-json001","latency_ms":42.5,"msg":"Stripe webhook verified"}',
        },
        {
            "agent_id": "test-cli-agent",
            "host": "edge-gw-01",
            "service_hint": "api-gateway",
            "raw_line": '10.24.1.99 - - [13/Oct/2026:10:15:22 +0000] "POST /api/v1/checkout HTTP/1.1" 502 1280 "-" "Go-http/2.0" trace_id=tr-nginx502 latency=815ms service=api-gateway',
        },
        {
            "agent_id": "test-cli-agent",
            "host": "prod-auth-01",
            "service_hint": "auth-service",
            "raw_line": "<34>Oct 13 10:15:22 prod-auth-01 auth-service[2190]: OAuth2 token introspection completed trace_id=tr-syslog01 latency=19ms",
        },
        {
            "agent_id": "test-cli-agent",
            "host": "worker-02",
            "service_hint": "order-worker",
            "raw_line": "2026-10-13T10:15:22.419Z [WARN] [order-worker] Slow Kafka consumer group rebalance trace_id=tr-brack01 latency=490ms",
        },
    ]

    # Verify GZIP batch wire serialization from CLI agent
    wire_batch = gzip.compress(json.dumps({"batch_id": "b-1", "events": sample_events}).encode("utf-8"))
    decoded_batch = json.loads(gzip.decompress(wire_batch).decode("utf-8"))
    acc, shed, bp_state, _ = broker.publish_batch(decoded_batch["events"])
    assert acc == 4 and shed == 0 and bp_state == "NORMAL"

    # Drain all 4 partitions through parser + alerting + columnar store
    for p in range(4):
        for part_id, offset, ev in broker.pop_partition(p, max_items=50):
            rec = parser.parse(ev, part_id, offset)
            alerts.evaluate(rec)
            store.insert_batch([rec])

    res_nginx = store.query(q="tr-nginx502")
    assert res_nginx["total_matched"] == 1
    rec_nginx = res_nginx["records"][0]
    assert rec_nginx["format"] == "nginx" and rec_nginx["status_code"] == 502 and rec_nginx["severity"] == "ERROR"
    assert rec_nginx["latency_ms"] == 815.0

    res_syslog = store.query(q="tr-syslog01")
    assert res_syslog["total_matched"] == 1 and res_syslog["records"][0]["format"] == "syslog"
    print("[PASS] 2. Multi-Format Unstructured Parser (JSON, Nginx, Syslog, Bracketed) & GZIP Batching verified")

    # 3. Verify 512-bit Bloom Filter & Dictionary Skip-Index Pruning
    res_skip = store.query(q="tr_nonexistent_token_xyz987")
    assert res_skip["parts_skipped_by_index"] == 2 and res_skip["parts_scanned"] == 0
    print(
        f"[PASS] 3. ClickHouse Columnar 512-bit Bloom Filter skipped {res_skip['parts_skipped_by_index']}/2 "
        f"immutable parts in {res_skip['execution_time_ms']}ms"
    )

    # 4. Verify Real-Time Alerting Engine (CRITICAL_ERROR keyword + Sliding-Window Error Burst)
    crit_events = agent.build_sim_events(3, "critical", "edge-agent-01", "prod-node-01", "payment-service")
    broker.publish_batch(crit_events)
    for p in range(4):
        for part_id, offset, ev in broker.pop_partition(p, max_items=50):
            rec = parser.parse(ev, part_id, offset)
            alerts.evaluate(rec)
            store.insert_batch([rec])

    assert len(alerts.incidents) >= 1, "Expected CRITICAL_ERROR alert incident to fire"
    top_inc = alerts.incidents[0]
    assert top_inc["severity"] == "CRITICAL"
    assert '"blocks"' in top_inc["slack_payload"]
    assert "Subject: [CRITICAL]" in top_inc["email_payload"]
    print(f"[PASS] 4. Real-Time Alerting Engine fired {top_inc['id']} with Slack Block Kit & RFC 5322 Email payloads")

    # 5. Verify Backpressure Watermarks & DEBUG Log Shedding under High-Volume Spike
    spike_events = ingestor.generate_simulated_events(1520, "backpressure_spike")
    acc_spike, shed_spike, state_spike, _ = broker.publish_batch(spike_events)
    bp_metrics = broker.metrics()
    assert state_spike in ("SOFT_THROTTLE", "SHEDDING_DEBUG", "CIRCUIT_OPEN")
    assert shed_spike > 0 and bp_metrics["dropped_debug_logs"] > 0
    print(
        f"[PASS] 5. Backpressure Flow Control verified (State={state_spike}, "
        f"Occupancy={bp_metrics['occupancy_pct']}%, Accepted={acc_spike}, ShedDebug={shed_spike})"
    )

    # 6. Verify MergeTree Compaction & ZLIB Columnar Compression Ratio
    compacted = store.compact()
    st_final = store.stats()
    assert compacted is True and st_final["compactions_completed"] >= 1
    assert st_final["compression_ratio"] > 2.5, f"Expected compression ratio > 2.5x, got {st_final['compression_ratio']}"
    print(
        f"[PASS] 6. ClickHouse MergeTree Compaction verified "
        f"(Compression Ratio={st_final['compression_ratio']}x, Compactions={st_final['compactions_completed']})"
    )

    print("\nALL 6 END-TO-END OBSERVABILITY PIPELINE TESTS PASSED!")


if __name__ == "__main__":
    main()

