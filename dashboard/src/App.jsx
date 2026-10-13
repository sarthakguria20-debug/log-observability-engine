import React, { useState, useEffect, useCallback } from "react";

const API_BASE = "";

export default function App() {
  const [metrics, setMetrics] = useState(null);
  const [logsResult, setLogsResult] = useState({
    records: [],
    total_matched: 0,
    execution_time_ms: 0,
    parts_scanned: 0,
    parts_skipped_by_index: 0,
  });
  const [alertsData, setAlertsData] = useState({ incidents: [], rules: [] });
  const [filters, setFilters] = useState({
    q: "",
    service: "",
    severity: "",
    min_latency_ms: "",
    trace_id: "",
  });
  const [expandedRow, setExpandedRow] = useState(null);
  const [selectedIncident, setSelectedIncident] = useState(null);
  const [activeTab, setActiveTab] = useState("explorer");

  const fetchDashboardState = useCallback(async () => {
    try {
      const params = new URLSearchParams();
      if (filters.q) params.set("q", filters.q);
      if (filters.service) params.set("service", filters.service);
      if (filters.severity) params.set("severity", filters.severity);
      if (filters.min_latency_ms) params.set("min_latency_ms", filters.min_latency_ms);
      if (filters.trace_id) params.set("trace_id", filters.trace_id);
      params.set("limit", "100");

      const [mRes, lRes, aRes] = await Promise.all([
        fetch(`${API_BASE}/api/v1/metrics`),
        fetch(`${API_BASE}/api/v1/logs?${params.toString()}`),
        fetch(`${API_BASE}/api/v1/alerts`),
      ]);
      if (mRes.ok) setMetrics(await mRes.json());
      if (lRes.ok) setLogsResult(await lRes.json());
      if (aRes.ok) setAlertsData(await aRes.json());
    } catch (err) {
      console.error("Telemetry poll error:", err);
    }
  }, [filters]);

  useEffect(() => {
    fetchDashboardState();
    const timer = setInterval(fetchDashboardState, 1200);
    return () => clearInterval(timer);
  }, [fetchDashboardState]);

  const triggerSimulation = async (action, count = 0) => {
    await fetch(`${API_BASE}/api/v1/simulate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action, count }),
    });
    fetchDashboardState();
  };

  const triggerCompaction = async () => {
    await fetch(`${API_BASE}/api/v1/storage/compact`, { method: "POST" });
    fetchDashboardState();
  };

  const updateIncidentStatus = async (incidentId, status) => {
    await fetch(`${API_BASE}/api/v1/alerts`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ incident_id: incidentId, status }),
    });
    fetchDashboardState();
  };

  const bp = metrics?.backpressure || {};
  const st = metrics?.storage || {};
  const firingCount = (alertsData.incidents || []).filter((i) => i.status === "FIRING").length;

  return (
    <div className="dashboard-shell">
      <header className="top-bar">
        <div className="brand">
          <span className="pulse-dot" />
          <h1>PulseLog</h1>
          <span className="badge-arch">Go Agent · Kafka Partitioned Queue · ClickHouse MergeTree</span>
        </div>
        <div className="chaos-actions">
          <button className="btn btn-danger" onClick={() => triggerSimulation("critical", 3)}>
            Inject CRITICAL_ERROR
          </button>
          <button className="btn btn-warning" onClick={() => triggerSimulation("backpressure_spike", 950)}>
            Spike 950 Logs (Backpressure Test)
          </button>
          <button className="btn btn-secondary" onClick={triggerCompaction}>
            Force MergeTree Compaction
          </button>
        </div>
      </header>

      <section className="kpi-grid">
        <div className="kpi-card">
          <span className="kpi-label">Ingestion & Columnar Rows</span>
          <strong className="kpi-value">{(st.total_rows || 0).toLocaleString()}</strong>
          <span className="kpi-sub">
            {(st.write_throughput_per_sec || 0).toFixed(1)} rows/sec · MemTable {st.memtable_rows || 0}/{st.memtable_flush_limit || 200}
          </span>
        </div>
        <div className="kpi-card">
          <span className="kpi-label">Backpressure & Queue State</span>
          <strong className={`kpi-value state-${(bp.state || "NORMAL").toLowerCase()}`}>
            {bp.state || "NORMAL"}
          </strong>
          <span className="kpi-sub">
            Occupancy {(bp.occupancy_pct || 0).toFixed(1)}% · Shed Debug: {bp.dropped_debug_logs || 0}
          </span>
        </div>
        <div className="kpi-card">
          <span className="kpi-label">ClickHouse Columnar Compression</span>
          <strong className="kpi-value">{(st.compression_ratio || 1).toFixed(2)}x</strong>
          <span className="kpi-sub">
            {st.active_columnar_parts || 0} Active Parts · {st.parts_skipped_by_index || 0} Granules Skipped via Bloom
          </span>
        </div>
        <div className="kpi-card">
          <span className="kpi-label">Active Alert Incidents</span>
          <strong className={`kpi-value ${firingCount > 0 ? "text-critical" : ""}`}>
            {firingCount} FIRING
          </strong>
          <span className="kpi-sub">{(alertsData.rules || []).length} Active Rules (Slack + Email)</span>
        </div>
      </section>

      <nav className="tab-bar">
        <button className={activeTab === "explorer" ? "active" : ""} onClick={() => setActiveTab("explorer")}>
          Log Explorer & Live Tail
        </button>
        <button className={activeTab === "backpressure" ? "active" : ""} onClick={() => setActiveTab("backpressure")}>
          Kafka Backpressure & ClickHouse Internals
        </button>
        <button className={activeTab === "alerts" ? "active" : ""} onClick={() => setActiveTab("alerts")}>
          Alerting & Slack/Email Payloads ({firingCount})
        </button>
      </nav>

      {activeTab === "explorer" && (
        <main className="explorer-panel">
          <div className="filter-bar">
            <input
              type="text"
              placeholder="Search raw logs, keywords (e.g. CRITICAL_ERROR, deadlock, timeout)..."
              value={filters.q}
              onChange={(e) => setFilters({ ...filters, q: e.target.value })}
            />
            <select value={filters.service} onChange={(e) => setFilters({ ...filters, service: e.target.value })}>
              <option value="">All Services</option>
              <option value="payment-service">payment-service</option>
              <option value="auth-service">auth-service</option>
              <option value="api-gateway">api-gateway</option>
              <option value="order-worker">order-worker</option>
              <option value="search-indexer">search-indexer</option>
              <option value="inventory-db">inventory-db</option>
            </select>
            <select value={filters.severity} onChange={(e) => setFilters({ ...filters, severity: e.target.value })}>
              <option value="">All Severities</option>
              <option value="CRITICAL">CRITICAL</option>
              <option value="ERROR">ERROR</option>
              <option value="WARN">WARN</option>
              <option value="INFO">INFO</option>
              <option value="DEBUG">DEBUG</option>
            </select>
          </div>

          <div className="query-meta">
            Matched <strong>{logsResult.total_matched}</strong> logs in{" "}
            <strong>{logsResult.execution_time_ms} ms</strong> · Columnar Parts Scanned:{" "}
            <strong>{logsResult.parts_scanned}</strong> · Parts Skipped by Bloom/MinMax Index:{" "}
            <strong>{logsResult.parts_skipped_by_index}</strong>
          </div>

          <table className="log-table">
            <thead>
              <tr>
                <th>Timestamp</th>
                <th>Severity</th>
                <th>Service</th>
                <th>Host</th>
                <th>Format</th>
                <th>Latency</th>
                <th>Structured Message</th>
              </tr>
            </thead>
            <tbody>
              {(logsResult.records || []).map((r) => (
                <React.Fragment key={r.id}>
                  <tr onClick={() => setExpandedRow(expandedRow === r.id ? null : r.id)}>
                    <td className="mono">{new Date(r.timestamp).toISOString().slice(11, 23)}</td>
                    <td><span className={`sev-pill sev-${r.severity}`}>{r.severity}</span></td>
                    <td className="mono">{r.service}</td>
                    <td className="mono">{r.host}</td>
                    <td><span className="fmt-tag">{r.format}</span></td>
                    <td className="mono">{r.latency_ms ? `${r.latency_ms}ms` : "-"}</td>
                    <td className="log-msg">{r.message}</td>
                  </tr>
                  {expandedRow === r.id && (
                    <tr className="expanded-details">
                      <td colSpan={7}>
                        <pre>{JSON.stringify(r, null, 2)}</pre>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </main>
      )}

      {activeTab === "alerts" && (
        <section className="alerts-panel">
          {(alertsData.incidents || []).map((inc) => (
            <div key={inc.id} className={`incident-card status-${inc.status}`}>
              <div className="incident-header">
                <strong>{inc.id} — {inc.rule_name}</strong>
                <span>{inc.status}</span>
              </div>
              <p>{inc.summary}</p>
              <pre className="raw-sample">{inc.sample_raw_log}</pre>
              <div className="incident-actions">
                <button onClick={() => setSelectedIncident(inc)}>Inspect Slack & Email Payloads</button>
                <button onClick={() => updateIncidentStatus(inc.id, "ACKNOWLEDGED")}>Acknowledge</button>
                <button onClick={() => updateIncidentStatus(inc.id, "RESOLVED")}>Resolve</button>
              </div>
            </div>
          ))}
          {selectedIncident && (
            <dialog open className="payload-modal">
              <h3>Dispatched Alert Payloads — {selectedIncident.id}</h3>
              <h4>Slack Block Kit Webhook JSON</h4>
              <pre>{selectedIncident.slack_payload}</pre>
              <h4>Email / SMTP RFC 5322 Message</h4>
              <pre>{selectedIncident.email_payload}</pre>
              <button onClick={() => setSelectedIncident(null)}>Close</button>
            </dialog>
          )}
        </section>
      )}
    </div>
  );
}

