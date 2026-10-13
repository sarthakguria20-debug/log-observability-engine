(function () {
  const state = {
    activeTab: "explorer",
    filters: {
      q: "",
      service: "",
      severity: "",
      min_latency_ms: "",
    },
    customLogLine:
      '{"level":"error","service":"payment-service","host":"prod-us-east-01","trace_id":"tr-custom99","latency_ms":1450,"msg":"CRITICAL_ERROR: Payment gateway circuit breaker tripped"}',
    newRule: {
      name: "OOM Killer Detection",
      type: "KEYWORD",
      keyword: "OOM_KILLED",
      threshold_count: 5,
      window_seconds: 30,
    },
    expandedRowId: null,
    selectedIncident: null,
    metrics: null,
    logsResult: {
      records: [],
      total_matched: 0,
      execution_time_ms: 0,
      parts_scanned: 0,
      parts_skipped_by_index: 0,
    },
    alertsData: { incidents: [], rules: [] },
    lastActionBanner: "",
  };

  async function fetchAll() {
    try {
      const params = new URLSearchParams();
      if (state.filters.q) params.set("q", state.filters.q);
      if (state.filters.service) params.set("service", state.filters.service);
      if (state.filters.severity) params.set("severity", state.filters.severity);
      if (state.filters.min_latency_ms) params.set("min_latency_ms", state.filters.min_latency_ms);
      params.set("limit", "80");

      const [mRes, lRes, aRes] = await Promise.all([
        fetch("/api/v1/metrics"),
        fetch(`/api/v1/logs?${params.toString()}`),
        fetch("/api/v1/alerts"),
      ]);

      if (mRes.ok) state.metrics = await mRes.json();
      if (lRes.ok) state.logsResult = await lRes.json();
      if (aRes.ok) state.alertsData = await aRes.json();
      render();
    } catch (e) {
      console.error("Failed to fetch telemetry:", e);
    }
  }

  async function postJSON(url, body) {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    });
    const data = await res.json().catch(() => ({}));
    await fetchAll();
    return data;
  }

  window.pulseActions = {
    setTab(tab) {
      state.activeTab = tab;
      render();
    },
    updateFilter(key, val) {
      state.filters[key] = val;
      fetchAll();
    },
    toggleExpand(id) {
      state.expandedRowId = state.expandedRowId === id ? null : id;
      render();
    },
    async triggerSim(action, count) {
      const res = await postJSON("/api/v1/simulate", { action, count });
      if (action === "critical") {
        state.lastActionBanner = `Injected ${res.accepted || count} CRITICAL_ERROR logs -> Alerting Engine triggered Slack + Email notifications!`;
      } else if (action === "backpressure_spike") {
        state.lastActionBanner = `Injected ${count} burst logs -> Backpressure State: ${res.state} (Shed Debug Logs: ${res.shed || 0})`;
      } else if (action === "error_burst") {
        state.lastActionBanner = `Injected ${count} ERROR logs -> Sliding-window threshold detector evaluated!`;
      } else if (action === "toggle_sim") {
        state.lastActionBanner = `Background simulator ${res.simulator_enabled ? "RESUMED" : "PAUSED"}`;
      }
      render();
    },
    async injectCustomLog() {
      const input = document.getElementById("custom-log-input");
      if (input) state.customLogLine = input.value;
      await postJSON("/api/v1/simulate", {
        action: "custom_log",
        raw_line: state.customLogLine,
      });
      state.lastActionBanner = "Ingested & parsed custom unstructured log line!";
      render();
    },
    async forceCompact() {
      const res = await postJSON("/api/v1/storage/compact", {});
      state.lastActionBanner = res.compacted
        ? "ClickHouse MergeTree Compaction completed: merged immutable parts & updated Bloom filters!"
        : "Flushed MemTable to immutable ColumnarPart!";
      render();
    },
    async setSlowdown(ms) {
      await postJSON("/api/v1/backpressure/slowdown", { delay_ms: Number(ms) });
      state.lastActionBanner = `Set consumer processing slowdown to ${ms}ms per event`;
      render();
    },
    async updateIncident(id, status) {
      await postJSON("/api/v1/alerts", { incident_id: id, status });
    },
    inspectIncident(id) {
      const found = (state.alertsData.incidents || []).find((i) => i.id === id);
      state.selectedIncident = found || null;
      render();
    },
    closeModal() {
      state.selectedIncident = null;
      render();
    },
    async createRule() {
      const nameEl = document.getElementById("rule-name");
      const typeEl = document.getElementById("rule-type");
      const kwEl = document.getElementById("rule-kw");
      const threshEl = document.getElementById("rule-thresh");
      if (!nameEl) return;

      await postJSON("/api/v1/rules", {
        name: nameEl.value || "Custom Rule",
        type: typeEl.value,
        keyword: kwEl.value,
        threshold_count: Number(threshEl.value || 5),
        window_seconds: 30,
        cooldown_seconds: 5,
        channels: ["slack", "email"],
      });
      state.lastActionBanner = `Created alert rule: ${nameEl.value}`;
      render();
    },
  };

  function escapeHtml(str) {
    return String(str || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function render() {
    const activeEl = document.activeElement;
    const activeId = activeEl ? activeEl.id : null;
    const selStart = activeEl && typeof activeEl.selectionStart === "number" ? activeEl.selectionStart : null;

    const root = document.getElementById("root");
    const bp = (state.metrics && state.metrics.backpressure) || {};
    const st = (state.metrics && state.metrics.storage) || {};
    const hist = (state.metrics && state.metrics.histogram) || [];
    const byService = (state.metrics && state.metrics.by_service) || {};
    const simOn = state.metrics ? state.metrics.simulator_enabled : true;
    const incidents = state.alertsData.incidents || [];
    const rules = state.alertsData.rules || [];
    const firingCount = incidents.filter((i) => i.status === "FIRING").length;

    const maxBucket = Math.max(1, ...hist.map((b) => b.total || 1));
    const serviceEntries = Object.entries(byService).sort((a, b) => b[1] - a[1]);
    const maxSvcCount = Math.max(1, ...serviceEntries.map((e) => e[1]));

    root.innerHTML = `
      <div class="dashboard-shell">
        <header class="top-bar">
          <div class="brand">
            <span class="pulse-dot"></span>
            <h1>PulseLog Engine</h1>
            <span class="badge-arch">Go CLI Agent · Partitioned Queue · ClickHouse MergeTree</span>
          </div>
          <div class="chaos-actions">
            <button class="btn" onclick="pulseActions.triggerSim('toggle_sim')">
              ${simOn ? "Pause Live Stream" : "Resume Live Stream"}
            </button>
            <button class="btn btn-danger" onclick="pulseActions.triggerSim('critical', 3)">
              Inject CRITICAL_ERROR
            </button>
            <button class="btn btn-warning" onclick="pulseActions.triggerSim('error_burst', 12)">
              Trigger Error Burst (12x)
            </button>
            <button class="btn btn-primary" onclick="pulseActions.triggerSim('backpressure_spike', 950)">
              Spike 950 Logs (Backpressure)
            </button>
            <button class="btn" onclick="pulseActions.forceCompact()">
              Force MergeTree Compaction
            </button>
          </div>
        </header>

        ${
          state.lastActionBanner
            ? `<div class="query-meta" style="border:1px solid rgba(56,189,248,0.4);color:#bae6fd;margin-bottom:14px;">
                 <span>⚡ ${escapeHtml(state.lastActionBanner)}</span>
                 <button class="btn" style="padding:2px 8px;font-size:0.72rem" onclick="this.parentElement.remove()">Dismiss</button>
               </div>`
            : ""
        }

        <section class="kpi-grid">
          <div class="kpi-card">
            <span class="kpi-label">Indexed Columnar Logs</span>
            <strong class="kpi-value">${(st.total_rows || 0).toLocaleString()}</strong>
            <span class="kpi-sub">
              ${(st.write_throughput_per_sec || 0).toFixed(1)} logs/sec · MemTable: ${st.memtable_rows || 0}/${st.memtable_flush_limit || 200} rows
            </span>
          </div>
          <div class="kpi-card">
            <span class="kpi-label">Ingestion Queue & Backpressure</span>
            <strong class="kpi-value state-${(bp.state || "NORMAL").toLowerCase()}">
              ${bp.state || "NORMAL"}
            </strong>
            <span class="kpi-sub">
              Buffer Occupancy: ${(bp.occupancy_pct || 0).toFixed(1)}% (${bp.queue_depth || 0}/${bp.queue_capacity || 1600}) · Shed Debug: ${bp.dropped_debug_logs || 0}
            </span>
          </div>
          <div class="kpi-card">
            <span class="kpi-label">ClickHouse Columnar Compression</span>
            <strong class="kpi-value">${(st.compression_ratio || 1).toFixed(2)}x</strong>
            <span class="kpi-sub">
              ${st.active_columnar_parts || 0} Immutable Parts · ${st.compactions_completed || 0} Compactions · ${st.parts_skipped_by_index || 0} Bloom Skips
            </span>
          </div>
          <div class="kpi-card" onclick="pulseActions.setTab('alerts')" style="cursor:pointer">
            <span class="kpi-label">Active Alert Incidents</span>
            <strong class="kpi-value ${firingCount > 0 ? "text-critical" : "state-normal"}">
              ${firingCount} FIRING
            </strong>
            <span class="kpi-sub">
              ${incidents.length} Total Incidents · ${rules.length} Active Rules (Slack + Email)
            </span>
          </div>
        </section>

        <section class="viz-row">
          <div class="panel-card">
            <div class="panel-header">
              <h3>Real-Time Log Ingestion Volume & Severity Distribution (10s Buckets)</h3>
              <span class="mono" style="color:var(--text-muted)">
                <span style="color:var(--sev-info)">● INFO</span>
                <span style="color:var(--sev-warn);margin-left:8px">● WARN</span>
                <span style="color:var(--sev-error);margin-left:8px">● ERROR</span>
                <span style="color:var(--sev-critical);margin-left:8px">● CRITICAL</span>
              </span>
            </div>
            <div class="histogram-bars">
              ${hist
                .map((b) => {
                  const totalH = Math.max(6, Math.round((b.total / maxBucket) * 108));
                  const pCrit = b.total ? (b.critical / b.total) * 100 : 0;
                  const pErr = b.total ? (b.error / b.total) * 100 : 0;
                  const pWarn = b.total ? (b.warn / b.total) * 100 : 0;
                  const pInfo = b.total ? (b.info / b.total) * 100 : 0;
                  const pDbg = Math.max(0, 100 - pCrit - pErr - pWarn - pInfo);
                  return `
                    <div class="hist-col" title="${b.timestamp} — Total: ${b.total} (Crit:${b.critical}, Err:${b.error}, Warn:${b.warn})">
                      <div class="hist-stack" style="height:${totalH}px">
                        <div class="seg-debug" style="height:${pDbg}%"></div>
                        <div class="seg-info" style="height:${pInfo}%"></div>
                        <div class="seg-warn" style="height:${pWarn}%"></div>
                        <div class="seg-error" style="height:${pErr}%"></div>
                        <div class="seg-critical" style="height:${pCrit}%"></div>
                      </div>
                      <span class="hist-time">${b.timestamp.slice(3)}</span>
                    </div>
                  `;
                })
                .join("")}
            </div>
          </div>

          <div class="panel-card">
            <div class="panel-header">
              <h3>Volume by Microservice</h3>
              <span class="mono" style="color:var(--text-muted)">Click to filter</span>
            </div>
            <div class="service-list">
              ${serviceEntries
                .map(([svc, count]) => {
                  const pct = Math.round((count / maxSvcCount) * 100);
                  return `
                    <div class="service-item" style="cursor:pointer" onclick="pulseActions.updateFilter('service', '${state.filters.service === svc ? "" : svc}')">
                      <div class="service-item-top">
                        <span>${escapeHtml(svc)} ${state.filters.service === svc ? "✓" : ""}</span>
                        <span>${count.toLocaleString()}</span>
                      </div>
                      <div class="progress-track">
                        <div class="progress-fill" style="width:${pct}%"></div>
                      </div>
                    </div>
                  `;
                })
                .join("")}
            </div>
          </div>
        </section>

        <nav class="tab-bar">
          <button class="tab-btn ${state.activeTab === "explorer" ? "active" : ""}" onclick="pulseActions.setTab('explorer')">
            🔍 Structured Log Explorer & Live Tail
          </button>
          <button class="tab-btn ${state.activeTab === "backpressure" ? "active" : ""}" onclick="pulseActions.setTab('backpressure')">
            ⚙️ Backpressure Queue & ClickHouse MergeTree Internals
          </button>
          <button class="tab-btn ${state.activeTab === "alerts" ? "active" : ""}" onclick="pulseActions.setTab('alerts')">
            🚨 Alerting Rules & Dispatched Slack/Email Payloads (${firingCount})
          </button>
        </nav>

        ${state.activeTab === "explorer" ? renderExplorerTab() : ""}
        ${state.activeTab === "backpressure" ? renderBackpressureTab(bp, st) : ""}
        ${state.activeTab === "alerts" ? renderAlertsTab(incidents, rules) : ""}
        ${state.selectedIncident ? renderPayloadModal(state.selectedIncident) : ""}
      </div>
    `;

    if (activeId) {
      const restored = document.getElementById(activeId);
      if (restored) {
        restored.focus();
        if (selStart !== null && typeof restored.setSelectionRange === "function") {
          restored.setSelectionRange(selStart, selStart);
        }
      }
    }
  }

  function renderExplorerTab() {
    const lr = state.logsResult || {};
    const records = lr.records || [];
    return `
      <div>
        <div class="filter-bar">
          <input
            id="filter-q"
            type="text"
            placeholder="Search full-text index, trace_id, or keywords (e.g. CRITICAL_ERROR, deadlock, 502)..."
            value="${escapeHtml(state.filters.q)}"
            oninput="pulseActions.updateFilter('q', this.value)"
          />
          <select id="filter-service" onchange="pulseActions.updateFilter('service', this.value)">
            <option value="">All Microservices</option>
            ${["payment-service", "auth-service", "api-gateway", "order-worker", "search-indexer", "inventory-db"]
              .map((s) => `<option value="${s}" ${state.filters.service === s ? "selected" : ""}>${s}</option>`)
              .join("")}
          </select>
          <select id="filter-severity" onchange="pulseActions.updateFilter('severity', this.value)">
            <option value="">All Severities</option>
            ${["CRITICAL", "ERROR", "WARN", "INFO", "DEBUG"]
              .map((s) => `<option value="${s}" ${state.filters.severity === s ? "selected" : ""}>${s}</option>`)
              .join("")}
          </select>
          <input
            id="filter-lat"
            type="number"
            placeholder="Min Latency (ms)"
            value="${escapeHtml(state.filters.min_latency_ms)}"
            oninput="pulseActions.updateFilter('min_latency_ms', this.value)"
          />
          <button class="btn" onclick="pulseActions.updateFilter('q','');pulseActions.updateFilter('service','');pulseActions.updateFilter('severity','');pulseActions.updateFilter('min_latency_ms','')">
            Reset Filters
          </button>
        </div>

        <div class="panel-card" style="margin-bottom:12px;padding:10px 14px">
          <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <span class="mono" style="color:var(--accent);font-size:0.75rem">Test Live Unstructured Log Parser:</span>
            <input
              id="custom-log-input"
              type="text"
              style="flex:1;min-width:280px;font-family:var(--font-mono);font-size:0.75rem"
              value="${escapeHtml(state.customLogLine)}"
              oninput="this.dataset.edited='1'"
            />
            <button class="btn btn-primary" onclick="pulseActions.injectCustomLog()">
              Parse & Ingest Log Line
            </button>
          </div>
        </div>

        <div class="query-meta">
          <span>
            Matched <strong>${lr.total_matched || 0}</strong> records in <strong>${(lr.execution_time_ms || 0).toFixed(2)} ms</strong>
          </span>
          <span class="mono">
            Columnar Parts Scanned: <strong>${lr.parts_scanned || 0}</strong> ·
            Parts Skipped via 512-bit Bloom & Dictionary Index: <strong>${lr.parts_skipped_by_index || 0}</strong>
          </span>
        </div>

        <div class="table-wrap">
          <table class="log-table">
            <thead>
              <tr>
                <th>UTC Timestamp</th>
                <th>Severity</th>
                <th>Service</th>
                <th>Host</th>
                <th>Format</th>
                <th>Partition/Offset</th>
                <th>Latency</th>
                <th>Parsed Message & Extracted Attributes</th>
              </tr>
            </thead>
            <tbody>
              ${records
                .map((r) => {
                  const tsStr = (r.timestamp || "").replace("T", " ").slice(0, 23);
                  const isExp = state.expandedRowId === r.id;
                  return `
                    <tr class="log-row" onclick="pulseActions.toggleExpand('${r.id}')">
                      <td class="mono">${escapeHtml(tsStr)}</td>
                      <td><span class="sev-pill sev-${r.severity}">${r.severity}</span></td>
                      <td class="mono">${escapeHtml(r.service)}</td>
                      <td class="mono" style="color:var(--text-muted)">${escapeHtml(r.host)}</td>
                      <td><span class="fmt-tag">${escapeHtml(r.format)}</span></td>
                      <td class="mono" style="color:var(--text-muted)">P${r.partition}:${r.offset}</td>
                      <td class="mono" style="color:${r.latency_ms >= 800 ? "var(--sev-error)" : "var(--text-main)"}">
                        ${r.latency_ms ? `${r.latency_ms}ms` : "-"}
                      </td>
                      <td class="log-msg">
                        ${escapeHtml(r.message)}
                        ${r.trace_id ? `<span class="fmt-tag" style="margin-left:6px">trace=${escapeHtml(r.trace_id)}</span>` : ""}
                      </td>
                    </tr>
                    ${
                      isExp
                        ? `<tr class="expanded-details">
                             <td colspan="8">
                               <div class="expanded-grid">
                                 <div>
                                   <div style="font-size:0.75rem;color:var(--text-muted);margin-bottom:4px">Structured Columnar Document (Indexed Fields + Tokens)</div>
                                   <pre class="code-block">${escapeHtml(JSON.stringify(r, null, 2))}</pre>
                                 </div>
                                 <div>
                                   <div style="font-size:0.75rem;color:var(--text-muted);margin-bottom:4px">Original Unstructured Raw Wire Line</div>
                                   <pre class="code-block">${escapeHtml(r.raw_line)}</pre>
                                 </div>
                               </div>
                             </td>
                           </tr>`
                        : ""
                    }
                  `;
                })
                .join("")}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }

  function renderBackpressureTab(bp, st) {
    const partLag = bp.partition_lag || {};
    return `
      <div class="arch-grid">
        <div class="panel-card">
          <div class="panel-header">
            <h3>Kafka-Style Partitioned Queue & Watermark Flow Control</h3>
            <span class="sev-pill sev-${bp.state === "NORMAL" ? "INFO" : "WARN"}">${bp.state || "NORMAL"}</span>
          </div>
          <p style="font-size:0.82rem;color:var(--text-muted);margin-bottom:14px">
            Events are consistently hashed by <code>fnv1a(service_name) % 4</code> across 4 independent partition channels.
            When queue occupancy crosses the <strong>Soft Watermark (${bp.soft_watermark_pct || 65}%)</strong>, producers receive adaptive throttle delays.
            Above the <strong>Hard Watermark (${bp.hard_watermark_pct || 85}%)</strong>, the broker automatically sheds <code>DEBUG</code> logs to preserve 100% capacity for <code>WARN/ERROR/CRITICAL</code> events.
          </p>

          <div class="partition-box">
            <div style="display:flex;justify-content:space-between;font-size:0.8rem;margin-bottom:6px">
              <span>Global Ring Buffer Occupancy</span>
              <span class="mono">${(bp.occupancy_pct || 0).toFixed(1)}% (${bp.queue_depth || 0} / ${bp.queue_capacity || 1600})</span>
            </div>
            <div class="progress-track" style="height:12px">
              <div class="progress-fill" style="width:${Math.min(100, bp.occupancy_pct || 0)}%"></div>
            </div>
          </div>

          <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:14px 0">
            ${[0, 1, 2, 3]
              .map(
                (p) => `
              <div class="partition-box">
                <div class="mono" style="color:var(--accent);font-weight:600">Topic Partition #${p}</div>
                <div class="mono" style="font-size:0.78rem;margin-top:4px">
                  Consumer Lag: <strong>${partLag[p] || 0}</strong> msgs
                </div>
              </div>
            `
              )
              .join("")}
          </div>

          <div class="partition-box">
            <div style="display:flex;justify-content:space-between;align-items:center">
              <span>Simulate Downstream Storage Slowdown (Consumer Delay):</span>
              <span class="mono">${bp.simulated_slowdown_ms || 0} ms / event</span>
            </div>
            <div style="display:flex;gap:8px;margin-top:10px">
              <button class="btn" onclick="pulseActions.setSlowdown(0)">0ms (Healthy)</button>
              <button class="btn btn-warning" onclick="pulseActions.setSlowdown(12)">12ms (Induce Soft Throttle)</button>
              <button class="btn btn-danger" onclick="pulseActions.setSlowdown(30)">30ms + Spike (Induce Debug Shedding)</button>
            </div>
          </div>
        </div>

        <div class="panel-card">
          <div class="panel-header">
            <h3>ClickHouse MergeTree Storage Engine Optimization</h3>
            <button class="btn btn-primary" onclick="pulseActions.forceCompact()">Run MergeTree Compaction</button>
          </div>
          <p style="font-size:0.82rem;color:var(--text-muted);margin-bottom:14px">
            Instead of random B-Tree page updates, incoming logs append to an in-memory <strong>MemTable</strong>.
            Once full (${st.memtable_flush_limit || 200} rows), it sorts rows by <code>(service, severity, timestamp)</code>, builds a
            <strong>512-bit Token Bloom Filter</strong> & <strong>LowCardinality Dictionary</strong>, compresses the block with ZLIB/ZSTD, and writes an immutable <strong>ColumnarPart</strong>.
          </p>

          <div class="partition-box">
            <div class="mono">MemTable Buffer Fill: ${st.memtable_rows || 0} / ${st.memtable_flush_limit || 200} rows</div>
            <div class="progress-track" style="margin-top:6px">
              <div class="progress-fill" style="width:${Math.min(100, ((st.memtable_rows || 0) / (st.memtable_flush_limit || 200)) * 100)}%"></div>
            </div>
          </div>

          <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
            <div class="partition-box">
              <span class="kpi-label">Active Immutable Parts</span>
              <div class="kpi-value" style="font-size:1.3rem">${st.active_columnar_parts || 0}</div>
            </div>
            <div class="partition-box">
              <span class="kpi-label">MergeTree Compactions</span>
              <div class="kpi-value" style="font-size:1.3rem">${st.compactions_completed || 0}</div>
            </div>
            <div class="partition-box">
              <span class="kpi-label">Uncompressed vs Compressed</span>
              <div class="mono" style="margin-top:4px">
                ${((st.uncompressed_bytes || 0) / 1024).toFixed(1)} KB → ${((st.compressed_bytes || 0) / 1024).toFixed(1)} KB (${(st.compression_ratio || 1).toFixed(2)}x)
              </div>
            </div>
            <div class="partition-box">
              <span class="kpi-label">Bloom Filter Granules Skipped</span>
              <div class="kpi-value" style="font-size:1.3rem;color:var(--sev-info)">${st.parts_skipped_by_index || 0}</div>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  function renderAlertsTab(incidents, rules) {
    return `
      <div class="arch-grid">
        <div>
          <h3 style="margin-bottom:12px">Triggered Alert Incidents (${incidents.length})</h3>
          ${
            incidents.length === 0
              ? `<div class="panel-card" style="color:var(--text-muted)">
                   No incidents fired yet. Click <strong>"Inject CRITICAL_ERROR"</strong> in the top bar to trigger a live Slack + Email alert!
                 </div>`
              : incidents
                  .map(
                    (inc) => `
                <div class="incident-card status-${inc.status}">
                  <div class="incident-header">
                    <strong>[${escapeHtml(inc.severity)}] ${escapeHtml(inc.id)} — ${escapeHtml(inc.rule_name)}</strong>
                    <span class="sev-pill sev-${inc.status === "FIRING" ? "CRITICAL" : "INFO"}">${escapeHtml(inc.status)}</span>
                  </div>
                  <div style="font-size:0.83rem;margin-bottom:8px">${escapeHtml(inc.summary)}</div>
                  <pre class="code-block" style="margin-bottom:10px">${escapeHtml(inc.sample_raw_log)}</pre>
                  <div style="display:flex;gap:8px;flex-wrap:wrap">
                    <button class="btn btn-primary" onclick="pulseActions.inspectIncident('${inc.id}')">
                      Inspect Dispatched Slack & Email Payloads
                    </button>
                    ${
                      inc.status === "FIRING"
                        ? `<button class="btn btn-warning" onclick="pulseActions.updateIncident('${inc.id}', 'ACKNOWLEDGED')">Acknowledge</button>`
                        : ""
                    }
                    ${
                      inc.status !== "RESOLVED"
                        ? `<button class="btn" onclick="pulseActions.updateIncident('${inc.id}', 'RESOLVED')">Resolve Incident</button>`
                        : ""
                    }
                  </div>
                </div>
              `
                  )
                  .join("")
          }
        </div>

        <div>
          <div class="panel-card" style="margin-bottom:14px">
            <h3 style="margin-bottom:10px">Configured Alerting Rules (${rules.length})</h3>
            ${rules
              .map(
                (r) => `
              <div class="partition-box" style="display:flex;justify-content:space-between;align-items:center">
                <div>
                  <div style="font-weight:600;font-size:0.85rem">${escapeHtml(r.name)}</div>
                  <div class="mono" style="color:var(--text-muted);font-size:0.74rem">
                    Type: ${escapeHtml(r.type)} ${r.keyword ? `| Keyword: "${escapeHtml(r.keyword)}"` : ""}
                    ${r.threshold_count ? `| Threshold: ${r.threshold_count}` : ""}
                    | Channels: ${(r.channels || []).join(", ")}
                  </div>
                </div>
                <span class="sev-pill sev-INFO">ACTIVE</span>
              </div>
            `
              )
              .join("")}
          </div>

          <div class="panel-card">
            <h3 style="margin-bottom:10px">Create Custom Alert Rule</h3>
            <div style="display:flex;flex-direction:column;gap:8px">
              <input id="rule-name" type="text" placeholder="Rule Name (e.g. Deadlock Detector)" value="Database Deadlock Detector" />
              <select id="rule-type">
                <option value="KEYWORD">Keyword Match Trigger (Immediate)</option>
                <option value="THRESHOLD">Sliding-Window Error Count Threshold</option>
                <option value="LATENCY_P99">Latency Breach Threshold (ms)</option>
              </select>
              <input id="rule-kw" type="text" placeholder="Keyword (e.g. DEADLOCK or OOM_KILLED)" value="deadlock" />
              <input id="rule-thresh" type="number" placeholder="Threshold Count or Latency (ms)" value="5" />
              <button class="btn btn-primary" onclick="pulseActions.createRule()">+ Register Alert Rule</button>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  function renderPayloadModal(inc) {
    return `
      <dialog open class="payload-modal">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
          <h3>Dispatched Notification Payloads — ${escapeHtml(inc.id)}</h3>
          <button class="btn" onclick="pulseActions.closeModal()">✕ Close</button>
        </div>
        <p style="font-size:0.82rem;color:var(--text-muted);margin-bottom:10px">
          Below are the exact Slack Block Kit Webhook JSON and RFC 5322 Email payloads generated when rule
          <code>${escapeHtml(inc.rule_name)}</code> triggered on service <code>${escapeHtml(inc.service)}</code>.
        </p>
        <h4 style="font-size:0.82rem;color:var(--accent);margin:10px 0 6px">1. Slack Webhook Payload (Block Kit JSON)</h4>
        <pre class="code-block">${escapeHtml(inc.slack_payload)}</pre>
        <h4 style="font-size:0.82rem;color:var(--accent);margin:14px 0 6px">2. SMTP / Email Notification (RFC 5322)</h4>
        <pre class="code-block">${escapeHtml(inc.email_payload)}</pre>
      </dialog>
    `;
  }

  fetchAll();
  setInterval(fetchAll, 1400);
})();

