package alerting

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pulselog/log-observability-engine/internal/model"
)

// Engine evaluates incoming structured logs against keyword, sliding-window error threshold, and latency rules.
type Engine struct {
	mu              sync.Mutex
	rules           []model.AlertRule
	incidents       []model.AlertIncident
	lastFiredByRule map[string]time.Time
	errorWindow     []time.Time
	incidentSeq     atomic.Int64
	slackWebhookURL string
	alertEmailTo    string
	onAlertCallback func(model.AlertIncident)
}

func NewEngine(slackWebhookURL, alertEmailTo string, onAlert func(model.AlertIncident)) *Engine {
	if alertEmailTo == "" {
		alertEmailTo = "sre-oncall@pulselog.io"
	}
	defaultRules := []model.AlertRule{
		{
			ID:              "rule-critical-keyword",
			Name:            "Critical Keyword Detector (CRITICAL_ERROR)",
			Type:            model.AlertRuleKeyword,
			Enabled:         true,
			ServiceFilter:   "*",
			Keyword:         "CRITICAL_ERROR",
			CooldownSeconds: 5,
			Channels:        []string{"slack", "email"},
		},
		{
			ID:              "rule-error-burst-threshold",
			Name:            "Sliding-Window Error Burst (>8 Errors / 30s)",
			Type:            model.AlertRuleThreshold,
			Enabled:         true,
			ServiceFilter:   "*",
			SeverityFilter:  model.SeverityError,
			ThresholdCount:  8,
			WindowSeconds:   30,
			CooldownSeconds: 10,
			Channels:        []string{"slack", "email"},
		},
		{
			ID:              "rule-high-latency-sla",
			Name:            "High Tail Latency Breach (>1200ms)",
			Type:            model.AlertRuleLatency,
			Enabled:         true,
			ServiceFilter:   "*",
			ThresholdCount:  1200,
			WindowSeconds:   60,
			CooldownSeconds: 15,
			Channels:        []string{"slack"},
		},
	}

	return &Engine{
		rules:           defaultRules,
		incidents:       make([]model.AlertIncident, 0, 128),
		lastFiredByRule: make(map[string]time.Time),
		errorWindow:     make([]time.Time, 0, 512),
		slackWebhookURL: slackWebhookURL,
		alertEmailTo:    alertEmailTo,
		onAlertCallback: onAlert,
	}
}

// Evaluate inspects a StructuredLogRecord and fires alerts when any active rule matches.
func (e *Engine) Evaluate(rec model.StructuredLogRecord) []model.AlertIncident {
	now := time.Now().UTC()
	e.mu.Lock()
	defer e.mu.Unlock()

	// Track sliding window of ERROR and CRITICAL events
	if rec.Severity == model.SeverityError || rec.Severity == model.SeverityCritical {
		e.errorWindow = append(e.errorWindow, now)
	}
	// Prune events older than 120s
	cutoff120 := now.Add(-120 * time.Second)
	validIdx := 0
	for _, ts := range e.errorWindow {
		if ts.After(cutoff120) {
			e.errorWindow[validIdx] = ts
			validIdx++
		}
	}
	e.errorWindow = e.errorWindow[:validIdx]

	var fired []model.AlertIncident

	for _, rule := range e.rules {
		if !rule.Enabled {
			continue
		}
		if rule.ServiceFilter != "" && rule.ServiceFilter != "*" && !strings.EqualFold(rule.ServiceFilter, rec.Service) {
			continue
		}

		// Respect per-rule cooldown window to prevent alert storms
		if last, ok := e.lastFiredByRule[rule.ID]; ok {
			cooldown := time.Duration(rule.CooldownSeconds) * time.Second
			if cooldown <= 0 {
				cooldown = 5 * time.Second
			}
			if now.Sub(last) < cooldown {
				continue
			}
		}

		switch rule.Type {
		case model.AlertRuleKeyword:
			if rule.Keyword != "" && strings.Contains(strings.ToUpper(rec.RawLine), strings.ToUpper(rule.Keyword)) {
				inc := e.createIncidentLocked(rule, rec, now,
					fmt.Sprintf("Keyword '%s' detected on %s (%s)", rule.Keyword, rec.Service, rec.Host),
					rule.Keyword, 1, 1)
				fired = append(fired, inc)
			}

		case model.AlertRuleThreshold:
			winSec := rule.WindowSeconds
			if winSec <= 0 {
				winSec = 30
			}
			winStart := now.Add(-time.Duration(winSec) * time.Second)
			count := 0
			for _, ts := range e.errorWindow {
				if ts.After(winStart) {
					count++
				}
			}
			if count >= rule.ThresholdCount {
				inc := e.createIncidentLocked(rule, rec, now,
					fmt.Sprintf("Error rate threshold crossed: %d errors in %ds window (limit: %d)", count, winSec, rule.ThresholdCount),
					"", float64(count), float64(rule.ThresholdCount))
				fired = append(fired, inc)
			}

		case model.AlertRuleLatency:
			if rec.LatencyMs >= float64(rule.ThresholdCount) && rule.ThresholdCount > 0 {
				inc := e.createIncidentLocked(rule, rec, now,
					fmt.Sprintf("High latency SLA breach on %s: %.1fms (threshold: %dms)", rec.Service, rec.LatencyMs, rule.ThresholdCount),
					"", rec.LatencyMs, float64(rule.ThresholdCount))
				fired = append(fired, inc)
			}
		}
	}

	return fired
}

func (e *Engine) createIncidentLocked(
	rule model.AlertRule,
	rec model.StructuredLogRecord,
	now time.Time,
	summary string,
	keyword string,
	observed, threshold float64,
) model.AlertIncident {
	seq := e.incidentSeq.Add(1)
	id := fmt.Sprintf("INC-%04d", seq)
	sev := rec.Severity
	if rule.Type == model.AlertRuleKeyword {
		sev = model.SeverityCritical
	} else if sev != model.SeverityCritical {
		sev = model.SeverityError
	}

	slackPayload := buildSlackBlockKitPayload(id, rule, rec, summary, sev, now)
	emailPayload := buildEmailRFC5322Payload(id, e.alertEmailTo, rule, rec, summary, sev, now)

	inc := model.AlertIncident{
		ID:             id,
		RuleID:         rule.ID,
		RuleName:       rule.Name,
		RuleType:       rule.Type,
		Severity:       sev,
		Status:         "FIRING",
		Service:        rec.Service,
		Host:           rec.Host,
		Summary:        summary,
		MatchedKeyword: keyword,
		ObservedValue:  observed,
		ThresholdValue: threshold,
		WindowSeconds:  rule.WindowSeconds,
		SampleRawLog:   rec.RawLine,
		TriggeredAt:    now,
		ChannelsSent:   rule.Channels,
		SlackPayload:   slackPayload,
		EmailPayload:   emailPayload,
	}

	e.lastFiredByRule[rule.ID] = now
	// Prepend newest incident first
	e.incidents = append([]model.AlertIncident{inc}, e.incidents...)
	if len(e.incidents) > 100 {
		e.incidents = e.incidents[:100]
	}

	// Dispatch asynchronous webhook if external Slack URL is configured
	if e.slackWebhookURL != "" {
		go func(url, body string) {
			client := &http.Client{Timeout: 3 * time.Second}
			_, _ = client.Post(url, "application/json", bytes.NewBufferString(body))
		}(e.slackWebhookURL, slackPayload)
	}

	if e.onAlertCallback != nil {
		go e.onAlertCallback(inc)
	}

	return inc
}

// AddRule registers a new custom AlertRule at runtime.
func (e *Engine) AddRule(r model.AlertRule) model.AlertRule {
	e.mu.Lock()
	defer e.mu.Unlock()
	if r.ID == "" {
		r.ID = fmt.Sprintf("rule-custom-%d", len(e.rules)+1)
	}
	if len(r.Channels) == 0 {
		r.Channels = []string{"slack", "email"}
	}
	r.Enabled = true
	e.rules = append(e.rules, r)
	return r
}

// ToggleRule enables or disables a rule by ID.
func (e *Engine) ToggleRule(ruleID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.rules {
		if e.rules[i].ID == ruleID {
			e.rules[i].Enabled = !e.rules[i].Enabled
			return true
		}
	}
	return false
}

// UpdateIncidentStatus transitions an incident between FIRING, ACKNOWLEDGED, and RESOLVED.
func (e *Engine) UpdateIncidentStatus(incidentID, status string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.incidents {
		if e.incidents[i].ID == incidentID {
			e.incidents[i].Status = status
			return true
		}
	}
	return false
}

func (e *Engine) Rules() []model.AlertRule {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]model.AlertRule, len(e.rules))
	copy(out, e.rules)
	return out
}

func (e *Engine) Incidents() []model.AlertIncident {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]model.AlertIncident, len(e.incidents))
	copy(out, e.incidents)
	return out
}

func buildSlackBlockKitPayload(id string, rule model.AlertRule, rec model.StructuredLogRecord, summary string, sev model.Severity, ts time.Time) string {
	payload := map[string]interface{}{
		"channel":  "#ops-critical-alerts",
		"username": "PulseLog AlertBot",
		"blocks": []map[string]interface{}{
			{
				"type": "header",
				"text": map[string]string{
					"type": "plain_text",
					"text": fmt.Sprintf("[%s] %s — %s", sev, id, rule.Name),
				},
			},
			{
				"type": "section",
				"fields": []map[string]string{
					{"type": "mrkdwn", "text": fmt.Sprintf("*Service:*\n`%s`", rec.Service)},
					{"type": "mrkdwn", "text": fmt.Sprintf("*Host:*\n`%s`", rec.Host)},
					{"type": "mrkdwn", "text": fmt.Sprintf("*Rule Type:*\n`%s`", rule.Type)},
					{"type": "mrkdwn", "text": fmt.Sprintf("*Triggered At:*\n`%s`", ts.Format(time.RFC3339))},
				},
			},
			{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": fmt.Sprintf("*Summary:* %s\n```%s```", summary, rec.RawLine),
				},
			},
		},
	}
	b, _ := json.MarshalIndent(payload, "", "  ")
	return string(b)
}

func buildEmailRFC5322Payload(id, to string, rule model.AlertRule, rec model.StructuredLogRecord, summary string, sev model.Severity, ts time.Time) string {
	var sb strings.Builder
	sb.WriteString("From: PulseLog Alerting Engine <alerts@pulselog.internal>\n")
	sb.WriteString(fmt.Sprintf("To: %s\n", to))
	sb.WriteString(fmt.Sprintf("Subject: [%s] %s: %s on %s\n", sev, id, rule.Name, rec.Service))
	sb.WriteString("MIME-Version: 1.0\n")
	sb.WriteString("Content-Type: text/plain; charset=UTF-8\n\n")
	sb.WriteString(fmt.Sprintf("INCIDENT ID : %s\n", id))
	sb.WriteString(fmt.Sprintf("RULE        : %s (%s)\n", rule.Name, rule.Type))
	sb.WriteString(fmt.Sprintf("SERVICE     : %s (Host: %s)\n", rec.Service, rec.Host))
	sb.WriteString(fmt.Sprintf("TIMESTAMP   : %s\n", ts.Format(time.RFC3339)))
	if rec.TraceID != "" {
		sb.WriteString(fmt.Sprintf("TRACE ID    : %s\n", rec.TraceID))
	}
	sb.WriteString(fmt.Sprintf("\nSUMMARY:\n  %s\n\nSAMPLE LOG LINE:\n  %s\n", summary, rec.RawLine))
	return sb.String()
}

