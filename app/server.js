// DataFlow - a tiny message pipeline with step-level observability.
// Mirrors the company's DF architecture:
//   Input -> Consumer -> Subscriber -> Mapper -> Transformer -> Publisher -> DB
//
// Every stage is instrumented with the same metric vocabulary (runs, errors,
// duration), every message carries a correlation ID through all stages, logs
// are structured JSON, and Prometheus alert rules + Alertmanager turn the
// metrics into firing alerts you can watch in Grafana.

const express = require('express');
const client = require('prom-client');
const crypto = require('crypto');

const app = express();
app.use(express.json());
app.use(express.static('public'));

// ---------------------------------------------------------------------------
// Metrics registry
// ---------------------------------------------------------------------------
const register = new client.Registry();
client.collectDefaultMetrics({ register }); // process metrics (memory, cpu, event loop)

const stageRuns = new client.Counter({
  name: 'dataflow_stage_runs_total',
  help: 'Messages that entered each pipeline stage',
  labelNames: ['stage'],
  registers: [register],
});

const stageErrors = new client.Counter({
  name: 'dataflow_stage_errors_total',
  help: 'Messages that failed in each pipeline stage',
  labelNames: ['stage', 'error_code', 'retryable'],
  registers: [register],
});

const stageDuration = new client.Histogram({
  name: 'dataflow_stage_duration_seconds',
  help: 'How long each pipeline stage takes per message',
  labelNames: ['stage'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
  registers: [register],
});

const e2eDuration = new client.Histogram({
  name: 'dataflow_e2e_duration_seconds',
  help: 'End-to-end latency of one message across the whole pipeline',
  buckets: [0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5],
  registers: [register],
});

const messagesTotal = new client.Counter({
  name: 'dataflow_messages_total',
  help: 'Messages that finished the pipeline, by final result',
  labelNames: ['result'], // success | failed
  registers: [register],
});

// ---------------------------------------------------------------------------
// The pipeline
// ---------------------------------------------------------------------------
// Numeric prefixes keep stages sorted correctly in the Grafana funnel.
// Each stage owns an error code from the error taxonomy; retryable=false means
// a retry storm would be pointless, which is exactly what the taxonomy is for.
const STAGES = [
  { name: '1_consumer',    error: { code: 'CONNECTION_ERROR',    retryable: true  } },
  { name: '2_subscriber',  error: { code: 'TIMEOUT',             retryable: true  } },
  { name: '3_mapper',      error: { code: 'SCHEMA_ERROR',        retryable: false } },
  { name: '4_transformer', error: { code: 'TRANSFORMATION_ERROR', retryable: false } },
  { name: '5_publisher',   error: { code: 'PUBLISH_ERROR',       retryable: true  } },
  { name: '6_db_write',    error: { code: 'DB_CONNECTION_ERROR', retryable: true  } },
];
const STAGE_NAMES = STAGES.map((s) => s.name);

// Pre-create series so the funnel and alert math work before the first failure.
for (const s of STAGES) {
  stageRuns.labels(s.name).inc(0);
  stageErrors.labels(s.name, s.error.code, String(s.error.retryable)).inc(0);
}
messagesTotal.labels('success').inc(0);
messagesTotal.labels('failed').inc(0);

// Failure injection: stage name -> probability (0..1) that the stage fails.
const failures = {};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(entry) {
  console.log(JSON.stringify({ timestamp: new Date().toISOString(), ...entry }));
}

async function runStage(stage, messageId) {
  const start = process.hrtime.bigint();
  stageRuns.labels(stage.name).inc();
  log({ level: 'INFO', service: stage.name, message_id: messageId, event: 'stage_start' });

  // Simulate real work: 10-60ms of latency per stage.
  await sleep(10 + Math.random() * 50);

  const failRate = failures[stage.name] || 0;
  const failed = Math.random() < failRate;

  const durationMs = Number(process.hrtime.bigint() - start) / 1e6;
  stageDuration.labels(stage.name).observe(durationMs / 1000);

  if (failed) {
    const { code, retryable } = stage.error;
    stageErrors.labels(stage.name, code, String(retryable)).inc();
    log({
      level: 'ERROR', service: stage.name, message_id: messageId, event: 'stage_error',
      error_code: code, retryable, duration_ms: Math.round(durationMs),
    });
    const err = new Error(`${stage.name}: ${code}`);
    err.stage = stage.name;
    err.errorCode = code;
    err.retryable = retryable;
    throw err;
  }

  log({ level: 'INFO', service: stage.name, message_id: messageId, event: 'stage_ok', duration_ms: Math.round(durationMs) });
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// Send one message through the pipeline, under one correlation ID.
app.post('/message', async (req, res) => {
  const messageId = crypto.randomUUID();
  const e2eStart = process.hrtime.bigint();
  log({ level: 'INFO', event: 'message_received', message_id: messageId });
  try {
    for (const stage of STAGES) {
      await runStage(stage, messageId);
    }
    const e2eMs = Number(process.hrtime.bigint() - e2eStart) / 1e6;
    e2eDuration.observe(e2eMs / 1000);
    messagesTotal.labels('success').inc();
    log({ level: 'INFO', event: 'message_complete', message_id: messageId, e2e_ms: Math.round(e2eMs) });
    res.json({ ok: true, message_id: messageId, e2e_ms: Math.round(e2eMs) });
  } catch (err) {
    const e2eMs = Number(process.hrtime.bigint() - e2eStart) / 1e6;
    e2eDuration.observe(e2eMs / 1000);
    messagesTotal.labels('failed').inc();
    log({ level: 'ERROR', event: 'message_failed', message_id: messageId, failed_stage: err.stage, error_code: err.errorCode, retryable: err.retryable });
    res.status(500).json({ ok: false, message_id: messageId, failed_stage: err.stage, error_code: err.errorCode, retryable: err.retryable });
  }
});

// Failure injection switch.
//   POST /fail/4_transformer?rate=1    -> that stage always fails
//   POST /fail/4_transformer?rate=0.3  -> fails 30% of the time (flaky)
//   POST /fail/4_transformer?rate=0    -> back to healthy
app.post('/fail/:stage', (req, res) => {
  const stage = STAGES.find((s) => s.name === req.params.stage);
  if (!stage) {
    return res.status(400).json({ ok: false, error: `unknown stage, use one of: ${STAGE_NAMES.join(', ')}` });
  }
  const rate = Math.max(0, Math.min(1, Number(req.query.rate ?? '1')));
  if (rate === 0) delete failures[stage.name];
  else failures[stage.name] = rate;
  log({ level: 'WARN', event: 'failure_injection_changed', stage: stage.name, rate });
  res.json({ ok: true, failures });
});

app.post('/fail-clear', (req, res) => {
  for (const k of Object.keys(failures)) delete failures[k];
  log({ level: 'INFO', event: 'failure_injection_cleared' });
  res.json({ ok: true, failures });
});

// Alertmanager posts firing/resolved alerts here so they show up in the
// structured log stream, next to the messages that caused them.
app.post('/alerts', (req, res) => {
  for (const a of req.body.alerts || []) {
    log({
      level: a.status === 'firing' ? 'ERROR' : 'INFO',
      event: 'alertmanager_notification',
      alert: a.labels?.alertname,
      severity: a.labels?.severity,
      stage: a.labels?.stage,
      status: a.status,
      summary: a.annotations?.summary,
    });
  }
  res.json({ ok: true });
});

// Current injection state, for the control page.
app.get('/status', (req, res) => {
  res.json({ stages: STAGE_NAMES, failures, auto_traffic: autoTraffic !== null });
});

// Auto-traffic generator so the dashboard comes alive during a demo.
let autoTraffic = null;
app.post('/traffic/:mode', (req, res) => {
  if (req.params.mode === 'on' && autoTraffic === null) {
    autoTraffic = setInterval(() => {
      fetch(`http://localhost:${PORT}/message`, { method: 'POST' }).catch(() => {});
    }, 300);
    log({ level: 'INFO', event: 'auto_traffic_on' });
  } else if (req.params.mode === 'off' && autoTraffic !== null) {
    clearInterval(autoTraffic);
    autoTraffic = null;
    log({ level: 'INFO', event: 'auto_traffic_off' });
  }
  res.json({ ok: true, auto_traffic: autoTraffic !== null });
});

// The endpoint Prometheus scrapes.
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  log({ level: 'INFO', event: 'server_started', port: PORT });
});
