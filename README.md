# DataFlow - pipeline observability demo

A small greenfield version of the company's DF architecture - a six-stage
message pipeline:

```
Input -> Consumer -> Subscriber -> Mapper -> Transformer -> Publisher -> DB
```

It shows **which stage failed, when, and why** instead of just "something is
broken", with custom metrics, structured logs, correlation IDs, a Grafana
dashboard, and real firing alerts. One command starts everything:
Node/Express app + Prometheus + Alertmanager + Grafana.

## Run it (Mac)

Needs only Docker Desktop installed and running.

```bash
cd observability-demo
docker compose up --build
```

Then open:

| What | URL | Login |
|---|---|---|
| Demo control page | http://localhost:8080 | - |
| Grafana dashboard | http://localhost:3000 | `admin` / `admin` |
| Prometheus (alert rules) | http://localhost:9090/alerts | - |
| Alertmanager (firing alerts) | http://localhost:9093 | - |
| Raw metrics endpoint | http://localhost:8080/metrics | - |

In Grafana, open the dashboard **"DataFlow / Pipeline observability"**
(already provisioned, no setup needed).

## The 90-second demo script

1. On http://localhost:8080 click **Start auto traffic**. Messages start
   flowing through all six stages.
2. Open the Grafana dashboard. Everything is healthy: equal throughput per
   stage, zero errors, ~300ms end-to-end p95, all six funnel bars equal,
   **Active alerts** empty.
3. Click **Break transformer** on the control page.
4. Within ~30-60 seconds:
   - **Errors per stage** spikes on exactly one line: `4_transformer`.
   - The **funnel** shows messages reaching stages 1-4 but dropping off after.
   - **Messages failed** and the **error ratio** go red.
   - Two alerts fire - `DataflowStageDown` and `DataflowStageErrorRate`,
     both naming `4_transformer` - visible in the **Active alerts** panel,
   at http://localhost:9090/alerts, and in Alertmanager at
   http://localhost:9093.
5. Click **Fix everything**. Errors return to zero and the alerts resolve,
   live.

That is the whole pitch: nobody greps logs to find the broken stage; the
dashboard and the alert both point at it by name.

## How it works (what to say when they ask)

- **One metric vocabulary across every stage.** Each stage records the same
  three things with the Prometheus client library: a **counter** of messages
  in, a **counter** of failures, and a **histogram** of durations. That is
  the RED method (Rate, Errors, Duration) applied per pipeline stage, so
  stages are directly comparable.
- **A message ID that travels.** Every message gets a UUID at the Consumer
  and it is passed through all six stages. Logs are structured JSON carrying
  `message_id`, `service`, `error_code` and `retryable`, so one ID shows a
  failed message's exact path and the stage that killed it.
- **End-to-end latency is its own metric.** `dataflow_e2e_duration_seconds`
  measures the full trip, because that is the number the business feels; the
  per-stage histograms explain it.
- **Alerts, not just graphs.** Prometheus evaluates rules every 5s
  (`prometheus/dataflow-alerts.yml`): stage down (100% errors), stage error
  rate > 5%, a **no-traffic heartbeat** (services up but zero messages
  processed - the sneaky one), and end-to-end p95 over 750ms. Firing alerts
  go to Alertmanager, which would page Slack/email/PagerDuty in production.
- **An error taxonomy.** Each stage's failures carry a code
  (`SCHEMA_ERROR`, `TRANSFORMATION_ERROR`, `DB_CONNECTION_ERROR`, ...) and a
  `retryable` flag - retryable errors get retried, non-retryable ones go
  straight to a dead-letter path instead of causing a retry storm.
- **Reconciliation.** The funnel panel compares how many messages entered
  each stage, so silently dropped records are visible even when every
  service looks green.

## If they ask "how does this map to our Java stack?"

The concepts are identical; only the libraries change. In their Java DF:
instrument with **OpenTelemetry** (or Micrometer + Actuator in Spring Boot),
ship spans to **Tempo/Jaeger** for distributed tracing, logs to **Loki/ELK**,
metrics to Prometheus - same Grafana, same Alertmanager. The OTel trace ID
plays the role this demo's `message_id` plays, and exemplars let you jump
from a metric spike straight to the exact trace. Keep label cardinality low
(stage names, never message IDs as metric labels).

## Troubleshooting

- `docker compose up` says a port is already in use: something else on the
  Mac holds 8080/3000/9090/9093. Either stop it or change the left-hand port
  in `docker-compose.yml` (e.g. `"8081:8080"` for the app).
- Dashboard looks empty: make sure auto traffic is on and give Prometheus
  ~10 seconds to scrape.
- Alerts take ~30-60s to fire after you break a stage: that is the rule
  evaluation interval plus the `for:` window, same as production.

## Files

```
app/server.js                  the pipeline: six stages, metrics, message IDs, /metrics, /alerts, failure switch
app/public/index.html          the demo control page
prometheus/prometheus.yml      scrape config (5s) + alert rules wiring
prometheus/dataflow-alerts.yml the four alert rules
alertmanager/alertmanager.yml  alert routing (posts back to the app's log for the demo)
grafana/provisioning/          datasource + dashboard, auto-loaded
docker-compose.yml             app + prometheus + alertmanager + grafana
```
