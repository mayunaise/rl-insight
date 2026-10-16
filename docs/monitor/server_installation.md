# Server Installation

RL-Insight Monitor needs three services before online monitoring can run:

| Service    | Role                                | Required version | Installer version |
| ---------- | ----------------------------------- | ---------------- | ----------------- |
| Prometheus | stores and queries training metrics | `>= 2.30.0`    | `2.54.1`        |
| Tempo      | stores and queries RL state traces  | `>= 2.0.0`     | `2.6.1`         |
| Grafana    | shows dashboards and trace views    | `>= 13.0.0`    | `13.0.0`        |

Choose one of the three approaches below depending on your network environment. To run the same stack in a container instead of installing services on the host, see the **Run with Docker** section below.

## Supported Platforms

Automatic installation supports Linux and Windows x64.

| OS family                    | CPU architectures                               |
| ---------------------------- | ----------------------------------------------- |
| Ubuntu / Debian              | `amd64` / `x86_64`, `arm64` / `aarch64` |
| CentOS / RHEL / Rocky / Alma | `amd64` / `x86_64`, `arm64` / `aarch64` |
| Windows | `amd64` / `x86_64` |

---

## Approach 1: Direct Installation (Official Source)

Suitable for users whose nodes can reach GitHub release assets and `dl.grafana.com` directly. The installer downloads and manages everything.

```bash
rl-insight server install
```

The command downloads Prometheus, Tempo, and Grafana into `~/.rl-insight/services`, then prints a dependency summary.

Then start the stack:

```bash
rl-insight server start
```

Use `rl-insight server start --auto-port` (optionally with `--detach`) to retain available configured ports and replace occupied ports with OS-assigned ports. This includes Tempo's internal gRPC and memberlist ports. Automatic mode waits for readiness and makes up to three startup attempts if a port is taken meanwhile. Actual ports are saved in the runtime config for service discovery, Grafana, and `server targets add`; original YAML is unchanged. Use the printed server and Grafana URLs, which may change between starts. Fixed-port mode is unchanged.

To use a custom data directory, pass `--log-dir`:

```bash
rl-insight server start --log-dir /path/to/rl-insight-data
```

The directory is created automatically and is used by Prometheus, Tempo, Grafana, and target state. See [Data Directory Migration](./data_migration.md) for backup and restore instructions.

Useful variants:

```bash
# Reinstall even if binaries already exist.
rl-insight server install --force

# Use a different managed install directory.
rl-insight server install --install-dir /opt/rl-insight/services

# Start in background and stop later.
rl-insight server start --detach
rl-insight server stop
```

---

## Approach 2: Offline Installation With Pre-downloaded Archives

Suitable for nodes that cannot access official sources or have no internet connection at all. Pre-download the archives on another machine and provide them locally.

### 1. Get the download list

Run `rl-insight server install` on the target node. Even if it cannot download, it prints the planned URLs before attempting:

```text
Planned downloads:
  prometheus   2.54.1    https://github.com/prometheus/prometheus/releases/download/v2.54.1/prometheus-2.54.1.linux-amd64.tar.gz
  tempo        2.6.1     https://github.com/grafana/tempo/releases/download/v2.6.1/tempo_2.6.1_linux_amd64.tar.gz
  grafana      13.0.0    https://dl.grafana.com/oss/release/grafana-13.0.0.linux-amd64.tar.gz
```

### 2. Download the archives

Use a machine with network access to download the files listed above. The filenames must match exactly.

### 3. Install from the local directory

Place the three release archives (`.tar.gz` or `.zip`, depending on platform) in a single directory and run:

```bash
rl-insight server install --local-archive /path/to/archives
```

RL-Insight checks the local directory for each archive by exact filename. Archives that match are copied and used directly; any missing archive falls back to the configured download URL. The version is verified implicitly — the archive filename must include the version configured in `install_version`.

---

## Approach 3: Manual Installation (No Installer)

Suitable for air-gapped environments, centralized operations with unified deployment tooling, or users who need full control over binary placement and custom deployment requirements.

RL-Insight searches for binaries in this order: manifest.json → `~/.rl-insight/services` → system PATH → system fixed paths. If your binaries are already on `PATH` or in a standard location, simply run `rl-insight server start`. To point at an arbitrary path, use `binary_path` in config:

```yaml
prometheus:
  binary_path: /opt/custom/prometheus
tempo:
  binary_path: /opt/custom/tempo
grafana:
  binary_path: /opt/custom/grafana-server
```

### 1. Get the archives

Follow Approach 2 to identify and download the three release archives (`.tar.gz` or `.zip`, depending on platform).

### 2. Extract and place

On Windows, extract the archives and configure `binary_path` for each service; keep the full Grafana directory. The commands below are for Linux.

```bash
PROMETHEUS_ARCHIVE=prometheus-2.54.1.linux-arm64.tar.gz
TEMPO_ARCHIVE=tempo_2.6.1_linux_arm64.tar.gz
GRAFANA_ARCHIVE=grafana-13.0.0.linux-arm64.tar.gz

# Prometheus — extracted directory name matches the archive (minus .tar.gz)
PROMETHEUS_DIR="${PROMETHEUS_ARCHIVE%.tar.gz}"
TMP_DIR="$(mktemp -d)"
tar -xzf "$PROMETHEUS_ARCHIVE" -C "$TMP_DIR"
mkdir -p ~/.rl-insight/services/prometheus
cp "$TMP_DIR/$PROMETHEUS_DIR/prometheus" ~/.rl-insight/services/prometheus/prometheus
chmod +x ~/.rl-insight/services/prometheus/prometheus

# Tempo — extracts a flat binary, no directory
TMP_DIR="$(mktemp -d)"
tar -xzf "$TEMPO_ARCHIVE" -C "$TMP_DIR"
mkdir -p ~/.rl-insight/services/tempo
cp "$TMP_DIR/tempo" ~/.rl-insight/services/tempo/tempo
chmod +x ~/.rl-insight/services/tempo/tempo

# Grafana — directory name drops the arch suffix; read it from the tarball
GRAFANA_DIR=$(tar -tzf "$GRAFANA_ARCHIVE" | head -1 | cut -d/ -f1)
TMP_DIR="$(mktemp -d)"
tar -xzf "$GRAFANA_ARCHIVE" -C "$TMP_DIR"
mkdir -p ~/.rl-insight/services/grafana
cp -a "$TMP_DIR/$GRAFANA_DIR" ~/.rl-insight/services/grafana/$GRAFANA_DIR
```

### 3. Verify and start

```bash
~/.rl-insight/services/prometheus/prometheus --version
~/.rl-insight/services/tempo/tempo --version
~/.rl-insight/services/grafana/$GRAFANA_DIR/bin/grafana --version

rl-insight server start
```

---

## Run with Docker

The project also provides an all-in-one image that bundles the CLI together with Prometheus, Tempo, and Grafana. With this approach nothing is installed on the host — no Node.js, no Grafana Labs repository, and no service binaries. Docker is the only requirement.

Build the image from the repository root:

```bash
docker build -f docker/Dockerfile -t rl-insight:latest .
```

To select a different Python base image or pip index, pass `PYTHON_VERSION` / `PIP_INDEX_URL` as `--build-arg` values.

Multi-architecture (amd64/arm64) images are also published to the GitHub Container Registry on every `v*` tag:

```bash
docker pull ghcr.io/verl-project/rl-insight:0.3.0
```

A rolling `nightly` image is additionally rebuilt from `main` (daily schedule and pushes), so you can try the latest unreleased changes without waiting for a version tag:

```bash
docker pull ghcr.io/verl-project/rl-insight:nightly
```

Every nightly build also pushes an immutable `nightly-<date>-<sha>` tag (e.g. `nightly-20260925-3c92f22`), and the same name exists as a git tag in the repository, so a given build can be pinned and reproduced exactly. Stable releases are published under their semver tags only (e.g. `0.3.0`, `0.3`); there is no rolling `latest` tag.

Start the full stack on Linux:

```bash
docker run -d --name rl-insight --network host --stop-timeout 45 \
    --shm-size=2g \
    -v rl-insight-data:/root/.rl-insight/data \
    rl-insight:latest
```

| Option                     | Meaning                                                                                                        |
| -------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `--network host`         | Services bind`0.0.0.0`, so ports 18080 / 9090 / 3200 / 3000 are published on the host directly (Linux only). |
| `--stop-timeout 45`      | Allow time for graceful shutdown (Prometheus data flush) before SIGKILL.                                       |
| `--shm-size=2g`          | Shared memory headroom; the Docker default of 64MB is too small.                                               |
| `-v rl-insight-data:...` | Persist server data in a named volume (see**Data Persistence** below).                                   |

The container runs as `root` and bundles common debugging tools such as `curl` and `vim`. `~` resolves to `/root`, so data stays in the default `~/.rl-insight` layout; the named volume mounts exactly that directory (see **Data Persistence** below).

### Port Mapping

With `--network host`, service ports land on the host unchanged and cannot be remapped. On macOS/Windows, or when a host port is already taken by another process, use bridge networking and publish ports explicitly. The container port after `:` is fixed by each service; the host port before it is freely configurable:

```bash
docker run -d --name rl-insight --stop-timeout 45 --shm-size=2g \
    -p 18080:18080 -p 9090:9090 -p 3200:3200 -p 13000:3000 \
    -v rl-insight-data:/root/.rl-insight/data \
    rl-insight:latest
```

The example above maps Grafana to host port 13000 to avoid a conflict with an existing Grafana instance; browse `http://<server-ip>:13000`.

| Container port | Service           |
| -------------- | ----------------- |
| 18080          | rl-insight web UI |
| 9090           | Prometheus        |
| 3200           | Tempo             |
| 3000           | Grafana           |

### Health Check and Logs

The image defines a `HEALTHCHECK` that polls all four service endpoints every 30s:

```bash
docker inspect --format '{{json .State.Health}}' rl-insight
docker logs -f rl-insight
```

The repository also provides `tests/monitor/special_e2e/docker_smoke_test.sh`, the same four-stage container test used by CI (healthy start, graceful stop, restart, core-service failure):

```bash
tests/monitor/special_e2e/docker_smoke_test.sh rl-insight:latest
```

### Offline Image Distribution

Where a registry is unavailable, distribute the image as a file:

```bash
docker save rl-insight:latest | gzip > rl-insight-latest-linux-arm64.tar.gz
```

Name the file with the architecture it was built for. On the target host, `docker load -i rl-insight-latest-linux-arm64.tar.gz` restores the original name:tag.

---

## Data Persistence

RL-Insight keeps server data on disk. By default, data is stored under `~/.rl-insight/data`:

| Service    | Persistent data                                                          |
| ---------- | ------------------------------------------------------------------------ |
| Prometheus | `~/.rl-insight/data/prometheus` TSDB blocks                            |
| Tempo      | `~/.rl-insight/data/tempo/traces` and `~/.rl-insight/data/tempo/wal` |
| Grafana    | `~/.rl-insight/data/grafana` data, logs, and plugins                   |

`Ctrl+C` and `rl-insight server stop` stop processes only. They do not delete collected metrics, traces, or dashboard state.

Prometheus and Tempo retain data for `30d` by default:

```yaml
prometheus:
  retention_time: 30d
tempo:
  retention_time: 30d
```

Set a different persistent directory in server YAML:

```yaml
server:
  data_dir: /path/to/rl-insight/data
```

## Run Data Isolation

With the Ray backend and a Run-capable server, each initialized training job
receives an immutable Run identity. The existing public API and Ray Hub continue
to collect metrics and traces; Run identity is added without changing metric
prefixes, span names or service names.

```python
import rl_insight

rl_insight.init(
    project="demo",
    experiment_name="experiment",
    config={"server": {"url": "http://<server-host>:18080"}},
)
rl_insight.metric_gauge("loss", 0.25)
context = rl_insight.get_context()
# Workers using the existing job Hub obtain the same context.
rl_insight.finish()
```

The server creates a fresh `run_id` even when an experiment name is reused.
Repeated creation with the same request ID is idempotent. `RunContext` contains
only identity, experiment metadata and the server URL; no framework objects or
backend credentials are included. Other clients can implement the optional
`MonitorClient.get_context()` method without changing the event interface.

Normal target registration through `update_prometheus_config` carries the Run
identity. Registrations made before logger initialization are deferred until a
context is available. A worker retries with backoff while targets remain pending,
including initialization longer than 30 seconds and failed registration requests;
a target leaves the queue only after successful registration. Unbound legacy
targets remain unclassified. No framework source patch or native exporter wrapper
is required for this increment.

Under the existing `server.data_dir`, configuration and readable metadata are
organized as:

```text
runs/<run_id>/
    run.json
    config.json
```

Directories use the full Run ID; project and experiment names stay in metadata.
Names are never used as path components, so renaming display metadata requires
no directory relocation. Short Run IDs are for display only.
SQLite is the identity catalog. JSON files are recoverable projections, not
editable control configuration. Only allowlisted observability config fields are
saved. These directories do not partition Prometheus/Tempo raw storage and do
not guarantee indefinite sample retention or implement data export.

Existing catalog entries remain valid. Reading a Run's metadata endpoint or
retrying its creation rebuilds snapshots at the new path from SQLite. Old
experimental directory snapshots are not automatically moved or deleted.

Run-scoped query endpoints are:

- `/api/v1/runs/<run_id>/prometheus/api/v1/query` and `query_range`
- `/api/v1/runs/<run_id>/prometheus/api/v1/series`, `labels`, `label/<name>/values`
- `/api/v1/runs/<run_id>/tempo/api/search` and `traces/<trace_id>`

PromQL selectors are constrained to the selected Run. TraceQL supports boolean
combinations of spansets; unsupported syntax is rejected, never sent unscoped.
Trace detail responses must belong entirely to the Run. Search results include
only identified matching spans, with root metadata derived from those spans.
Responses and requests have bounded budgets; oversized or incomplete searches
fail explicitly.

`/ui/runs/<run_id>` opens the original full verl/vLLM dashboard with Run-scoped
data sources. Its native Grafana filter bar includes a single-select **Run ID**
variable alongside the existing experiment filters, present by default even
before the first training job (with no options until a Run is registered). Each
option displays and selects the full Run ID. Finished jobs remain selectable
from the persistent Run catalog; a live trainer or worker is not required.
Selecting a Run switches both Prometheus and Tempo to that Run's scoped data
sources. There is no All/multi-Run option. The existing dashboard keeps its UID,
title and folder permissions; no per-Run dashboard is created. It is available
from the original Grafana folder as well as the RL-Insight entry. Run creation
and server startup refresh the options, including Runs with no remaining backend
samples. Reopening the entry also refreshes them. A refresh failure is logged
without preventing Run creation; the original dashboard must already be
provisioned. Bound runtime files are updated so file provisioning retains the
Run filter. On restart, data-source URLs use `server.public_url` when set,
otherwise the current server port, rather than the historical registration URL.

`/api/v1/runs/<run_id>/view?template=<relative-json-path>` supports
another deployed Grafana v2 template using the same data sources. Panel layout,
visualization and metric expressions remain unchanged. Templates requiring
unsupported query syntax report that limitation; this increment does not claim
all Agentic/third-party templates are validated.

The default verl timeline names lanes by `state_lane_id` (for example `rank_0`
or `replica_0`), without Grafana's `Trace Name` field prefix. Its state values,
colors and ordering are preserved.

Legacy servers and clients retain the original behavior. Run scoping applies
only when both sides support Run identity, and is not authentication/RBAC.
Direct access to the shared backends bypasses these query routes.

The direct-emit push backend retains its URL-free initialization, local target
registration and sink routing. It does not use this server-managed Run catalog
or query isolation. No Run-capable server is required for push operation.

This server supports concurrent observation of independent Ray jobs; it does
not schedule training tasks. Leased, proof-wrapped endpoints use the fixed Run
collection path described below. Address-only registrations remain on the
compatibility path and cannot prove ownership after abnormal port reuse.
Run status is explicitly updated by the creating client; absence of active
endpoints is not treated as successful training completion.

`finish()` invokes the optional `MonitorClient.flush()`, releases resources with
`close()` when supported even if flushing fails, then clears local state. The
Ray client drains prior submissions, flushes queued OTLP spans and,
when Run-aware Prometheus is enabled, waits for a scrape to store its internal
`rl_insight_sdk_flush_sequence` marker. This keeps the Hub available for the
final SDK metric sample without adding sleeps to training scripts. The Hub has
a 30-second flush budget and the client waits at most 35 seconds. Failure is
logged and local state is still cleared; unavailable backends cannot guarantee
delivery. Custom clients may keep the default no-op. This does not stop the
shared Hub or guarantee native exporter shutdown ordering.

Local HTTP/Ray E2E tests run with the monitor suite. To run sequential Runs
against a separately deployed real stack:

```bash
RL_INSIGHT_RUN_E2E_URL=http://<server-host>:18080 \
  pytest tests/monitor/e2e/test_run_isolation.py::test_sequential_runs_on_managed_backends
```

The acceptance test uses explicit collector ports 19102 and 19103; ensure they
are available. It checks actual stored metrics, traces, cross-Run trace-ID reads
and reuse of the original Grafana dashboard. It emits a final metric immediately
before finish and checks that value after Ray shutdown. No concurrent training
or accelerator is required.

## Concurrent Run Monitoring

Start one shared server stack with the existing commands. Use one server process
per stack; the ephemeral endpoint leases are not a distributed registry. Independent training
jobs use the same `server.url` and existing API. The default Hub metrics port
is now `0`: the operating system allocates a port when the HTTP server binds.
Workers connect to their own job's Hub and obtain the same RunContext. Explicit
fixed ports remain supported and fail clearly on conflicts.

```python
import ray
import rl_insight

config = {"server": {"url": "http://<server-host>:18080"}}
ray.init(address="auto")
rl_insight.init(project="demo", experiment_name="same-name", config=config)
# Pass the same config to workers; an existing Ray cluster does not automatically
# inherit environment variables from a newly connected driver.
rl_insight.metric_gauge("loss", 0.25)
rl_insight.finish()
```

The Hub creator owns Run shutdown. A worker's finish flushes its submissions
and clears its local client, without ending other workers or the shared Hub.
Creator finish fences late events, flushes OTLP and waits for the final SDK scrape
marker before closing its Run. The existing 30/35-second flush budgets remain;
endpoint and HTTP cleanup are separately bounded. `run.json` records closing /
ended status and `final_collection_confirmed`. A failed flush still permits
cleanup; it does not claim final data delivery.

Leased endpoints are registered using `/api/v1/runs/<run_id>/endpoints`.
Prometheus scrapes `/api/v1/runs/<run_id>/metrics` through the same server port.
The existing file_sd mechanism discovers one target per active Run; managed
endpoints are removed from direct target groups to prevent duplicate sampling.
The Run scrape job uses `honor_labels` to preserve endpoint job/instance labels.
Legacy targets for a normally closed Run are also removed. Targets in manually
configured static jobs remain the deployment owner's responsibility.

Strict endpoints publish literal IPv4/IPv6 addresses; server-side collection
does not perform DNS resolution. The server connection URL can still use a
hostname.

The server verifies the actual response Run/incarnation and the current lease;
TTL alone is not proof of endpoint identity. Default TTL is 30 seconds, renewed
at one-third TTL. `server.endpoint_ttl` can set 1–3600 seconds. On server restart
active leases are discarded; publishers re-register the same Run and endpoint,
without creating a new Run. Ended Runs cannot be reactivated. Hub/job failure
replay and multiple sequential Runs inside one Ray job are outside this version.

Standard WSGI/ASGI applications can reuse the public response wrappers and
publisher, without server changes or framework-specific adapters:

```python
from uuid import uuid4
import rl_insight

context = rl_insight.get_context()  # Obtain in an initialized job.
incarnation = uuid4().hex
app = rl_insight.metrics_asgi_app(app, context, incarnation)
# Start the wrapped app using your existing HTTP runtime, then use its actual
# bound address (not a port guessed before binding).
publisher = rl_insight.publish_metrics_endpoint(
    context, actual_metrics_url,
    component="native", instance="replica-0", incarnation=incarnation,
    labels={"job": "native", "replica": "0"},
)
# Keep the publisher for the endpoint's lifetime and close it before shutdown.
publisher.close()
```

`metrics_wsgi_app` supports the same identity contract for a WSGI application.
Wrapping and publication are explicit; this version does not globally patch
HTTP libraries or auto-discover all vLLM/TQ applications. Existing address-only
`update_prometheus_config` calls keep their direct collection behavior. They do
not acquire proof/lease guarantees merely by supplying a URL; publisher handles
must own renewal. An already proved address cannot also enter direct collection.
Compatibility registration can add nonconflicting description labels to an
already proved endpoint without creating another publisher or extending its
lease. Publishers absorb those labels on renewal for subsequent re-registration.
The independent push backend and its poller/sinks are unchanged.

Each Run has at most four concurrent endpoint reads (including registration
verification), with a shared sixteen-slot
pool; endpoints per Run are limited to 64. Responses are bounded to 4 MiB per
endpoint and 16 MiB per aggregate. Collection respects the
[X-Prometheus-Scrape-Timeout-Seconds header](https://github.com/prometheus/prometheus/blob/main/scrape/scrape.go)
with response margin, capped at nine seconds. HTTP reads have a socket deadline;
one slow endpoint cannot create an unlimited queue or consume the entire pool.
Supported exposition is Prometheus text 0.0.4; OpenMetrics/exemplars are not
claimed as losslessly forwarded.

Partial failures return successful samples plus `rl_insight_proxy_endpoint_up`,
`rl_insight_proxy_endpoints`, `rl_insight_proxy_failures` and
`rl_insight_proxy_empty`. All-endpoint failure returns 503. An empty Run returns
an explicit empty marker. Proxy target up does not mean every exporter is healthy.
Original Grafana layouts, Run filtering, queries and trace collection are reused;
no dashboard copies or new UI modules are added.


## Choosing bundled backends or platform services

### Configure monitoring in the training task

Keep the original training command. The existing framework logger passes its
monitoring configuration to `rl_insight.init(config=...)`; the SDK expands its
bundled template and prepares services automatically. No training command wrapper,
second monitor YAML, or generated configuration file needs to be passed by users.
An uninstrumented framework still needs its ordinary SDK integration.

For example, verl already passes `trainer.rl_insight` through its logger:

```yaml
trainer:
  logger: [console, rl_insight]
  rl_insight:
    enabled: true
```

This uses the existing Ray transport and bundled Prometheus, Tempo and Grafana.
Install their binaries once in the task image with `rl-insight server install`.
An initialized Ray runtime is required for the bundled SDK transport; the external
outputs below also support an ordinary process without Ray initialization.

For a platform that pulls metrics and accepts OTLP/HTTP traces:

```yaml
trainer:
  logger: [console, rl_insight]
  rl_insight:
    metrics:
      pull: true
    traces:
      endpoint: ${oc.env:OTEL_EXPORTER_OTLP_TRACES_ENDPOINT}
      headers:
        X-Scope-OrgID: ${oc.env:PLATFORM_TENANT_ID}
    labels:
      task_id: ${oc.env:PLATFORM_TASK_ID}
```

For metrics push, replace only `metrics` with native remote-write destinations:

```yaml
trainer:
  logger: [console, rl_insight]
  rl_insight:
    metrics:
      remote_write:
        - url: ${oc.env:PROMETHEUS_REMOTE_WRITE_URL}
          headers:
            X-Scope-OrgID: ${oc.env:PLATFORM_TENANT_ID}
    traces:
      endpoint: ${oc.env:OTEL_EXPORTER_OTLP_TRACES_ENDPOINT}
      headers:
        X-Scope-OrgID: ${oc.env:PLATFORM_TENANT_ID}
    labels:
      task_id: ${oc.env:PLATFORM_TASK_ID}
```

The platform supplies its existing environment values; users do not create
`RL_INSIGHT_CONFIG` or manually export monitoring connection variables. Existing
framework enablement and runtime environment propagation remain in that
framework's training configuration. Optional `metrics.binary_path`,
`metrics.data_dir`, `metrics.scrape_interval` and `metrics.scrape_timeout` reuse
native Prometheus configuration. Keep Agent WAL isolated by task, tenant and
destination. Remote write requires an installed Prometheus Agent binary.

`enabled: false` disables this SDK configuration. With no external outputs,
`enabled: true` selects the bundled stack. Declaring external outputs enables
only those signals: trace-only starts no server, and `metrics.pull` and nonempty
`metrics.remote_write` are mutually exclusive. Use `enabled: true` for an otherwise
empty configuration fragment so existing logger calls with no configuration keep
their original behavior. Explicit `rl_insight` envelopes are also accepted by the SDK.
`labels` supplies fixed metric labels and trace attributes, independent of the
Tempo tenant header; the SDK never invents a platform task ID.

For advanced sink options, use `sinks` as the complete output list in the same
training configuration. Choose either `sinks` or the simplified `metrics`/`traces`
fields; mixing them is rejected before services start.

```yaml
trainer:
  logger: [console, rl_insight]
  rl_insight:
    labels:
      task_id: ${oc.env:PLATFORM_TASK_ID}
    sinks:
      - name: metrics
        driver: rl_insight.client.push.sinks.prometheus:create_sink
        streams: [metric]
        metric_mapping:
          - {source: loss, name: training_loss, op: store}
      - name: traces
        driver: rl_insight.client.push.sinks.otlp:create_sink
        streams: [trace]
        endpoint: ${oc.env:OTEL_EXPORTER_OTLP_TRACES_ENDPOINT}
        headers:
          X-Scope-OrgID: ${oc.env:PLATFORM_TENANT_ID}
        service_name: training
        strip_name_prefix: trace.
```

Sink-specific fields are passed to the existing driver factory without a new
plugin protocol. Custom drivers use the same `package.module:factory` syntax and
must be installed in the process running the sinks (the Ray head Hub for Ray
jobs). Factories continue to validate their own options and use the existing
sink-loading failure behavior.

Shared `labels` apply to every sink; per-sink labels can add fields but cannot
change shared identity or supply a Run ID. Names must be unique (the driver path
is the default name); identical sink configurations are rejected even with
different names. At most one built-in Prometheus sink is allowed. Built-in OTLP
sinks cannot subscribe overlapping streams to the same endpoint. Custom factories
remain responsible for checking destinations hidden in driver-specific options.

For the built-in Prometheus sink, the SDK supplies `server_url`, starts/reuses the
task collector and preserves raw native exporter metrics. Configure a shared
collector URL through `server.url`, rather than overriding it inside the sink.
`metric_mapping` filters SDK scalar events only; unmapped scalar events are
omitted, while raw native metrics are retained. An OTLP/custom-only output list
starts no collector or bundled storage services. Automatic native Agent remote
write remains available through the simplified `metrics.remote_write` interface;
`sinks` does not add a second Agent configuration interface.

For Ray jobs the existing job-scoped Hub owns startup and is placed on the Ray
head. Workers join it and obtain resolved configuration automatically; they do
not start services or receive Tempo/remote-write credentials. Native endpoints
registered before SDK initialization wait for that job's configuration. A
trace-only job ignores native metric registration. Other runtimes can use the
same generic service session from their existing driver initialization; they
must supply a reachable head URL when configuring separate processes to connect.

The API port defaults to 18080 (`server.port`). The metrics endpoint inherits
that port unless `metrics.port` is set. For example, keep registration/discovery
on 18080 and expose metrics separately on 18480:

```yaml
trainer:
  logger: [console, rl_insight]
  rl_insight:
    server:
      port: 18080
    metrics:
      pull: true
      port: 18480
```

Prometheus scrapes `http://<head-address>:18480/metrics`; workers still register
through `http://<head-address>:18080`. Both listeners share one collector and its
lifecycle. Omitting `metrics.port` uses the API listener, without opening another
port. An explicitly configured nonzero metrics port must be available; it will
not silently move. Setting it to 0 selects a separate available port, reported
by the API's `/api/v1/services` as `metrics_port`. Remote-write Agent scrapes this
same metrics port internally. Trace destination ports belong in `traces.endpoint`
and are unaffected.

Optional `server.namespace`, `server.endpoint_ttl` and `server.url` live in the
same monitoring section. `server.url` connects to an existing compatible service;
a nonzero `metrics.port` must match that service's advertised metrics port.
Use a declared fixed metrics port matching the platform's existing discovery
contract for K8s scraping; port configuration does not install discovery rules.
Pod networks can reuse 18080 across Pods; hostNetwork tasks need distinct ports.
The platform still owns head Service selection, K8s discovery/relabel and network
reachability. A remote exporter must advertise an address reachable from the head.

`finish()` flushes the owner and retains final task metrics for an endpoint TTL
(default 45 seconds), then stops SDK-owned collector/Agent processes. A small
owner watchdog also reclaims these processes if their owner is killed, without
replacing framework signal handlers. Abrupt termination cannot guarantee final
unflushed samples or traces. Existing shared services are preserved. A full
bundled stack remains available for historical dashboards after the Ray job exits;
the Hub worker retains these service processes. A fresh bundled instance uses
Grafana bootstrap credentials for its Run view client. Existing services retain
their deployment settings. Its
service URL and private runtime configuration path are logged for administration.
Generated runtime YAML uses 0600 permissions and does not print credentials.
Conflicting legacy `RL_INSIGHT_CONFIG`, backend or server URL settings fail before
starting services. Existing low-level configurations remain supported separately.

### Existing explicit service configuration

Training instrumentation uses the same `init`, metric, trace and `finish` APIs.
Deployment configuration determines who operates collection, storage and queries.
The following describes the two supported configurations, rather than introducing
a new `managed/platform` setting.

| Concern | RL-Insight backends | External platform services |
| --- | --- | --- |
| Server lifetime | Deploy a shared full stack before training; independent tasks reuse it. | Start one task-local collector on the head node, before workers; retain it through shutdown grace. |
| Services | RL-Insight starts/configures its Prometheus, Tempo and Grafana. | Disable all three bundled backends. Configuring remote_write additionally starts a head-only Prometheus Agent for forwarding. |
| Worker configuration | Existing Ray backend and shared `server.url`. | Existing push backend with Prometheus and OTLP sinks; distribute its YAML and environment to every worker. |
| Task identity | RL-Insight creates a RunContext and unique `run_id`. | Platform owns identity; collector creates no RunContext or Run ID. |
| Metrics | Local Prometheus discovers and scrapes each Run through the server. | Platform pulls the head `/metrics`, or a head-only Agent forwards that same entry via remote_write. |
| Traces | Existing Hub path exports to the stack's Tempo. | Each SDK trace sink exports to the configured platform OTLP endpoint or platform OTel Collector. |
| Queries and UI | Original Grafana dashboards with Run ID selection and Run-scoped queries. | Platform query/UI and task filters; this collector provisions no dashboard or data source. |
| Data retention | Stack storage plus per-Run metadata directories. | Platform retention; task-local endpoint leases and final snapshots are in memory. |
| Final delivery | Existing SDK flush marker can confirm the local Prometheus scrape. | `finish` flushes traces and preserves a bounded metrics snapshot; external metrics persistence is not acknowledged. |

For bundled backends, start the shared stack as described above, provide
`RL_INSIGHT_SERVER_URL` (or `server.url`), and keep the default Ray client.
Multiple tasks may share that server and are distinguished by their Run IDs.
The server may be hosted separately from the training nodes.

For a platform deployment, its job template supplies the collector startup,
`RL_INSIGHT_CONFIG`, reachable head URL, OTLP endpoint and optional task tags.
The training user continues calling the same SDK APIs and does not need to
configure Prometheus discovery or choose sink drivers in training code. An
existing Ray cluster still requires the template to propagate configuration
to its workers. Changing an HTTP URL alone cannot select between the two
pipelines: both services use HTTP and expose different capabilities. This first
path uses explicit existing client/sink configuration; it does not guess from
URL spelling or silently fall back to a full stack.

The `push` backend name describes dispatching SDK events into sinks. The
Prometheus sink **exposes** metrics for pulling; it does not send metrics to a
Prometheus query URL. Only the trace sink needs the platform's ingest URL.
Neither configuration automatically instruments a framework that has no SDK
calls or endpoint registration. Existing instrumentation can keep its API calls;
new producers use those same generic APIs without RL-Insight framework adapters.

## Platform Prometheus pull / remote write and OTLP traces

For a platform that owns task identity and already provides Prometheus and Tempo,
start **one collector on the training head node per task**. Workers run SDK sinks;
they do not start additional collectors. Disable the bundled backends using the
existing service flags. With all three disabled, the HTTP server exposes a fixed
`/metrics` entry and task-local endpoint leases, with no Run catalog or query UI.

Head-node configuration (`collector.yaml`):

```yaml
server:
  port: 18080
  runtime_dir: ${oc.env:RL_INSIGHT_TASK_DIR}/runtime
  state_file: ${oc.env:RL_INSIGHT_TASK_DIR}/state.json
  endpoint_ttl: 45
prometheus:
  enable: false
tempo:
  enable: false
grafana:
  enable: false
```

```bash
# RL_INSIGHT_TASK_DIR is an isolated writable directory supplied by the deployment.
rl-insight server start --config collector.yaml --detach
```

Publish the head node's reachable URL to every worker, for example
`RL_INSIGHT_SERVER_URL=http://<training-head>:18080`. Mount the following SDK YAML
in every worker and set `RL_INSIGHT_CONFIG` to its path. Do not use a worker's
loopback address as the shared collector URL.

```yaml
server:
  backend: push
  url: ${oc.env:RL_INSIGHT_SERVER_URL}
push:
  # Match the existing trainer dashboard metric names.
  metric_prefix: rl_insight_monitor_
  sinks:
    - name: prometheus
      driver: rl_insight.client.push.sinks.prometheus:create_sink
      streams: [metric]
      server_url: ${server.url}
      # Optional: explicitly advertise the worker Pod IP if automatic routing
      # chooses an address the head node cannot reach.
      # advertise_host: ${oc.env:POD_IP}
      tags_from_env:
        - tag: task_id
          env: PLATFORM_TASK_ID
    - name: tempo
      driver: rl_insight.client.push.sinks.otlp:create_sink
      streams: [trace]
      endpoint: ${oc.env:OTEL_EXPORTER_OTLP_TRACES_ENDPOINT}
      tags_from_env:
        - tag: task_id
          env: PLATFORM_TASK_ID
```

Keep existing `rl_insight.init`, metric and trace API calls. The Prometheus sink
uses a private registry and dynamic HTTP port per process. Counter events
increment, gauges set the current value, and histograms record observations.
Metric names normalize non-Prometheus characters to underscores; timer values
retain the sink interface's microsecond unit. Set names/mappings deliberately
when using an existing dashboard.

Existing `update_prometheus_config` calls register native vLLM/TQ or other
exporters with the same task collector. Raw metric families, including native
histogram buckets, are forwarded without PR181's rollout translation. Do not
combine this sink with `push.rollout.metrics`; that ambiguous configuration
logs a warning and disables monitoring rather than double collecting data.
Only one raw-target collector may be configured per process.

The platform must discover and scrape **only the head collector** at
`http://<head>:18080/metrics`, and add its authoritative task label through the
platform's existing Kubernetes discovery/relabel configuration. A scrape target
belongs to one task; the collector generates no task or Run ID. Optional
`tags_from_env` on the Prometheus sink applies to SDK and registered native
metrics too, as enabled in the example. Omitting it adds no task label. Supply
the same task identity as the Pod label; when relabel also sets `task_id`, the
platform's label conflict policy applies (`honor_labels: false` gives target
labels precedence and may rename the exporter label to `exported_task_id`).
These labels are transparent metadata, not authentication. For
Tempo, use the platform Collector's identity enrichment or the trace sink's
`tags_from_env`; Prometheus relabel does not enrich traces. The trace sink accepts
`headers` for deployment-provided OTLP authentication.

Worker-to-head registration and head-to-worker dynamic metrics ports must be
reachable. The registration API assumes a task-isolated network and is not a
multi-tenant authorization boundary. SDK exporters carry an incarnation proof;
URL-only native exporters cannot prove ownership after port reuse without
explicit response wrapping. Endpoint renewal re-registers live exporters after
collector restart, without creating a task identity. Native registration network
failures are logged and retried without failing the training caller. Start the
head collector before SDK initialization so its SDK sink can register initially.

On graceful worker close, the collector captures a bounded final snapshot and
serves it for one endpoint TTL, even after the worker HTTP server exits. Keep the
head collector alive for this grace period; choose TTL at least as long as the
platform's scrape interval plus scrape timeout. This is **not** an acknowledgment
that the external Prometheus persisted the final value. Collector restart loses
in-memory final snapshots, and abrupt worker exit has no final-snapshot guarantee.
No backend storage, platform dashboard provisioning or durable replay is added.

Inspect `rl_insight_proxy_endpoint_up` and `rl_insight_proxy_failures` in addition
to Prometheus target `up`: partial endpoint failures may still return HTTP 200.

### Choosing pull or push

The SDK's `server.backend: push` selects event dispatch to sinks. It does not
select the Prometheus wire protocol. Keep the same PrometheusSink and endpoint
registration for both transports:

| Configuration | Head processes | Platform integration |
| --- | --- | --- |
| No `prometheus.remote_write` (default) | Task collector | K8s discovery pulls the fixed `/metrics`. No local Prometheus binary is needed. |
| Nonempty `prometheus.remote_write`, bundled backends disabled | Task collector + Prometheus Agent | Agent scrapes `/metrics` locally and pushes to the configured ingest URL. K8s scrape discovery is unnecessary for this path. |

For remote write, add the following fields to the same YAML used by the head
CLI. All SDK sink fields stay unchanged. Native Prometheus remote_write options,
including headers, authentication, TLS and queue_config, are passed through:

```yaml
prometheus:
  enable: false
  scrape_interval: 15s
  scrape_timeout: 10s
  external_labels:
    task_id: ${oc.env:PLATFORM_TASK_ID}
  remote_write:
    - url: ${oc.env:PROMETHEUS_REMOTE_WRITE_URL}
      headers:
        X-Scope-OrgID: ${oc.env:PLATFORM_TENANT_ID}
```

The URL must be a complete remote-write ingest URL (for a Prometheus receiver,
the usual path is `/api/v1/write`), not its query API or a Pushgateway URL.
Remote write does not inherit K8s target relabeling. SDK `tags_from_env` labels
remain available; `external_labels` fills missing labels, including on proxy
health metrics. Both must use the same platform identity. Existing sample labels
take precedence over external_labels. Tenant headers are separate from task_id.
Do not also scrape the same task into the same destination unless deliberate
duplicate collection is required; the pull entry remains available for diagnosis.

The existing install command installs only the Agent dependency with this
configuration; Tempo and Grafana stay disabled:

```bash
rl-insight server install --config monitor.yaml
rl-insight server start --config monitor.yaml --detach
```

Configure `prometheus.binary_path` when the binary is already in the image.
Agent mode requires Prometheus >=2.32; 2.x uses `--enable-feature=agent`, and
3.x uses `--agent`, chosen from the actual installed binary version. The Agent's
HTTP interface is bound to the matching IPv4/IPv6 loopback. Its default WAL is
`<server.runtime_dir>/prometheus/agent`, so task-specific runtime directories
also isolate the queues. An explicit `prometheus.data_dir` (or `server.data_dir`)
overrides that base. It does not enable local TSDB queries,
alerting rules or dashboards. Set `prometheus.data_dir` to a task-specific
persistent volume if recovery must survive Pod replacement; keep the destination
and tenant unchanged when reusing that WAL.

The native Agent handles batching, Snappy/protobuf, retries and WAL replay.
The process runtime allows five seconds for its remote flush within the existing
eight-second termination budget. Samples already in the WAL may be retried on
restart while that directory is retained; samples never scraped are not buffered.
Retain the head through the final-snapshot grace period before stopping it.
An unavailable receiver, exhausted disk or deleted ephemeral Pod volume cannot
guarantee delivery. Inspect the Agent's `prometheus_remote_storage_*` metrics and
logs in addition to collector health.

An enabled full Prometheus backend can also use `prometheus.remote_write`; that
configuration retains its existing TSDB and Run behavior rather than Agent mode.

## Next Step

Continue with [Quick Start](./quick_start.md) to start the stack, instrument training code, and open Grafana.
