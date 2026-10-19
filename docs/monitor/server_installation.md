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

## Selecting experiment executions

With the Ray backend and a Run-capable server, use **Experiment** in any built-in
Grafana dashboard to select an execution. Labels such as
`gsm8k_20261019_143025_a1b2c3d4` include the experiment name, UTC creation time and
short ID. Keep the original experiment name in training config; selection
switches both metrics and traces. Each dashboard lists only executions with matching
metrics in the selected time range; changing Project or the range refreshes the choices.

## Concurrent Run Monitoring

Independent training jobs can share the same server, Prometheus, Tempo and Grafana.
Use the existing API and server URL. Each Ray job has its own Hub and RunContext;
the default Hub port is `0`, so the OS allocates a free port. Explicit fixed ports
remain supported; set `prometheus.metrics_report_port: 9092` to retain the old
port, or give concurrent jobs different fixed ports.

```python
import ray
import rl_insight

ray.init(address="auto")
rl_insight.init(
    project="demo", experiment_name="same-name",
    config={"server": {"url": "http://<server-host>:18080"}},
)
rl_insight.metric_gauge("loss", 0.25)
rl_insight.finish()
```

Pass the same configuration/environment to workers. Connecting a driver to an
existing Ray cluster does not propagate its environment automatically. Existing
framework integrations can keep their `update_prometheus_config` calls; no
framework-specific adapter or exporter wrapper is required.

### Direct collection and target lifetime

Prometheus scrapes the actual Hub/native exporter addresses. The server manages
registration through the existing `/api/v1/prometheus/targets` route and writes
the existing file_sd file. It does not fetch or aggregate exporter metrics.
**Prometheus must be able to reach each exporter address and dynamic port**, even
when the server is on another machine. Trace OTLP and Run-scoped dashboard queries
follow the PR201 paths unchanged.

With RunContext, the new SDK automatically includes a process-level publisher ID
and renews registrations using one background thread per process. Default TTL is
30 seconds, renewed every TTL/3; `server.endpoint_ttl` accepts 1–3600 seconds.
The server removes expired targets within an additional 5 seconds; Prometheus
then observes its usual file_sd refresh. A process that dies stops renewing.
An exporter stopped inside a still-running publisher process is reported through
Prometheus `up=0`; endpoint health is not inferred by the server.

One address cannot belong to different active Runs or publishers. Target metadata
retains Run ownership even when samples already contain run_id. Internal discovery
labels do not appear in stored metrics. No new component/endpoint labels or metric
names are added. Original scrape profiles, including native metric options, remain
in effect.

The Hub creator owns Run shutdown. Worker `finish()` only flushes; creator finish
first closes admission, waits for the existing final SDK scrape/OTLP flush, then
ends this Run and removes only its targets. Existing targets can renew while
closing. Call creator finish after workers have finished. Run metadata records
ended status and final collection confirmation; failed flush never claims delivery.
Historical metrics/traces remain queryable. This does not guarantee a final scrape
of native exporters that exit independently.

Use one server process per stack. Run metadata survives restart, but renewable
target state is ephemeral: on restart the server clears stale discovery entries,
and live SDK processes re-register the same Run. A short collection gap is possible.
There is no distributed registry or HA guarantee.

### Compatibility and limits

Old clients without a publisher ID retain persistent target registration; they
do not gain automatic expiry. Run-less/manual registrations remain persistent.
New SDKs talking to a PR201 server retain direct scraping, but cannot gain renewal
or owner-close guarantees that server does not support. Upgrade the shared server
first. The separate push backend is unchanged; platform mode and external
forwarding are outside this feature.

Direct scraping does not verify response ownership on every scrape. Registration
ownership and TTL reduce stale target lifetime but do not eliminate address reuse
inside that window. Run isolation is not tenant authentication. Concurrency and
scrape budgets are controlled by Prometheus; this version adds no server-side
metric parsing pool or throughput limit.
