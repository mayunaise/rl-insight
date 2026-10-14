# Run data isolation

Each initialized training job receives an immutable Run identity. The existing
public API and Ray Hub continue to collect metrics and traces; Run identity is
added without changing metric prefixes, span names or service names.

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
context is available. Unbound legacy targets remain unclassified. No framework
source patch or native exporter wrapper is required for this increment.

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
variable alongside the existing experiment filters. Each option displays and
selects the full Run ID.
Selecting a Run switches both Prometheus and Tempo to that Run's scoped data
sources. There is no All/multi-Run option. The dashboard title is neutral so it
stays accurate after switching. Reopening the RL-Insight entry refreshes the
catalog options, including Runs that have no remaining backend samples.

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

This increment implements data isolation, not concurrent-task orchestration.
Port allocation, endpoint leases/proofs, fixed-entry metric aggregation and
independent task shutdown are separate work. Native target attribution assumes
the registered address still belongs to the publisher; port-reuse protection
requires that later increment. `finish` keeps its original local behavior;
catalog entries do not infer training completion from a worker finishing.

Local HTTP/Ray E2E tests run with the monitor suite. To run sequential Runs
against a separately deployed real stack:

```bash
RL_INSIGHT_RUN_E2E_URL=http://<server-host>:18080 \
  pytest tests/monitor/e2e/test_run_isolation.py::test_sequential_runs_on_managed_backends
```

The acceptance test uses explicit collector ports 19102 and 19103; ensure they
are available. It checks actual stored metrics, traces, cross-Run trace-ID reads
and Grafana provisioning. No concurrent training or accelerator is required.
