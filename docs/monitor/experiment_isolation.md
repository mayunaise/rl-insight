# Experiment-Scoped Target Isolation and Archiving

This document describes how RL-Insight isolates Prometheus targets (and the
identities written into metrics and traces) per experiment, and how to archive
and restore an experiment.

## Experiment identity

An experiment is identified by the **name pair `(project, experiment_name)`**:

- Both fields are required together. Server-side operations (registration,
  archive, restore) reject requests that provide only one of the two, or whose
  values are empty after trimming surrounding whitespace.
- The pair is case-sensitive and Unicode-preserving; values are compared after
  whitespace trimming (`" exp-1 "` and `"exp-1"` are the same experiment).
- `project` and `experiment_name` are **reserved labels**. The server injects
  them into every discovery record, and per-target labels may not override
  them. Calls that set metric labels or trace attributes conflicting with the
  identity passed to `init()` fail with an explicit error.
- Metrics and traces use the same two label names, and Grafana variables
  filter by both. Selecting `(project-a, exp-1)` in a dashboard therefore
  always resolves to one experiment, including distinguishing
  `project-a/exp-1` from `project-b/exp-1`.

> **Known limitation (phase 1):** two runs that reuse the same
> `(project, experiment_name)` pair are **the same logical experiment**. Their
> targets merge into one partition, their series share the same labels, and
> archiving that key stops discovery for both. Renaming `project` or
> `experiment_name` creates a new experiment; old history is not migrated.
> Phase 1 deliberately does not introduce stable experiment IDs.

## Storage layout

```text
<data_dir>/
  targets/prometheus-targets.yml                        # legacy/global partition
  projects/<sha256(project)>__<sha256(experiment_name)>.active.yml      # watched while active
  projects/<sha256(project)>__<sha256(experiment_name)>.archived.yml    # snapshot while archived
  projects/<sha256(project)>__<sha256(experiment_name)>.manifest.yaml   # state, names, counts
  backups/prometheus-targets.<timestamp>.bak            # pre-migration backup
```

- File-name keys are the two SHA-256 digests of the UTF-8 names joined by
  `__`, so untrusted names can never escape `data_dir`; the readable names
  live in the `.manifest.yaml` file. Digest keys are filesystem keys, not
  business IDs.
- **Why flat?** Prometheus file_sd only accepts wildcards in the *base file
  name* (`patFileSDName` in `discovery/file`), so a single static glob cannot
  watch nested `projects/*/experiments/*/...` paths. One flat watch
  directory keeps the config static (`projects/*.active.yml`) and lets
  archive/restore be a plain atomic rename in place.
- The generated `prometheus.yml` watches the legacy/global file **and**
  `projects/*.active.yml`. Archived files are never watched.
- Every mutation of one experiment is serialized under that experiment's lock
  file and written atomically (temp file + `os.replace`), so Prometheus only
  ever reads a complete file, and concurrent updates to *different*
  experiments never block or overwrite each other.
- Infrastructure targets without experiment ownership (node exporters,
  manually maintained endpoints) stay in the legacy/global partition.

## Registration

Trainer-side `rl_insight.init(project=..., experiment_name=...)` forwards the
normalized identity to the monitor hub, which registers its scrape endpoint
with the composite key; the server writes the target into the matching
partition.

- Old clients (or `init()` without identity) keep the previous behavior:
  their targets go to the legacy/global partition, and the server logs a
  warning so upgrade progress can be monitored.
- Registering into an **archived** experiment is rejected with HTTP 409 and an
  `ArchivedExperimentError` on the trainer side; the message points to
  `restore`. Old processes cannot silently re-enable collection.

## Archive and restore

```text
first valid registration -> active
active  --archive--> archived
archived --restore--> active
```

- `archive` atomically renames the experiment's `*.active.yml` out of the
  Prometheus watch glob (to `*.archived.yml` in the same directory) and
  updates the manifest. It is **idempotent**.
- `restore` renames the archived snapshot back. It is **idempotent**.
- Neither operation deletes the manifest, the target snapshot, or any
  Prometheus TSDB / Tempo data. Metrics and traces written before archiving
  stay queryable by their labels until the storage engines' normal retention
  expires.
- Other experiments and the legacy/global partition are never affected.
- After the file operation the server reloads Prometheus and polls its targets
  API for up to one file_sd refresh interval plus margin, then reports whether
  convergence was confirmed. If Prometheus is unreachable the persisted state
  still stands and file_sd converges on its own refresh cycle.
- If a crash interrupts an archive/restore between the rename and the
  manifest update, the next read repairs the manifest from the discovery file
  location (the file position is the source of truth).

## Upgrade migration

On server start, before accepting registrations, RL-Insight scans the existing
legacy/global `prometheus-targets.yml`:

1. Records carrying both reserved labels move into the matching experiment
   partition; records missing one label, blank, or otherwise unidentified stay
   in the legacy/global file. Nothing guesses ownership.
2. A byte-level backup of the original global file is written to
   `<data_dir>/backups/` before anything changes.
3. The global file is replaced **only after** every partition write succeeded;
   a failure leaves the old discovery state untouched and reports the error.
   Re-runs are idempotent: repeated starts neither duplicate targets nor alter
   archived state.

**Rollback:** stop the server, restore the backup file over
`<data_dir>/targets/prometheus-targets.yml`, and remove the migrated
`projects/` directories recorded for the affected experiments (or the whole
`projects/` tree if no other state matters), then start the previous version.

## API and CLI

HTTP endpoints (prefix `/api/v1`):

- `GET /experiments[?project=...]` — list experiments with state, composite
  key, target count, and update time.
- `GET /experiments/targets?project=...&experiment_name=...` — show one
  partition's discovery records.
- `POST /experiments/archive` / `POST /experiments/restore` — body
  `{"project": "...", "experiment_name": "..."}`; idempotent; unknown
  experiments return 404.

CLI (talks to a running server; `--server-url` defaults to
`$RL_INSIGHT_SERVER_URL` or `127.0.0.1:18080`):

```bash
rl-insight server experiments list [--project project-a]
rl-insight server experiments show --project project-a --experiment-name exp-1
rl-insight server experiments archive --project project-a --experiment-name exp-1
rl-insight server experiments restore --project project-a --experiment-name exp-1
```

The archive command repeats the same-name limitation and warns when
Prometheus did not confirm convergence within one refresh cycle.

## Grafana

Every dashboard carries `project` and `experiment_name` template variables;
the experiment variable's query cascades on the selected project
(`label_values({..., project=~"$project"}, experiment_name)`), so experiments
of the same name in different projects are distinguishable. All PromQL panels
and TraceQL queries constrain both identity labels.
