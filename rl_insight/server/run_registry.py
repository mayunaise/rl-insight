# Copyright (c) 2026 verl-project authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run Registry: the single authority for run identity, endpoints and leases.

Implements the S0 detailed design §3.2 / §5 protocol on one SQLite file.
Lifecycle states beyond ``active``/``archived`` arrive with S4; S1 only needs
the write gate plus the ``archived`` install used by bundle import.

Every state change is guarded by a revision (CAS) and the publication epoch, so
a stale worker can never overwrite a newer target. Run-scoped URLs use the
identity's irreversible ``run_key`` (S0 §3.1) instead of the raw ID, so IDs
containing ``/`` or Unicode never reach a path segment.
"""

from __future__ import annotations

import contextlib
import functools
import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, Callable

from ..sdk.config import DEGRADED, NOT_READY, READY
from ..sdk.identity import (
    MANAGED,
    RunIdentity,
    normalize_project,
    validate_identity_name,
)
from ..sdk.publisher import canonical_address

LEASE_TTL_SECONDS = 30.0
RENEW_AFTER_SECONDS = 10.0
ACTIVE = "active"
ARCHIVED = "archived"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    run_key TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    project TEXT NOT NULL,
    experiment_name TEXT,
    state TEXT NOT NULL,
    revision INTEGER NOT NULL,
    publication_epoch INTEGER NOT NULL,
    server_instance_id TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS run_requests (
    idempotency_key TEXT PRIMARY KEY,
    request_digest TEXT NOT NULL,
    run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS endpoints (
    endpoint_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    slot_key TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    component TEXT NOT NULL,
    instance_key TEXT NOT NULL,
    incarnation TEXT NOT NULL,
    canonical_address TEXT NOT NULL,
    revision INTEGER NOT NULL,
    UNIQUE (run_id, slot_key)
);
CREATE INDEX IF NOT EXISTS endpoints_address ON endpoints (canonical_address);
CREATE TABLE IF NOT EXISTS leases (
    lease_id TEXT PRIMARY KEY,
    endpoint_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    expires_at REAL NOT NULL,
    revoked INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS leases_endpoint ON leases (endpoint_id);
CREATE TABLE IF NOT EXISTS retired_instances (
    run_id TEXT NOT NULL,
    slot_key TEXT NOT NULL,
    incarnation TEXT NOT NULL,
    PRIMARY KEY (run_id, slot_key, incarnation)
);
"""


def _channel(status: str, reason_code: str = "") -> dict[str, str]:
    """One channel entry of the health projection (S2 §3.1)."""
    return {"status": status, "reason_code": reason_code}


def _locked(method):
    """Serialize one Registry call.

    ``# ponytail:`` one process-wide lock over one SQLite connection; that is
    what already enforces the protocol's CAS semantics, and per-run locking is
    only worth adding when a single server stops being the writer.
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper


class RegistryError(RuntimeError):
    """A registry call rejected by the protocol, carrying its wire status."""

    def __init__(
        self,
        code: str,
        message: str = "",
        *,
        status: int = 400,
        retryable: bool = False,
    ):
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message
        self.status = status
        self.retryable = retryable

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "request_id": "",
            "details": {},
        }


class RunRegistry:
    """SQLite-backed run, endpoint and lease registry."""

    def __init__(
        self,
        path: Path | str,
        *,
        server_instance_id: str | None = None,
        active_run_limit: int | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.server_instance_id = server_instance_id or str(uuid.uuid4())
        self.active_run_limit = active_run_limit
        self._now = now
        self._lock = threading.RLock()
        # Observation snapshots for the health projection (S2 §3.1): they carry
        # no CAS authority and are deliberately not durable.
        self._scrape_observations: dict[str, dict[str, Any]] = {}
        self._forwarding: dict[str, dict[str, Any]] = {}
        # FastAPI runs these handlers in a thread pool, so the connection is
        # shared across threads and serialized by ``self._lock``.
        self._db = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)

    @_locked
    def close(self) -> None:
        self._db.close()

    # -- runs ----------------------------------------------------------
    @_locked
    def create_run(
        self,
        *,
        project: str = "",
        experiment_name: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Create one managed run; a repeated idempotency key returns the first result."""
        try:
            normalized_project = normalize_project(project)
            normalized_name = (
                validate_identity_name(experiment_name, "experiment_name")
                if experiment_name
                else None
            )
        except ValueError as exc:
            raise RegistryError("INVALID_IDENTITY", str(exc)) from exc

        key = str(idempotency_key or uuid.uuid4())
        digest = json.dumps(
            {"project": normalized_project, "experiment_name": normalized_name},
            sort_keys=True,
        )
        with self._transaction():
            existing = self._db.execute(
                "SELECT request_digest, run_id FROM run_requests WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
            if existing:
                if existing["request_digest"] != digest:
                    raise RegistryError(
                        "IDEMPOTENCY_CONFLICT",
                        "the idempotency key was already used with different parameters",
                        status=409,
                    )
                return self._run_payload(existing["run_id"])

            self._check_capacity()
            run_id = str(uuid.uuid4())
            self._insert_run(
                RunIdentity(
                    id=run_id,
                    source=MANAGED,
                    project=normalized_project,
                    experiment_name=normalized_name,
                ),
                state=ACTIVE,
            )
            self._db.execute(
                "INSERT INTO run_requests (idempotency_key, request_digest, run_id) VALUES (?, ?, ?)",
                (key, digest, run_id),
            )
        return self._run_payload(run_id)

    @_locked
    def install_imported_run(
        self, identity: RunIdentity, *, state: str = ARCHIVED
    ) -> dict[str, Any]:
        """Install a run that arrived in a bundle; never overwrites an existing id."""
        if state not in (ACTIVE, ARCHIVED):
            raise RegistryError(
                "INVALID_CONFIG", f"unsupported install state {state!r}"
            )
        with self._transaction():
            if self._db.execute(
                "SELECT 1 FROM runs WHERE id = ?", (identity.id,)
            ).fetchone():
                raise RegistryError(
                    "RUN_CONFLICT",
                    f"run {identity.id!r} already exists here",
                    status=409,
                )
            if state == ACTIVE:
                self._check_capacity()
            self._insert_run(identity, state=state)
        return self._run_payload(identity.id)

    @_locked
    def get_run(self, run_key: str) -> dict[str, Any]:
        return self._run_payload(self._row(run_key)["id"])

    @_locked
    def list_runs(
        self,
        *,
        project: str | None = None,
        state: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> list[dict[str, Any]]:
        """List runs in stable ``(created_at, id)`` order with an optional cursor."""
        clauses: list[str] = []
        params: list[Any] = []
        if project:
            clauses.append("project = ?")
            params.append(str(project))
        if state:
            clauses.append("state = ?")
            params.append(str(state))
        if cursor:
            created_at, _, run_id = str(cursor).partition(":")
            try:
                params.extend([float(created_at), run_id])
            except ValueError as exc:
                raise RegistryError(
                    "INVALID_CURSOR", "cursor is not a created_at:id pair"
                ) from exc
            clauses.append("(created_at, id) > (?, ?)")
        sql = "SELECT id FROM runs"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at, id"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        rows = self._db.execute(sql, params).fetchall()
        return [self._run_payload(row["id"]) for row in rows]

    @_locked
    def resume_run(
        self, run_key: str, *, project: str, expected_revision: int
    ) -> dict[str, Any]:
        """Attach to an existing run; only ``active`` runs accept a plain resume."""
        row = self._writable_row(run_key)
        try:
            resume_project = normalize_project(project)
        except ValueError as exc:
            raise RegistryError("INVALID_IDENTITY", str(exc)) from exc
        if resume_project != row["project"]:
            raise RegistryError(
                "OWNERSHIP_CONFLICT",
                "project does not match the stored run",
                status=409,
            )
        if int(expected_revision) and int(expected_revision) != int(row["revision"]):
            raise RegistryError("REVISION_CONFLICT", "stale revision", status=409)
        return self._run_payload(row["id"])

    @_locked
    def writable_identity(
        self, run_key: str, *, epoch: int | None = None
    ) -> RunIdentity:
        """Resolve the identity of a writable run, or raise the protocol error."""
        row = self._writable_row(run_key, epoch=epoch)
        return self._identity_of(row)

    @_locked
    def endpoints_for(self, run_key: str) -> list[dict[str, Any]]:
        """Sanitized endpoint history of one run: no leases, epochs or pids."""
        row = self._row(run_key)
        rows = self._db.execute(
            "SELECT * FROM endpoints WHERE run_id = ? ORDER BY slot_key", (row["id"],)
        ).fetchall()
        return [
            {
                "endpoint_id": item["endpoint_id"],
                "target_kind": item["target_kind"],
                "component": item["component"],
                "instance_key": item["instance_key"],
                "canonical_address": item["canonical_address"],
                "revision": int(item["revision"]),
            }
            for item in rows
        ]

    # -- endpoints and leases ------------------------------------------
    @_locked
    def upsert_endpoint(
        self, run_key: str, slot_key: str, facts: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Register one endpoint slot with a conditional update on its revision."""
        try:
            requested_epoch = (
                None
                if facts.get("lifecycle_epoch") is None
                else int(facts["lifecycle_epoch"])
            )
        except (TypeError, ValueError) as exc:
            raise RegistryError(
                "INVALID_CONFIG", "lifecycle_epoch is not an integer"
            ) from exc
        row = self._writable_row(run_key, epoch=requested_epoch)
        incarnation = str(facts.get("incarnation") or "")
        if not incarnation:
            raise RegistryError("INVALID_CONFIG", "incarnation is required")
        # The Registry derives the authoritative address itself and never takes
        # a client-supplied string: two spellings of one address must not become
        # two owners, and the SDK only ever sends the address parts.
        try:
            address = canonical_address(
                str(facts["scheme"]),
                str(facts["advertised_host"]),
                int(facts["actual_port"]),
                str(facts.get("path") or ""),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RegistryError(
                "INVALID_CONFIG", f"endpoint address cannot be derived: {exc}"
            ) from exc
        expected_revision = int(facts.get("expected_revision") or 0)
        # A lease is always signed with the run's current epoch, so a request
        # that arrived with a stale one cannot mint a lease that outlives it.
        epoch = int(row["publication_epoch"])

        with self._transaction():
            if self._db.execute(
                "SELECT 1 FROM retired_instances WHERE run_id = ? AND slot_key = ? AND incarnation = ?",
                (row["id"], slot_key, incarnation),
            ).fetchone():
                raise RegistryError(
                    "OWNERSHIP_CONFLICT",
                    "this incarnation was replaced and cannot publish again in this run",
                    status=409,
                )

            endpoint = self._db.execute(
                "SELECT * FROM endpoints WHERE run_id = ? AND slot_key = ?",
                (row["id"], slot_key),
            ).fetchone()
            owner = self._db.execute(
                "SELECT endpoint_id FROM endpoints WHERE canonical_address = ?",
                (address,),
            ).fetchone()
            if owner is not None and (
                endpoint is None or owner["endpoint_id"] != endpoint["endpoint_id"]
            ):
                # One normalized address belongs to exactly one slot, in this
                # run and in every other one: two slots would be scraped twice.
                raise RegistryError(
                    "OWNERSHIP_CONFLICT",
                    "this endpoint address is already registered to another endpoint",
                    status=409,
                )

            if endpoint is None:
                if expected_revision != 0:
                    raise RegistryError(
                        "REVISION_CONFLICT",
                        "no endpoint exists for this slot yet",
                        status=409,
                    )
                endpoint_id = str(uuid.uuid4())
                revision = 1
                self._db.execute(
                    "INSERT INTO endpoints (endpoint_id, run_id, slot_key, target_kind, component,"
                    " instance_key, incarnation, canonical_address, revision)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        endpoint_id,
                        row["id"],
                        slot_key,
                        str(facts.get("target_kind") or ""),
                        str(facts.get("component") or ""),
                        str(facts.get("instance_key") or ""),
                        incarnation,
                        address,
                        revision,
                    ),
                )
            else:
                endpoint_id = endpoint["endpoint_id"]
                if (
                    endpoint["incarnation"] == incarnation
                    and endpoint["canonical_address"] == address
                ):
                    revision = int(endpoint["revision"])  # identical retry: idempotent
                elif expected_revision == int(endpoint["revision"]):
                    revision = int(endpoint["revision"]) + 1
                    self._db.execute(
                        "INSERT OR IGNORE INTO retired_instances (run_id, slot_key, incarnation)"
                        " VALUES (?, ?, ?)",
                        (row["id"], slot_key, endpoint["incarnation"]),
                    )
                    self._db.execute(
                        "UPDATE endpoints SET incarnation = ?, canonical_address = ?, revision = ?"
                        " WHERE endpoint_id = ?",
                        (incarnation, address, revision, endpoint_id),
                    )
                else:
                    raise RegistryError(
                        "REVISION_CONFLICT",
                        "endpoint revision changed; re-read before replacing it",
                        status=409,
                    )
            lease = self._issue_lease(endpoint_id, row["id"], epoch)

        return {
            "endpoint_id": endpoint_id,
            "lease_id": lease["lease_id"],
            "revision": revision,
            "lifecycle_epoch": epoch,
            "expires_at": lease["expires_at"],
            "renew_after": RENEW_AFTER_SECONDS,
        }

    @_locked
    def renew_lease(
        self, run_key: str, lease_id: str, facts: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Extend a live lease; expired, revoked and superseded leases stay dead."""
        lease = self._lease_for_write(run_key, lease_id, facts)
        expires_at = self._now() + LEASE_TTL_SECONDS
        self._db.execute(
            "UPDATE leases SET expires_at = ? WHERE lease_id = ?",
            (expires_at, lease_id),
        )
        endpoint = self._db.execute(
            "SELECT revision FROM endpoints WHERE endpoint_id = ?",
            (lease["endpoint_id"],),
        ).fetchone()
        return {
            "lease_id": lease_id,
            "endpoint_id": lease["endpoint_id"],
            # The endpoint's real revision: a client that carries 0 forward
            # would otherwise CAS against a revision that never existed.
            "revision": int(endpoint["revision"]),
            "lifecycle_epoch": int(lease["epoch"]),
            "expires_at": expires_at,
            "renew_after": RENEW_AFTER_SECONDS,
        }

    @_locked
    def delete_lease(
        self, run_key: str, lease_id: str, facts: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Revoke one owned lease; idempotent for the same owner."""
        lease = self._lease_for_write(run_key, lease_id, facts, allow_revoked=True)
        self._db.execute(
            "UPDATE leases SET revoked = 1 WHERE lease_id = ?", (lease_id,)
        )
        return {
            "deleted": True,
            "endpoint_id": lease["endpoint_id"],
            "lease_id": lease_id,
        }

    # -- discovery projection, scrape gate and health (S2 §4) ----------
    @_locked
    def effective_targets(self) -> list[dict[str, Any]]:
        """Endpoints that may be scraped right now (S2 §4.1).

        A target is effective only when its run is writable, the lease is live
        and not revoked, and the endpoint still is the current incarnation. The
        Registry is the authority; the file-SD snapshot is a projection of this.
        """
        rows = self._db.execute(
            "SELECT e.endpoint_id, e.target_kind, e.component, e.instance_key, e.incarnation,"
            " e.canonical_address, e.revision, l.lease_id, l.expires_at,"
            " r.id AS run_id, r.run_key, r.source, r.project, r.experiment_name"
            " FROM endpoints e"
            " JOIN runs r ON r.id = e.run_id"
            " JOIN leases l ON l.endpoint_id = e.endpoint_id"
            " WHERE r.state = ? AND l.revoked = 0 AND l.expires_at > ?"
            " AND l.epoch = r.publication_epoch"
            " ORDER BY r.created_at, r.id, e.slot_key",
            (ACTIVE, self._now()),
        ).fetchall()
        return [dict(row) for row in rows]

    @_locked
    def expire_leases(self) -> int:
        """Mark leases whose TTL passed as revoked; returns how many were retired."""
        cursor = self._db.execute(
            "UPDATE leases SET revoked = 1 WHERE revoked = 0 AND expires_at <= ?",
            (self._now(),),
        )
        return int(cursor.rowcount)

    @_locked
    def scrape_target(self, endpoint_id: str) -> dict[str, Any]:
        """Resolve one endpoint for the controlled scrape route (S2 §4.2).

        The address always comes from the Registry, never from the request, and
        the caller re-reads this after fetching to confirm the lease and
        revision did not move while the scrape was in flight.
        """
        row = self._db.execute(
            "SELECT e.*, l.lease_id, l.expires_at, l.epoch AS lease_epoch, r.run_key,"
            " r.project, r.experiment_name, r.source, r.state"
            " FROM endpoints e"
            " JOIN runs r ON r.id = e.run_id"
            " LEFT JOIN leases l ON l.endpoint_id = e.endpoint_id AND l.revoked = 0"
            " WHERE e.endpoint_id = ?",
            (str(endpoint_id),),
        ).fetchone()
        if row is None:
            raise RegistryError(
                "TARGET_NOT_FOUND", "unknown scrape endpoint", status=404
            )
        if row["state"] != ACTIVE:
            code = "RUN_ARCHIVED" if row["state"] == ARCHIVED else "RUN_NOT_WRITABLE"
            raise RegistryError(code, f"run is {row['state']}", status=410)
        if row["lease_id"] is None or row["expires_at"] <= self._now():
            raise RegistryError(
                "LEASE_EXPIRED", "the endpoint has no live lease", status=410
            )
        return {
            "endpoint_id": row["endpoint_id"],
            "run_key": row["run_key"],
            "run_id": row["run_id"],
            "source": row["source"],
            "project": row["project"],
            "experiment_name": row["experiment_name"],
            "target_kind": row["target_kind"],
            "component": row["component"],
            "instance_key": row["instance_key"],
            "incarnation": row["incarnation"],
            "revision": int(row["revision"]),
            "lease_id": row["lease_id"],
            "lifecycle_epoch": int(row["lease_epoch"]),
            "address": row["canonical_address"],
        }

    @_locked
    def note_scrape(self, endpoint_id: str, incarnation: str = "") -> None:
        """Record that the controlled scrape route served a verified response.

        The incarnation is part of the observation: a new process on the same
        endpoint id must prove its own channel instead of inheriting the old
        process's healthy scrape.
        """
        self._scrape_observations[str(endpoint_id)] = {
            "incarnation": str(incarnation),
            "observed_at": self._now(),
        }

    @_locked
    def note_forwarding(
        self, run_key: str, *, backlog: int, error_code: str = ""
    ) -> None:
        """Record the trace forwarder's backlog snapshot for one run."""
        self._forwarding[str(run_key)] = {
            "backlog": int(backlog),
            "error_code": str(error_code),
            "updated_at": self._now(),
        }

    @_locked
    def run_health(self, run_key: str) -> dict[str, Any]:
        """Per-channel health snapshot of one run (S2 §3.1).

        ``active`` alone never makes a channel ready: the metrics channel needs a
        live registered target, and scrape health needs an observed scrape.
        """
        row = self._row(run_key)
        active = row["state"] == ACTIVE
        run_state_code = (
            "RUN_ARCHIVED" if row["state"] == ARCHIVED else "RUN_NOT_WRITABLE"
        )
        targets = [
            item for item in self.effective_targets() if item["run_id"] == row["id"]
        ]

        if not active:
            channels = {
                name: _channel(NOT_READY, run_state_code)
                for name in ("registration", "metrics_scrape", "trace_accept")
            }
        else:
            if not targets:
                scrape = _channel(NOT_READY, "NO_ACTIVE_TARGET")
            elif any(
                (self._scrape_observations.get(item["endpoint_id"]) or {}).get(
                    "incarnation"
                )
                == item["incarnation"]
                for item in targets
            ):
                scrape = _channel(READY)
            else:
                scrape = _channel(DEGRADED, "SCRAPE_NOT_OBSERVED")
            channels = {
                "registration": _channel(READY),
                "metrics_scrape": scrape,
                "trace_accept": _channel(READY),
            }

        forwarding = self._forwarding.get(run_key)
        if forwarding is None:
            channels["trace_online"] = _channel(NOT_READY, "FORWARDER_NOT_RUNNING")
        elif forwarding["error_code"]:
            channels["trace_online"] = _channel(DEGRADED, forwarding["error_code"])
        elif forwarding["backlog"]:
            channels["trace_online"] = _channel(DEGRADED, "FORWARD_BACKLOG")
        else:
            channels["trace_online"] = _channel(READY)

        return {
            "run_key": row["run_key"],
            "state": row["state"],
            "targets": len(targets),
            "channels": channels,
            "updated_at": self._now(),
        }

    # -- internals ------------------------------------------------------
    def _insert_run(self, identity: RunIdentity, *, state: str) -> None:
        self._db.execute(
            "INSERT INTO runs (id, run_key, source, project, experiment_name, state, revision,"
            " publication_epoch, server_instance_id, created_at) VALUES (?, ?, ?, ?, ?, ?, 1, 1, ?, ?)",
            (
                identity.id,
                identity.run_key,
                identity.source,
                identity.project,
                identity.experiment_name,
                state,
                self.server_instance_id,
                self._now(),
            ),
        )

    def _issue_lease(self, endpoint_id: str, run_id: str, epoch: int) -> dict[str, Any]:
        self._db.execute(
            "UPDATE leases SET revoked = 1 WHERE endpoint_id = ? AND revoked = 0",
            (endpoint_id,),
        )
        lease_id = str(uuid.uuid4())
        expires_at = self._now() + LEASE_TTL_SECONDS
        self._db.execute(
            "INSERT INTO leases (lease_id, endpoint_id, run_id, epoch, expires_at, revoked)"
            " VALUES (?, ?, ?, ?, ?, 0)",
            (lease_id, endpoint_id, run_id, epoch, expires_at),
        )
        return {"lease_id": lease_id, "expires_at": expires_at}

    def _lease_for_write(
        self,
        run_key: str,
        lease_id: str,
        facts: Mapping[str, Any],
        *,
        allow_revoked: bool = False,
    ) -> sqlite3.Row:
        row = self._row(run_key)
        lease = self._db.execute(
            "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        if lease is None or lease["run_id"] != row["id"]:
            raise RegistryError(
                "LEASE_NOT_FOUND", f"unknown lease {lease_id!r}", status=404
            )
        if lease["revoked"]:
            if not allow_revoked:
                raise RegistryError("LEASE_EXPIRED", "lease was revoked", status=410)
            return lease
        if lease["expires_at"] <= self._now():
            raise RegistryError("LEASE_EXPIRED", "lease expired", status=410)
        endpoint = self._db.execute(
            "SELECT * FROM endpoints WHERE endpoint_id = ?", (lease["endpoint_id"],)
        ).fetchone()
        incarnation = str(facts.get("incarnation") or "")
        if incarnation and endpoint and endpoint["incarnation"] != incarnation:
            raise RegistryError(
                "OWNERSHIP_CONFLICT",
                "the lease belongs to a replaced incarnation",
                status=409,
            )
        revision = facts.get("revision")
        if (
            revision is not None
            and endpoint
            and int(revision) != int(endpoint["revision"])
        ):
            raise RegistryError("REVISION_CONFLICT", "stale revision", status=409)
        return lease

    def _row(self, run_key: str) -> sqlite3.Row:
        row = self._db.execute(
            "SELECT * FROM runs WHERE run_key = ?", (run_key,)
        ).fetchone()
        if row is None:
            raise RegistryError(
                "RUN_NOT_FOUND", f"unknown run key {run_key!r}", status=404
            )
        return row

    def _writable_row(self, run_key: str, *, epoch: int | None = None) -> sqlite3.Row:
        row = self._row(run_key)
        if row["state"] != ACTIVE:
            code = "RUN_ARCHIVED" if row["state"] == ARCHIVED else "RUN_NOT_WRITABLE"
            raise RegistryError(code, f"run is {row['state']}", status=410)
        if epoch is not None and int(epoch) != int(row["publication_epoch"]):
            raise RegistryError(
                "STALE_EPOCH", "publication epoch has moved on", status=409
            )
        return row

    def _identity_of(self, row: sqlite3.Row) -> RunIdentity:
        return RunIdentity(
            id=row["id"],
            source=row["source"],
            project=row["project"],
            experiment_name=row["experiment_name"],
        )

    def _run_payload(self, run_id: str) -> dict[str, Any]:
        row = self._db.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return {
            "identity": self._identity_of(row).to_dict(),
            "run_key": row["run_key"],
            "state": row["state"],
            "revision": int(row["revision"]),
            # Wire name of the publication epoch (S0 §3.1/§5); the column keeps
            # the design's storage name.
            "lifecycle_epoch": int(row["publication_epoch"]),
            "server_instance_id": row["server_instance_id"],
            "created_at": row["created_at"],
        }

    def _check_capacity(self) -> None:
        if self.active_run_limit is None:
            return
        active = self._db.execute(
            "SELECT COUNT(*) AS total FROM runs WHERE state = ?", (ACTIVE,)
        ).fetchone()["total"]
        if int(active) >= int(self.active_run_limit):
            raise RegistryError(
                "CAPACITY_EXCEEDED",
                f"active-run limit {self.active_run_limit} reached",
                status=429,
                retryable=True,
            )

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[None]:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        self._db.execute("COMMIT")
