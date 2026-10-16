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

"""Run catalog with recoverable, readable per-Run file projections."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..protocol import RunContext, RunError, endpoint_description, validate_id


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _config_snapshot(config: Mapping[str, Any]) -> dict[str, Any]:
    """Allowlist observability fields; never persist arbitrary framework config."""
    if not isinstance(config, Mapping):
        raise RunError("Observability config must be an object")
    result: dict[str, Any] = {}
    for section, fields in {
        "server": ("namespace", "backend"),
        "prometheus": ("metrics_report_port",),
    }.items():
        raw = config.get(section, {})
        if not isinstance(raw, Mapping):
            raise RunError("Invalid observability config section")
        selected = {key: raw[key] for key in fields if key in raw}
        if any(not isinstance(value, (str, int, bool)) for value in selected.values()):
            raise RunError("Invalid observability config value")
        if selected:
            result[section] = selected
    return result


class EndpointRegistry:
    """In-memory endpoint leases, optionally scoped by a managed Run registry."""

    def __init__(self, lease_ttl: float = 30):
        if not 1 <= lease_ttl <= 3600:
            raise RunError("Endpoint TTL must be between 1 and 3600 seconds")
        self._lease_ttl = lease_ttl
        self._lock = threading.RLock()
        self._endpoints: dict[str, dict[str, Any]] = {}

    def get_context(self, run_id=None):
        if run_id is not None:
            raise RunError("This collector does not manage Runs", 404)

    def assert_open(self, run_id: str | None, *, allow_closing=True):
        self.get_context(run_id)

    def endpoint(self, lease_id):
        with self._lock:
            self.endpoints()
            item = self._endpoints.get(validate_id(lease_id))
            if item is None:
                raise RunError("Endpoint lease expired or replaced", 409)
            return dict(item)

    def retire_endpoint(self, lease_id, snapshot):
        """Keep a bounded final snapshot available for one TTL after graceful close."""
        with self._lock:
            item = self._endpoints[self.endpoint(lease_id)["lease_id"]]
            if "snapshot" not in item:
                item.update(
                    snapshot=snapshot, expires_at=time.monotonic() + self._lease_ttl
                )

    def close(self):
        with self._lock:
            self._endpoints.clear()

    def register_endpoint(self, description: dict[str, Any]) -> dict[str, Any]:
        run_id = description.get("run_id")
        with self._lock:
            self.assert_open(run_id, allow_closing=False)
            active = self.endpoints()
            for item in active:
                if "snapshot" in item:
                    if item["metrics_url"] == description["metrics_url"]:
                        self._endpoints.pop(item["lease_id"], None)
                    continue
                slot = ("run_id", "component", "instance")
                if all(item.get(k) == description.get(k) for k in slot):
                    if any(
                        item[k] != description[k]
                        for k in ("incarnation", "metrics_url")
                    ):
                        raise RunError(
                            "Endpoint slot already belongs to an active publisher", 409
                        )
                    self.annotate_endpoint(item["lease_id"], description["labels"])
                    return self.renew_endpoint(run_id, item["lease_id"])
                if item["metrics_url"] == description["metrics_url"]:
                    raise RunError(
                        "Endpoint address already belongs to an active publisher", 409
                    )
            if sum(item.get("run_id") == run_id for item in self.endpoints()) >= 64:
                raise RunError("Run endpoint capacity exceeded", 429)
            item = {
                **description,
                "lease_id": uuid4().hex,
                "endpoint_id": hashlib.sha256(
                    _json(
                        {
                            key: description.get(key)
                            for key in (
                                "run_id",
                                "component",
                                "instance",
                                "incarnation",
                            )
                        }
                    ).encode()
                ).hexdigest()[:32],
                "expires_at": time.monotonic() + self._lease_ttl,
                "ttl": self._lease_ttl,
            }
            self._endpoints[item["lease_id"]] = item
            return dict(item)

    def annotate_endpoint(self, lease_id: str, labels: Mapping[str, Any]) -> None:
        """Add descriptive labels without changing ownership or extending the lease."""
        with self._lock:
            self.endpoints()
            item = self._endpoints.get(lease_id)
            if item is None:
                raise RunError("Endpoint lease expired or replaced", 409)
            merged = dict(item["labels"])
            for key, value in labels.items():
                if key in merged and merged[key] != str(value):
                    raise RunError("Endpoint description labels conflict", 409)
                merged[key] = value
            description = endpoint_description(
                self.get_context(item.get("run_id")), {**item, "labels": merged}
            )
            item["labels"] = description["labels"]

    def endpoints(self, run_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            now = time.monotonic()
            self._endpoints = {
                key: val
                for key, val in self._endpoints.items()
                if val["expires_at"] > now
            }
            return [
                dict(item)
                for item in self._endpoints.values()
                if run_id is None or item.get("run_id") == run_id
            ]

    def endpoint_valid(self, item: dict[str, Any]) -> bool:
        with self._lock:
            current = self._endpoints.get(item["lease_id"])
            return current is not None and current["expires_at"] > time.monotonic()

    def renew_endpoint(self, run_id: str | None, lease_id: str) -> dict[str, Any]:
        with self._lock:
            self.assert_open(run_id)
            self.endpoints()
            item = self._endpoints.get(validate_id(lease_id))
            if item is None or item.get("run_id") != run_id or "snapshot" in item:
                raise RunError("Endpoint lease expired or replaced", 409)
            item["expires_at"] = time.monotonic() + item["ttl"]
            return dict(item)

    def revoke_endpoint(self, run_id: str | None, lease_id: str) -> None:
        with self._lock:
            item = self._endpoints.get(validate_id(lease_id))
            if item is not None and item.get("run_id") != run_id:
                raise RunError("Endpoint lease belongs to another Run", 409)
            self._endpoints.pop(lease_id, None)


class RunRegistry(EndpointRegistry):
    def __init__(self, root: Path, lease_ttl: float = 30):
        super().__init__(lease_ttl)
        self.root = root.expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(
            self.root / "runs.sqlite", timeout=10, check_same_thread=False
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("""CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL,
            context TEXT NOT NULL, config TEXT NOT NULL, created_at REAL NOT NULL
        )""")
        columns = {row[1] for row in self._db.execute("PRAGMA table_info(runs)")}
        for name, definition in {
            "status": "TEXT NOT NULL DEFAULT 'registered'",
            "ended_at": "REAL",
            "owner_hash": "TEXT NOT NULL DEFAULT ''",
            "final_confirmed": "INTEGER",
        }.items():
            if name not in columns:
                self._db.execute(f"ALTER TABLE runs ADD COLUMN {name} {definition}")
        self._db.commit()

    def create(
        self,
        request_id: str,
        project: str,
        experiment_name: str,
        server_url: str,
        config: Mapping[str, Any],
        owner_token: str = "",
    ) -> RunContext:
        validate_id(request_id)
        if owner_token:
            validate_id(owner_token)
        context = RunContext(uuid4().hex, project, experiment_name, server_url)
        snapshot = _config_snapshot(config)
        with self._lock, self._db:
            self._db.execute("BEGIN IMMEDIATE")
            row = self._db.execute(
                "SELECT * FROM runs WHERE request_id=?", (request_id,)
            ).fetchone()
            if row:
                expected_owner = (
                    hashlib.sha256(owner_token.encode()).hexdigest()
                    if owner_token
                    else ""
                )
                if row["owner_hash"] != expected_owner:
                    raise RunError(
                        "Run creation owner conflicts with its original request", 409
                    )
                existing = RunContext.from_dict(json.loads(row["context"]))
                if (
                    existing.project,
                    existing.experiment_name,
                    json.loads(row["config"]),
                ) != (project, experiment_name, snapshot):
                    raise RunError(
                        "Run creation request was reused with different metadata", 409
                    )
                context = existing
            else:
                self._db.execute(
                    "INSERT INTO runs (run_id,request_id,context,config,created_at,owner_hash) VALUES (?,?,?,?,?,?)",
                    (
                        context.run_id,
                        request_id,
                        _json(context.to_dict()),
                        _json(snapshot),
                        time.time(),
                        hashlib.sha256(owner_token.encode()).hexdigest()
                        if owner_token
                        else "",
                    ),
                )
        # The DB owns identity. A failed projection can be rebuilt on get/retry.
        self.snapshot(context.run_id)
        return context

    def get_context(self, run_id):
        return self.get(run_id)

    def get(self, run_id: str) -> RunContext:
        return RunContext.from_dict(json.loads(self._read_run(run_id)["context"]))

    def list_runs(self) -> list[RunContext]:
        with self._lock:
            rows = self._db.execute(
                "SELECT context FROM runs ORDER BY created_at DESC, run_id DESC"
            ).fetchall()
        return [RunContext.from_dict(json.loads(row["context"])) for row in rows]

    def _read_run(self, run_id: str) -> sqlite3.Row:
        validate_id(run_id)
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                raise RunError("Run not found", 404)
            return row

    def snapshot(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._read_run(run_id)
            context = RunContext.from_dict(json.loads(row["context"]))
            directory = self.root / "runs" / run_id
            metadata = {
                "schema_version": 1,
                "context": context.to_dict(),
                "created_at": row["created_at"],
                "status": row["status"],
                "ended_at": row["ended_at"],
                "final_collection_confirmed": None
                if row["final_confirmed"] is None
                else bool(row["final_confirmed"]),
            }
            self._write(directory / "run.json", metadata)
            self._write(directory / "config.json", json.loads(row["config"]))
            return metadata

    def assert_open(self, run_id: str | None, *, allow_closing: bool = True) -> None:
        if run_id is None:
            raise RunError("Run identity is required")
        status = self._read_run(run_id)["status"]
        if status == "ended" or (not allow_closing and status == "closing"):
            raise RunError("Run is closed", 409)

    def ended_runs(self) -> set[str]:
        with self._lock:
            return {
                row[0]
                for row in self._db.execute(
                    "SELECT run_id FROM runs WHERE status='ended'"
                )
            }

    def transition(
        self, run_id: str, owner_token: str, status: str, final_confirmed=None
    ) -> None:
        validate_id(owner_token)
        with self._lock, self._db:
            row = self._read_run(run_id)
            if (
                not row["owner_hash"]
                or row["owner_hash"] != hashlib.sha256(owner_token.encode()).hexdigest()
            ):
                raise RunError("Only the Run creator can close it", 409)
            if row["status"] == "ended":
                return
            self._db.execute(
                "UPDATE runs SET status=?,ended_at=? WHERE run_id=?",
                (status, time.time() if status == "ended" else None, run_id),
            )
            if status == "ended":
                self._db.execute(
                    "UPDATE runs SET final_confirmed=? WHERE run_id=?",
                    (
                        int(final_confirmed)
                        if isinstance(final_confirmed, bool)
                        else None,
                        run_id,
                    ),
                )
                self._endpoints = {
                    key: value
                    for key, value in self._endpoints.items()
                    if value["run_id"] != run_id
                }
        self.snapshot(run_id)

    @staticmethod
    def _write(path: Path, value: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name("." + path.name + "." + uuid4().hex)
        try:
            temporary.write_text(_json(value) + "\n", encoding="utf-8")
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    def close(self) -> None:
        with self._lock:
            self._db.close()
