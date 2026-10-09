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

"""Single-run bundle export and import (S1 §8).

The prototype bundle is a directory: staging, verify, then one atomic rename,
so a failed export never leaves a visible complete package. Import validates
before it installs anything, uses only self-generated paths, and commits the
Registry last — a half-visible run is not a state this module can produce.

``# ponytail:`` the payload stays uncompressed and directory-shaped; tar/gzip
packaging, streaming and the formal v1 schema belong to S6.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .archive.records import METRICS, STREAMS, TRACES
from .archive.writer import ArchiveStore
from .sdk.identity import RunIdentity
from .server.run_registry import RegistryError

SCHEMA_VERSION = "0.1-prototype"
PAYLOAD_FORMAT = "rlinsight-records-v0"
MANIFEST_NAME = "manifest.json"
RECORD_FILES = {METRICS: "records/metrics.jsonl", TRACES: "records/traces.jsonl"}
TARGETS_FILE = "targets/history.json"

MAX_FILES = 256
MAX_TOTAL_BYTES = 512 * 1024 * 1024
MAX_SINGLE_BYTES = 256 * 1024 * 1024

REQUIRED_FIELDS = (
    "schema_version",
    "payload_format",
    "producer",
    "identity",
    "cutoff_time",
    "streams",
    "files",
    "complete",
    "completed_at",
)

BUNDLE_BROKEN = "BUNDLE_BROKEN"
UNSUPPORTED_FORMAT = "UNSUPPORTED_FORMAT"
MISSING_FIELDS = "MISSING_FIELDS"
PATH_TRAVERSAL = "PATH_TRAVERSAL"
UNSAFE_MEMBER = "UNSAFE_MEMBER"
HASH_MISMATCH = "HASH_MISMATCH"
BUDGET_EXCEEDED = "BUDGET_EXCEEDED"
INCOMPLETE_BUNDLE = "INCOMPLETE_BUNDLE"
RUN_CONFLICT = "RUN_CONFLICT"
MIXED_IDENTITY = "MIXED_IDENTITY"


class BundleError(RuntimeError):
    """A bundle that must not become visible, with its reason code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ExportResult:
    ok: bool
    path: str | None = None
    complete: bool = False
    reasons: tuple[str, ...] = ()
    manifest: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "path": self.path,
            "complete": self.complete,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class ImportResult:
    ok: bool
    run_id: str | None = None
    state: str = ""
    code: str = ""
    message: str = ""
    records: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "run_id": self.run_id,
            "state": self.state,
            "code": self.code,
            "message": self.message,
            "records": self.records,
        }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def export_bundle(
    *,
    store: ArchiveStore,
    identity: RunIdentity,
    registry_run: Mapping[str, Any],
    dest_root: Path | str,
    boundaries: Mapping[str, Any],
    reconciliations: Mapping[str, Any],
    accept_seq: Mapping[str, int],
    allow_partial: bool = False,
    now: Any = time.time,
) -> ExportResult:
    """Freeze one snapshot of one run and publish it atomically.

    ``complete`` is written only when every stream boundary was *proved*; a
    partial export must be requested explicitly and still carries its reasons.
    """
    dest_root = Path(dest_root)
    reasons: list[str] = []
    stream_complete: dict[str, bool] = {}
    for stream in STREAMS:
        outcome = reconciliations.get(stream)
        complete = outcome is not None and bool(outcome.complete)
        if outcome is None:
            reasons.append(f"{stream}:NO_BARRIER")
        elif not outcome.complete:
            reasons.append(f"{stream}:{outcome.reason}")
        # An archive that already knows about conflicts or gaps cannot prove a
        # complete export even when the boundary reconciliation passed (S1 §4.3).
        coverage = store.coverage(identity, stream)
        if coverage["conflict_count"]:
            reasons.append(f"{stream}:RECORD_CONFLICTS")
            complete = False
        if coverage["gaps"]:
            reasons.append(f"{stream}:COVERAGE_GAPS")
            complete = False
        stream_complete[stream] = complete
    if reasons and not allow_partial:
        return ExportResult(ok=False, complete=False, reasons=tuple(reasons))

    staging = dest_root / "staging" / f"bundle-{uuid.uuid4()}"
    staging.mkdir(parents=True, exist_ok=False)
    completed_at = float(now())
    try:
        files: dict[str, dict[str, Any]] = {}
        streams: dict[str, Any] = {}
        for stream in STREAMS:
            boundary = boundaries.get(stream)
            records = store.export_records(identity, stream, accept_seq[stream])
            target = staging / RECORD_FILES[stream]
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("w", encoding="utf-8") as handle:
                for row in records:
                    handle.write(
                        json.dumps({"run_id": identity.id, **row}, sort_keys=True)
                        + "\n"
                    )
                handle.flush()
                os.fsync(handle.fileno())
            files[RECORD_FILES[stream]] = {
                "size": target.stat().st_size,
                "sha256": _sha256(target),
            }
            streams[stream] = {
                "boundary": boundary.to_dict() if boundary is not None else None,
                "coverage": store.coverage(identity, stream),
                "accept_seq": int(accept_seq[stream]),
                "records": len(records),
                "complete": stream_complete[stream],
            }

        targets = staging / TARGETS_FILE
        targets.parent.mkdir(parents=True, exist_ok=True)
        targets.write_text(
            json.dumps(_sanitized_targets(registry_run), sort_keys=True),
            encoding="utf-8",
        )
        files[TARGETS_FILE] = {
            "size": targets.stat().st_size,
            "sha256": _sha256(targets),
        }

        manifest = {
            "schema_version": SCHEMA_VERSION,
            "payload_format": PAYLOAD_FORMAT,
            "producer": {"name": "rl-insight", "component": "bundle-service"},
            "identity": identity.to_dict(),
            "source_instance": registry_run.get("server_instance_id"),
            "snapshot_id": str(uuid.uuid4()),
            "cutoff_time": completed_at,
            "streams": streams,
            "files": files,
            "complete": not reasons,
            "reasons": reasons,
            "completed_at": completed_at,
        }
        manifest_path = staging / MANIFEST_NAME
        manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
        _fsync_dir(staging)

        published = dest_root / "bundles" / str(manifest["snapshot_id"])
        published.parent.mkdir(parents=True, exist_ok=True)
        if published.exists():
            raise BundleError(BUNDLE_BROKEN, "snapshot id already published")
        os.rename(staging, published)
        _fsync_dir(published.parent)
        return ExportResult(
            ok=True,
            path=str(published),
            complete=bool(manifest["complete"]),
            reasons=tuple(reasons),
            manifest=manifest,
        )
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def import_bundle(
    bundle_path: Path | str,
    *,
    store: ArchiveStore,
    registry: Any,
    state: str = "archived",
) -> ImportResult:
    """Validate a bundle, install its files, then make the run visible."""
    bundle_path = Path(bundle_path)
    try:
        manifest = _read_manifest(bundle_path)
    except BundleError as exc:
        return ImportResult(ok=False, code=exc.code, message=exc.message)

    identity = RunIdentity.from_dict(manifest["identity"])
    existing = {row["identity"]["id"] for row in registry.list_runs()}
    if identity.id in existing:
        return ImportResult(
            ok=False,
            code=RUN_CONFLICT,
            message=f"run {identity.id!r} already exists on this instance",
        )

    installed: list[str] = []
    try:
        records = 0
        for stream in STREAMS:
            rows = _read_records(bundle_path, manifest, stream)
            records += store.install_records(identity, stream, rows)
            installed.append(stream)
        registry.install_imported_run(identity, state=state)
    except (BundleError, RegistryError) as exc:
        _cleanup(store, identity)
        return ImportResult(ok=False, code=exc.code, message=exc.message)
    except OSError as exc:  # a half-installed run must not stay visible
        _cleanup(store, identity)
        return ImportResult(ok=False, code=BUNDLE_BROKEN, message=str(exc))
    return ImportResult(ok=True, run_id=identity.id, state=state, records=records)


# --------------------------------------------------------------------------- validation


def _read_manifest(bundle_path: Path) -> dict[str, Any]:
    if not bundle_path.is_dir():
        raise BundleError(BUNDLE_BROKEN, "a bundle must be a directory")
    manifest_path = bundle_path / MANIFEST_NAME
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise BundleError(BUNDLE_BROKEN, "manifest.json is missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    missing = [key for key in REQUIRED_FIELDS if key not in manifest]
    if missing:
        raise BundleError(MISSING_FIELDS, f"manifest is missing {', '.join(missing)}")
    if (
        manifest["schema_version"] != SCHEMA_VERSION
        or manifest["payload_format"] != PAYLOAD_FORMAT
    ):
        raise BundleError(
            UNSUPPORTED_FORMAT,
            f"unsupported schema {manifest['schema_version']!r}/{manifest['payload_format']!r}",
        )
    _check_files(bundle_path, manifest)
    return manifest


def _check_files(bundle_path: Path, manifest: Mapping[str, Any]) -> None:
    files = manifest["files"]
    if len(files) > MAX_FILES:
        raise BundleError(BUDGET_EXCEEDED, "too many files in the bundle")
    total = 0
    for name, entry in files.items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise BundleError(PATH_TRAVERSAL, f"unsafe member path {name!r}")
        path = bundle_path / relative
        if path.is_symlink() or not path.is_file():
            raise BundleError(UNSAFE_MEMBER, f"{name!r} is not a regular file")
        if path.stat().st_nlink > 1:
            raise BundleError(UNSAFE_MEMBER, f"{name!r} is a hard link")
        if path.stat().st_size > MAX_SINGLE_BYTES:
            raise BundleError(BUDGET_EXCEEDED, f"{name!r} is too large")
        total += path.stat().st_size
        if total > MAX_TOTAL_BYTES:
            raise BundleError(BUDGET_EXCEEDED, "bundle exceeds the total size budget")
        if (
            path.stat().st_size != int(entry["size"])
            or _sha256(path) != entry["sha256"]
        ):
            raise BundleError(HASH_MISMATCH, f"{name!r} does not match its checksum")


def _read_records(
    bundle_path: Path, manifest: Mapping[str, Any], stream: str
) -> list[dict[str, Any]]:
    name = RECORD_FILES[stream]
    if name not in manifest["files"]:
        raise BundleError(MISSING_FIELDS, f"{name} is not declared in the manifest")
    rows: list[dict[str, Any]] = []
    with (bundle_path / name).open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("record") is None or row.get("seq") is None:
                raise BundleError(BUNDLE_BROKEN, f"{name} has a malformed record line")
            if row.get("run_id") != manifest["identity"]["id"]:
                raise BundleError(
                    MIXED_IDENTITY, f"{name} carries another run's records"
                )
            rows.append(row)
    if len(rows) != int(manifest["streams"][stream]["records"]):
        raise BundleError(
            MIXED_IDENTITY, f"{name} record count does not match the manifest"
        )
    return rows


def _sanitized_targets(registry_run: Mapping[str, Any]) -> dict[str, Any]:
    """History metadata only: no leases, epochs, incarnations or pids (S1 §8)."""
    return {
        "run_key": registry_run.get("run_key"),
        "endpoints": [
            {
                "target_kind": item.get("target_kind"),
                "component": item.get("component"),
                "instance_key": item.get("instance_key"),
                "address": item.get("canonical_address"),
                "revision": item.get("revision"),
            }
            for item in registry_run.get("endpoints", ())
        ],
    }


def _cleanup(store: ArchiveStore, identity: RunIdentity) -> None:
    store.discard_run(identity)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
