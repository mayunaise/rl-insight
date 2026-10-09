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

"""Per-run archive segments: durable append, dedup, seal, recovery (S1 §4).

One file store, one commit point: a record is accepted only after its segment
line is written and fsynced, so nothing is ever ACKed before it is durable.
The dedup index is rebuilt by replaying segments, which removes the
file-plus-SQLite split-commit problem entirely (S1 §4.2).

``# ponytail:`` the index is rebuilt by full replay at open; when archives grow
past the prototype, persist the index and keep this as the repair path.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..sdk.identity import RunIdentity
from .records import METRICS, STREAMS, decode_line, encode_line, record_from_dict

logger = logging.getLogger(__name__)

SEGMENT_MAX_RECORDS = 2000
MANIFEST_NAME = "manifest.json"
CONFLICTS_NAME = "conflicts.jsonl"
FORWARD_CURSOR_NAME = "forwarded_seq"
_SEGMENT_NAME = re.compile(r"^segment-(\d+)\.jsonl$")


class ArchiveError(RuntimeError):
    """Raised when an archive operation cannot proceed."""


@dataclass(frozen=True)
class RecordConflict:
    """Same canonical key, different content: blocks the complete export gate."""

    stream: str
    key: tuple[Any, ...]
    existing_digest: str
    incoming_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "stream": self.stream,
            "key": list(self.key),
            "existing_digest": self.existing_digest,
            "incoming_digest": self.incoming_digest,
        }


@dataclass(frozen=True)
class AppendResult:
    """Outcome of one append transaction; the batch is all-or-nothing."""

    stream: str
    first_seq: int
    last_seq: int
    accepted: int
    duplicates: int
    conflicts: tuple[RecordConflict, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "stream": self.stream,
            "first_seq": self.first_seq,
            "last_seq": self.last_seq,
            "accepted": self.accepted,
            "duplicates": self.duplicates,
            "conflicts": [item.to_dict() for item in self.conflicts],
        }


@dataclass
class _StreamState:
    directory: Path
    next_seq: int = 1
    index: dict[tuple[Any, ...], str] = field(default_factory=dict)
    open_segment: Path | None = None
    open_records: int = 0
    sealed: list[dict[str, Any]] = field(default_factory=list)
    conflicts: list[RecordConflict] = field(default_factory=list)
    min_timestamp_ms: int | None = None
    max_timestamp_ms: int | None = None
    gaps: list[dict[str, Any]] = field(default_factory=list)
    forwarded_seq: int = 0


class ArchiveStore:
    """Append-only, per-run observation archive on the local filesystem."""

    def __init__(self, data_dir: Path | str, *, now: Any = time.time) -> None:
        self.data_dir = Path(data_dir)
        self._now = now
        self._states: dict[tuple[str, str], _StreamState] = {}

    # -- layout --------------------------------------------------------
    def run_dir(self, identity: RunIdentity) -> Path:
        return (
            self.data_dir
            / "projects"
            / identity.project_key
            / "runs"
            / identity.run_key
        )

    def stream_dir(self, identity: RunIdentity, stream: str) -> Path:
        return self.run_dir(identity) / "archive" / self._check_stream(stream)

    @staticmethod
    def _check_stream(stream: str) -> str:
        if stream not in STREAMS:
            raise ArchiveError(f"unknown archive stream {stream!r}")
        return stream

    # -- write path ----------------------------------------------------
    def append(
        self, identity: RunIdentity, stream: str, records: Sequence[Any]
    ) -> AppendResult:
        """Accept a batch durably, then return; conflicts never overwrite data."""
        state = self._state(identity, stream)
        accepted_lines: list[str] = []
        accepted_keys: list[tuple[tuple[Any, ...], str]] = []
        conflicts: list[RecordConflict] = []
        duplicates = 0
        first_seq = state.next_seq
        seq = state.next_seq
        # The committed index only moves after the bytes are durable, so one
        # batch must also be checked against what it already accepted itself:
        # otherwise a key repeated inside the batch is appended twice, and a
        # conflicting pair inside it is accepted with no conflict recorded.
        pending: dict[tuple[Any, ...], str] = {}

        for record in records:
            key = record.key
            digest = record.digest
            known = pending[key] if key in pending else state.index.get(key)
            if known is None:
                accepted_lines.append(encode_line(seq, record))
                accepted_keys.append((key, digest))
                pending[key] = digest
                seq += 1
            elif known == digest:
                duplicates += 1
            else:
                conflicts.append(RecordConflict(stream, key, known, digest))

        if accepted_lines:
            target = self._open_segment(state)
            _durable_append(target, accepted_lines, self._sync_dir)
            # Only committed once the bytes are durable: a failed write leaves
            # the batch unknown, so the retry appends it instead of being
            # deduplicated against a line that never reached the disk (S1 §4.2).
            for key, digest in accepted_keys:
                state.index[key] = digest
            state.open_records += len(accepted_lines)
            self._track_timestamps(state, stream, records)
            state.next_seq = seq
            if state.open_records >= SEGMENT_MAX_RECORDS:
                self.seal(identity, stream)

        if conflicts:
            self._persist_conflicts(state, conflicts)
            state.conflicts.extend(conflicts)

        return AppendResult(
            stream=stream,
            first_seq=first_seq,
            last_seq=seq - 1,
            accepted=len(accepted_lines),
            duplicates=duplicates,
            conflicts=tuple(conflicts),
        )

    def seal(self, identity: RunIdentity, stream: str) -> dict[str, Any]:
        """Close the open segment: fsync it and its directory, then register it."""
        state = self._state(identity, stream)
        segment = state.open_segment
        if segment is None:
            return {"sealed": None, "records": 0}
        with segment.open("rb") as handle:
            os.fsync(handle.fileno())
        marker = segment.with_suffix(".sealed")
        marker.write_text("", encoding="utf-8")
        self._sync_dir(state.directory)
        entry = {
            "name": segment.name,
            "records": state.open_records,
            "last_seq": state.next_seq - 1,
            "sha256": _file_sha256(segment),
        }
        state.sealed.append(entry)
        state.open_segment = None
        state.open_records = 0
        self._write_manifest(state)
        return {"sealed": segment.name, **entry}

    def mark_forwarded(self, identity: RunIdentity, stream: str, seq: int) -> int:
        """Advance the forwarding cursor; the cursor never moves backwards."""
        state = self._state(identity, stream)
        if seq > state.forwarded_seq:
            state.forwarded_seq = int(seq)
            _atomic_write_text(
                state.directory / FORWARD_CURSOR_NAME, str(state.forwarded_seq)
            )
        return state.forwarded_seq

    # -- read path -----------------------------------------------------
    def read(
        self,
        identity: RunIdentity,
        stream: str,
        *,
        upto_seq: int | None = None,
        since_seq: int = 0,
    ) -> list[Any]:
        return [
            record
            for seq, record in self.read_with_seq(identity, stream, upto_seq=upto_seq)
            if seq > since_seq
        ]

    def pending_forward(
        self, identity: RunIdentity, stream: str
    ) -> list[tuple[int, Any]]:
        """Records accepted but not yet handed to the online backend."""
        state = self._state(identity, stream)
        pending: list[tuple[int, Any]] = []
        for path in sorted(state.directory.glob("segment-*.jsonl")):
            for seq, record in self._read_segment(
                stream, path, strict=path.with_suffix(".sealed").exists()
            ):
                if seq > state.forwarded_seq:
                    pending.append((seq, record))
        return pending

    def coverage(self, identity: RunIdentity, stream: str) -> dict[str, Any]:
        """What this archive can prove about the run's accepted records."""
        state = self._state(identity, stream)
        return {
            "run_id": identity.id,
            "stream": stream,
            "first_seq": 1,
            "last_seq": state.next_seq - 1,
            "record_count": len(state.index),
            "sealed_segments": [entry["name"] for entry in state.sealed],
            "conflict_count": len(state.conflicts),
            "gaps": list(state.gaps),
            "min_timestamp_ms": state.min_timestamp_ms,
            "max_timestamp_ms": state.max_timestamp_ms,
        }

    def snapshot_subset(
        self,
        identity: RunIdentity,
        stream: str,
        upto_seq: int,
        *,
        snapshot_id: str | None = None,
    ) -> dict[str, Any]:
        """Rewrite exactly the records inside an accept boundary into a snapshot file.

        Records that share the physical segment but arrived after the boundary
        are excluded, which is what makes a snapshot reproducible (S1 §5).
        """
        state = self._state(identity, stream)
        snapshot_id = snapshot_id or str(uuid.uuid4())
        directory = state.directory / "snapshots"
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f"{snapshot_id}.jsonl"
        written = 0
        with target.open("w", encoding="utf-8") as handle:
            for path in sorted(state.directory.glob("segment-*.jsonl")):
                for seq, record in self._read_segment(
                    stream, path, strict=path.with_suffix(".sealed").exists()
                ):
                    if seq <= upto_seq:
                        handle.write(encode_line(seq, record))
                        written += 1
            handle.flush()
            os.fsync(handle.fileno())
        self._sync_dir(directory)
        return {
            "snapshot_id": snapshot_id,
            "path": str(target),
            "records": written,
            "upto_seq": upto_seq,
            "sha256": _file_sha256(target),
        }

    def export_records(
        self, identity: RunIdentity, stream: str, upto_seq: int
    ) -> list[dict[str, Any]]:
        """Boundary-subset records in a transport shape (no store paths leak out)."""
        out: list[dict[str, Any]] = []
        for seq, record in self.read_with_seq(identity, stream, upto_seq=upto_seq):
            out.append({"seq": seq, "record": record.to_dict()})
        return out

    def read_with_seq(
        self, identity: RunIdentity, stream: str, *, upto_seq: int | None = None
    ) -> list[tuple[int, Any]]:
        state = self._state(identity, stream)
        out: list[tuple[int, Any]] = []
        for path in sorted(state.directory.glob("segment-*.jsonl")):
            for seq, record in self._read_segment(
                stream, path, strict=path.with_suffix(".sealed").exists()
            ):
                if upto_seq is None or seq <= upto_seq:
                    out.append((seq, record))
        return out

    def discard_run(self, identity: RunIdentity) -> None:
        """Remove an aborted install; only used by bundle import rollback."""
        for key in [key for key in self._states if key[0] == identity.run_key]:
            self._states.pop(key, None)
        shutil.rmtree(self.run_dir(identity), ignore_errors=True)

    def install_records(
        self, identity: RunIdentity, stream: str, rows: Iterable[Mapping[str, Any]]
    ) -> int:
        """Install records that arrived in a bundle, keeping their original seq."""
        state = self._state(identity, stream)
        if state.open_segment is not None:
            self.seal(identity, stream)
        lines: list[str] = []
        highest = state.next_seq - 1
        for row in rows:
            seq = int(row["seq"])
            record = record_from_dict(stream, row["record"])
            state.index[record.key] = record.digest
            lines.append(encode_line(seq, record))
            highest = max(highest, seq)
        if not lines:
            return 0
        target = (
            state.directory
            / f"segment-{_next_segment_index(state.directory):06d}.jsonl"
        )
        with target.open("x", encoding="utf-8") as handle:
            handle.writelines(lines)
            handle.flush()
            os.fsync(handle.fileno())
        self._sync_dir(state.directory)
        state.open_segment = target
        state.open_records = len(lines)
        state.next_seq = highest + 1
        self.seal(identity, stream)
        return len(lines)

    # -- internals -----------------------------------------------------
    def _state(self, identity: RunIdentity, stream: str) -> _StreamState:
        key = (identity.run_key, self._check_stream(stream))
        if key not in self._states:
            self._states[key] = self._recover(identity, stream)
        return self._states[key]

    def _recover(self, identity: RunIdentity, stream: str) -> _StreamState:
        directory = self.stream_dir(identity, stream)
        directory.mkdir(parents=True, exist_ok=True)
        state = _StreamState(directory=directory)
        manifest = directory / MANIFEST_NAME
        if manifest.exists():
            state.sealed = list(
                json.loads(manifest.read_text(encoding="utf-8")).get("sealed", [])
            )

        for path in sorted(directory.glob("segment-*.jsonl")):
            sealed = path.with_suffix(".sealed").exists()
            rows = self._read_segment(stream, path, strict=sealed)
            if not sealed:
                # An unsealed tail may be a torn write: keep the valid prefix only.
                torn_bytes = path.stat().st_size
                self._truncate_tail(path, rows)
                if path.stat().st_size != torn_bytes:
                    # This repair also discards the bytes it could not decode, so
                    # it must not be invisible: the window is kept as a gap.
                    logger.warning(
                        "[rl-insight] dropped a torn tail from %s/%s (%d -> %d "
                        "bytes); the decoded prefix was kept",
                        stream,
                        path.name,
                        torn_bytes,
                        path.stat().st_size,
                    )
            for seq, record in rows:
                state.index[record.key] = record.digest
                state.next_seq = max(state.next_seq, seq + 1)
                self._track_one(state, stream, record)
            if not sealed:
                state.open_segment = path
                state.open_records = len(rows)

        conflicts = directory / CONFLICTS_NAME
        if conflicts.exists():
            for line in conflicts.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    state.conflicts.append(RecordConflict(**json.loads(line)))
        cursor = directory / FORWARD_CURSOR_NAME
        if cursor.exists():
            state.forwarded_seq = int(cursor.read_text(encoding="utf-8").strip() or 0)

        sealed_names = {entry["name"] for entry in state.sealed}
        for entry in state.sealed:
            if not (directory / entry["name"]).exists():
                state.gaps.append(
                    {
                        "from_seq": None,
                        "to_seq": entry.get("last_seq"),
                        "reason": "SEGMENT_MISSING",
                    }
                )
        for path in directory.glob("segment-*.jsonl"):
            if path.with_suffix(".sealed").exists() and path.name not in sealed_names:
                rows = self._read_segment(stream, path, strict=True)
                entry = {
                    "name": path.name,
                    "records": len(rows),
                    "last_seq": rows[-1][0] if rows else 0,
                    "sha256": _file_sha256(path),
                }
                # A crash between the seal marker and the manifest leaves a
                # sealed segment the manifest does not know: register it so it
                # is never reopened for appending, and keep the gap so an
                # export of this window is not claimed complete.
                state.sealed.append(entry)
                state.gaps.append(
                    {
                        "from_seq": None,
                        "to_seq": None,
                        "reason": f"UNREGISTERED_SEGMENT:{path.name}",
                    }
                )
        return state

    def _read_segment(
        self, stream: str, path: Path, *, strict: bool
    ) -> list[tuple[int, Any]]:
        rows: list[tuple[int, Any]] = []
        if not path.exists():
            return rows
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    rows.append(decode_line(stream, line))
                except (ValueError, KeyError):
                    if strict:
                        raise ArchiveError(
                            f"sealed segment {path.name} is corrupt"
                        ) from None
                    break
        return rows

    def _truncate_tail(self, path: Path, rows: list[tuple[int, Any]]) -> None:
        with path.open("r+", encoding="utf-8") as handle:
            payload = "".join(encode_line(seq, record) for seq, record in rows)
            handle.seek(0)
            handle.write(payload)
            handle.truncate()
            handle.flush()
            os.fsync(handle.fileno())

    def _open_segment(self, state: _StreamState) -> Path:
        if state.open_segment is None:
            name = f"segment-{_next_segment_index(state.directory):06d}.jsonl"
            state.open_segment = state.directory / name
            state.open_segment.touch()
            state.open_records = 0
        return state.open_segment

    def _write_manifest(self, state: _StreamState) -> None:
        _atomic_write_text(
            state.directory / MANIFEST_NAME,
            json.dumps({"sealed": state.sealed}, sort_keys=True),
        )

    def _persist_conflicts(
        self, state: _StreamState, conflicts: Sequence[RecordConflict]
    ) -> None:
        with (state.directory / CONFLICTS_NAME).open("a", encoding="utf-8") as handle:
            for conflict in conflicts:
                handle.write(json.dumps(conflict.to_dict(), sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def _track_timestamps(
        self, state: _StreamState, stream: str, records: Sequence[Any]
    ) -> None:
        for record in records:
            self._track_one(state, stream, record)

    def _track_one(self, state: _StreamState, stream: str, record: Any) -> None:
        if stream != METRICS:
            return
        stamp = int(record.timestamp_ms)
        state.min_timestamp_ms = (
            stamp
            if state.min_timestamp_ms is None
            else min(state.min_timestamp_ms, stamp)
        )
        state.max_timestamp_ms = (
            stamp
            if state.max_timestamp_ms is None
            else max(state.max_timestamp_ms, stamp)
        )

    def _sync_dir(self, path: Path) -> None:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _durable_append(
    target: Path, lines: Sequence[str], sync_dir: Callable[[Path], None]
) -> None:
    """Append a batch durably; a failed append leaves no partial batch behind."""
    offset = target.stat().st_size
    try:
        with target.open("a", encoding="utf-8") as handle:
            handle.writelines(lines)
            handle.flush()
            os.fsync(handle.fileno())
        # The directory sync is part of the same guarantee: it happens inside
        # the rollback so a failure there reverts the batch instead of leaving
        # bytes the (uncommitted) index would let a retry append twice.
        sync_dir(target.parent)
    except BaseException:
        # Roll the segment back to its last committed length so the retry
        # appends a whole batch instead of continuing after a torn line.
        with target.open("r+", encoding="utf-8") as handle:
            handle.truncate(offset)
            handle.flush()
            os.fsync(handle.fileno())
        raise


def _next_segment_index(directory: Path) -> int:
    """One past the highest segment number on disk: names are never reused."""
    highest = 0
    for path in directory.glob("segment-*.jsonl"):
        match = _SEGMENT_NAME.match(path.name)
        if match:
            highest = max(highest, int(match.group(1)))
    return highest + 1


def _atomic_write_text(path: Path, payload: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = ["AppendResult", "ArchiveError", "ArchiveStore", "RecordConflict"]
