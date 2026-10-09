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

"""Metrics completeness barrier: prove the source frontier, then allow export.

S1 §6. A pending gauge, a maximum timestamp or an archive-side counter are all
insufficient: only a source-side boundary enumeration reconciled record by
record against the archive may report ``complete``. Everything else returns an
explicit incomplete reason, and this module never upgrades a partial result.

``# ponytail:`` the source-side enumerator (Prometheus WAL/commit semantics) is
deliberately out of scope here; this module is the contract and the receiving
half, so the real enumerator plugs in and the unit tests still cover the gate.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Callable

COMPLETE = "complete"
MISSING_RECORDS = "MISSING_RECORDS"
DIGEST_MISMATCH = "DIGEST_MISMATCH"
SOURCE_GENERATION_CHANGED = "SOURCE_GENERATION_CHANGED"
SOURCE_HISTORY_LOST = "SOURCE_HISTORY_LOST"
TYPE_UNSUPPORTED = "TYPE_UNSUPPORTED"
INCOMPLETE_BARRIER = "INCOMPLETE_BARRIER"

SUPPORTED_TYPES = ("float", "histogram")


class SourceHistoryLost(RuntimeError):
    """The source can no longer enumerate the boundary: no complete export."""


def record_digest(records: Iterable[Any]) -> str:
    """Digest over per-record normalized content, not over a query result."""
    digest = hashlib.sha256()
    for key, bits in sorted(_canonical(record) for record in records):
        digest.update(repr(key).encode("utf-8"))
        digest.update(b"\x00")
        digest.update(bits.encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _canonical(record: Any) -> tuple[Any, str]:
    return (record.key, record.digest)


@dataclass(frozen=True)
class Boundary:
    """The source-side commit frontier one snapshot is allowed to claim."""

    source_generation: str
    committed_frontier: int
    record_count: int
    record_digest: str
    type_set: tuple[str, ...]
    captured_at: float
    run_id: str = ""
    unsupported_types: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_generation": self.source_generation,
            "committed_frontier": self.committed_frontier,
            "record_count": self.record_count,
            "record_digest": self.record_digest,
            "type_set": list(self.type_set),
            "unsupported_types": list(self.unsupported_types),
            "captured_at": self.captured_at,
            "run_id": self.run_id,
        }


@dataclass(frozen=True)
class Reconciliation:
    """Whether the archive holds exactly the records the source promised."""

    complete: bool
    reason: str = COMPLETE
    missing: int = 0
    extra: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "reason": self.reason,
            "missing": self.missing,
            "extra": self.extra,
        }


def capture_boundary(
    records: Sequence[Any],
    *,
    source_generation: str,
    committed_frontier: int,
    captured_at: float | None = None,
    run_id: str = "",
    supported_types: Sequence[str] = SUPPORTED_TYPES,
) -> Boundary:
    """Build the boundary from the source's own committed record list.

    ``supported_types`` is what the archive can represent losslessly for this
    stream; anything else is recorded on the boundary instead of being dropped.
    """
    types = sorted({record.sample_kind for record in records})
    unsupported = tuple(sorted(set(types) - set(supported_types)))
    return Boundary(
        source_generation=str(source_generation),
        committed_frontier=int(committed_frontier),
        record_count=len(records),
        record_digest=record_digest(records),
        type_set=tuple(types),
        captured_at=time.time() if captured_at is None else captured_at,
        run_id=run_id,
        unsupported_types=unsupported,
    )


def reconcile(
    boundary: Boundary,
    *,
    expected: Sequence[Any],
    received: Sequence[Any],
    source_generation: str | None = None,
) -> Reconciliation:
    """Compare the enumerated source set with the archive, record by record.

    ``expected`` is what the source-side enumerator says belongs to the
    boundary; ``received`` is what the archive persisted. Equal key sets alone
    are not enough — the digests must match too.
    """
    if (
        source_generation is not None
        and str(source_generation) != boundary.source_generation
    ):
        return Reconciliation(False, SOURCE_GENERATION_CHANGED)

    if boundary.unsupported_types:
        return Reconciliation(False, TYPE_UNSUPPORTED)

    expected_keys = {_canonical(record)[0] for record in expected}
    received_keys = {_canonical(record)[0] for record in received}
    if expected_keys != received_keys:
        return Reconciliation(
            False,
            MISSING_RECORDS,
            missing=len(expected_keys - received_keys),
            extra=len(received_keys - expected_keys),
        )
    if len(expected) != boundary.record_count:
        return Reconciliation(
            False, MISSING_RECORDS, missing=boundary.record_count - len(expected)
        )
    if record_digest(expected) != boundary.record_digest:
        return Reconciliation(False, DIGEST_MISMATCH)
    if record_digest(received) != boundary.record_digest:
        return Reconciliation(False, DIGEST_MISMATCH)
    return Reconciliation(True)


def await_durable(
    boundary: Boundary,
    expected: Sequence[Any],
    received_provider: Callable[[], Sequence[Any]],
    *,
    timeout_seconds: float = 5.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    source_generation: str | None = None,
    interval_seconds: float = 0.2,
) -> Reconciliation:
    """Wait, within a bounded budget, for the archive to catch up to the boundary.

    A timeout is a real answer, not a retry loop: it returns
    ``INCOMPLETE_BARRIER`` and the caller must not export a complete bundle.
    """
    deadline = clock() + float(timeout_seconds)
    while True:
        try:
            received = received_provider()
        except SourceHistoryLost:
            return Reconciliation(False, SOURCE_HISTORY_LOST)
        outcome = reconcile(
            boundary,
            expected=expected,
            received=received,
            source_generation=source_generation,
        )
        if outcome.complete or outcome.reason not in (MISSING_RECORDS, DIGEST_MISMATCH):
            return outcome
        if clock() >= deadline:
            return Reconciliation(False, INCOMPLETE_BARRIER, missing=outcome.missing)
        sleep(interval_seconds)
