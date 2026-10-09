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

"""Run-scoped query routing across archive and online coverage (S1 §7).

The planner never evaluates PromQL: it decides *where* the raw samples for a
window come from and refuses anything it cannot cover. When a window spans both
the sealed archive and the online region the raw samples are merged and handed
to one evaluation, so ``rate``/``increase`` never see two independently
computed halves (S1 §7.2).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

ARCHIVE = "archive"
ONLINE = "online"
CROSS = "cross-boundary"
NONE = "none"

OUT_OF_COVERAGE = "OUT_OF_COVERAGE"
CROSS_BOUNDARY_QUERY_UNSUPPORTED = "CROSS_BOUNDARY_QUERY_UNSUPPORTED"
RAW_SAMPLE_CONFLICT = "RAW_SAMPLE_CONFLICT"
ARCHIVE_QUERY_UNAVAILABLE = "ARCHIVE_QUERY_UNAVAILABLE"


@dataclass(frozen=True)
class Coverage:
    """Accepted intervals of one run/stream, in epoch milliseconds."""

    run_id: str
    sealed: tuple[tuple[int, int], ...] = ()
    online: tuple[tuple[int, int], ...] = ()
    gaps: tuple[tuple[int, int], ...] = ()

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> Coverage:
        def intervals(key: str) -> tuple[tuple[int, int], ...]:
            return tuple((int(a), int(b)) for a, b in payload.get(key, ()))

        return cls(
            run_id=payload["run_id"],
            sealed=intervals("sealed"),
            online=intervals("online"),
            gaps=intervals("gaps"),
        )


@dataclass(frozen=True)
class QueryPlan:
    """Where a bounded raw window must be read from."""

    source: str
    error_code: str = ""
    message: str = ""
    sealed_window: tuple[int, int] | None = None
    online_window: tuple[int, int] | None = None

    @property
    def ok(self) -> bool:
        return not self.error_code

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "error_code": self.error_code,
            "message": self.message,
            "sealed_window": list(self.sealed_window) if self.sealed_window else None,
            "online_window": list(self.online_window) if self.online_window else None,
        }


@dataclass(frozen=True)
class MergeResult:
    """Raw samples from both regions, deduplicated by canonical key."""

    samples: tuple[Any, ...] = ()
    conflicts: tuple[tuple[Any, Any], ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        return not self.conflicts


def _overlaps(window: tuple[int, int], intervals: Iterable[tuple[int, int]]) -> bool:
    start, end = window
    return any(
        start <= other_end and other_start <= end
        for other_start, other_end in intervals
    )


def _covers(window: tuple[int, int], intervals: Iterable[tuple[int, int]]) -> bool:
    """Whether the *union* of ``intervals`` contains the whole window.

    Overlap is not coverage: a window that merely touches an interval leaves
    samples the region cannot answer, and serving it anyway would return a
    quietly truncated result (S2 §6.2). Intervals are inclusive epoch
    milliseconds, so two intervals that meet at consecutive milliseconds form
    one continuous range.
    """
    start, end = window
    reached = start
    for other_start, other_end in sorted(intervals):
        if other_end < reached:
            continue
        if other_start > reached + 1:
            return False
        reached = max(reached, other_end)
        if reached >= end:
            return True
    return False


def plan_query(
    window: tuple[int, int],
    coverage: Coverage,
    *,
    online_raw_available: bool = True,
    sealed_raw_available: bool = True,
) -> QueryPlan:
    """Route one raw window, or refuse it explicitly.

    A region may serve the window only when its own intervals fully contain it;
    a window that neither region covers alone needs one merged raw evaluation
    over both. A window with no coverage is reported as ``OUT_OF_COVERAGE``
    rather than an empty success, and an archive-covered window whose raw
    samples cannot be read is refused instead of being silently answered from
    the online region.
    """
    start, end = (int(window[0]), int(window[1]))
    if start > end:
        return QueryPlan(NONE, OUT_OF_COVERAGE, "query window start is after its end")
    if _overlaps((start, end), coverage.gaps):
        return QueryPlan(
            NONE, OUT_OF_COVERAGE, "the window overlaps a recorded coverage gap"
        )

    in_sealed = _covers((start, end), coverage.sealed)
    in_online = _covers((start, end), coverage.online)

    if in_sealed:
        if not sealed_raw_available:
            return QueryPlan(
                NONE,
                ARCHIVE_QUERY_UNAVAILABLE,
                "the window is fully covered by the archive but its raw samples "
                "cannot be read",
            )
        return QueryPlan(ARCHIVE, sealed_window=(start, end))
    if in_online:
        return QueryPlan(ONLINE, online_window=(start, end))
    # Neither region contains the window: only a single evaluation over the
    # merged raw samples of both regions keeps rate/increase semantics, and it
    # needs the online raw samples to exist.
    if _covers((start, end), coverage.sealed + coverage.online):
        if not sealed_raw_available:
            return QueryPlan(
                NONE,
                ARCHIVE_QUERY_UNAVAILABLE,
                "the window spans the archive and the online region; the archived "
                "raw samples cannot be read",
            )
        if not online_raw_available:
            return QueryPlan(
                NONE,
                CROSS_BOUNDARY_QUERY_UNSUPPORTED,
                "the window spans the archive and the online region; "
                "raw online samples are required to keep one evaluation",
            )
        return QueryPlan(CROSS, sealed_window=(start, end), online_window=(start, end))
    return QueryPlan(NONE, OUT_OF_COVERAGE, "no region fully covers this window")


def merge_raw_samples(sealed: Sequence[Any], online: Sequence[Any]) -> MergeResult:
    """Merge raw samples from both regions: same key and value merge, else conflict."""
    merged: dict[tuple[Any, ...], Any] = {}
    conflicts: list[tuple[Any, Any]] = []
    for record in list(sealed) + list(online):
        existing = merged.get(record.key)
        if existing is None:
            merged[record.key] = record
        elif existing.digest != record.digest:
            conflicts.append((existing, record))
    if conflicts:
        return MergeResult(conflicts=tuple(conflicts))
    # Canonical-key order: per series, in event-time order, as an evaluator needs.
    return MergeResult(samples=tuple(merged[key] for key in sorted(merged)))
