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

"""Unit tests for the metrics completeness barrier (S1 §6)."""

from __future__ import annotations

import pytest

from rl_insight.archive.barrier import (
    COMPLETE,
    DIGEST_MISMATCH,
    INCOMPLETE_BARRIER,
    MISSING_RECORDS,
    SOURCE_GENERATION_CHANGED,
    SOURCE_HISTORY_LOST,
    TYPE_UNSUPPORTED,
    SourceHistoryLost,
    await_durable,
    capture_boundary,
    reconcile,
    record_digest,
)
from rl_insight.archive.records import MetricRecord, SpanRecord, encode_value


def _metric(value: float, stamp: int, kind: str = "float") -> MetricRecord:
    return MetricRecord(
        labels={"run_id": "run-a"},
        timestamp_ms=stamp,
        value_bits=encode_value(value),
        sample_kind=kind,
    )


def _boundary(records, **overrides):
    kwargs = {
        "source_generation": "gen-1",
        "committed_frontier": 42,
        "captured_at": 1.0,
    }
    kwargs.update(overrides)
    return capture_boundary(records, **kwargs)


def test_a_boundary_only_claims_records_inside_it():
    records = [_metric(1.0, 10), _metric(2.0, 20)]
    boundary = _boundary(records)
    assert boundary.record_count == 2
    assert boundary.record_digest == record_digest(records)
    assert boundary.type_set == ("float",)
    assert boundary.committed_frontier == 42


def test_reconciliation_requires_the_same_records_and_the_same_digest():
    records = [_metric(1.0, 10), _metric(2.0, 20)]
    boundary = _boundary(records)
    assert reconcile(boundary, expected=records, received=records).complete is True

    reordered = [records[1], records[0]]
    assert reconcile(boundary, expected=records, received=reordered).complete is True


def test_a_missing_record_is_incomplete_even_when_timestamps_match():
    records = [_metric(1.0, 10), _metric(2.0, 20)]
    boundary = _boundary(records)
    outcome = reconcile(boundary, expected=records, received=[records[1]])
    assert outcome.complete is False
    assert outcome.reason == MISSING_RECORDS
    assert outcome.missing == 1


def test_a_new_source_generation_invalidates_the_old_boundary():
    records = [_metric(1.0, 10)]
    boundary = _boundary(records)
    outcome = reconcile(
        boundary, expected=records, received=records, source_generation="gen-2"
    )
    assert (outcome.complete, outcome.reason) == (False, SOURCE_GENERATION_CHANGED)


def test_an_unsupported_type_blocks_completeness_instead_of_being_dropped():
    # A source can enumerate a type the archive cannot represent; the boundary
    # must then report TYPE_UNSUPPORTED rather than drop the records silently.
    records = [_SourceRecord("native_histogram")]
    boundary = _boundary(records, supported_types=("float", "histogram"))
    outcome = reconcile(boundary, expected=records, received=records)
    assert (outcome.complete, outcome.reason) == (False, TYPE_UNSUPPORTED)


def test_await_durable_returns_incomplete_on_timeout_and_never_claims_success():
    records = [_metric(1.0, 10), _metric(2.0, 20)]
    boundary = _boundary(records)
    clock = _FakeClock()
    outcome = await_durable(
        boundary,
        records,
        lambda: [records[0]],
        timeout_seconds=0.5,
        clock=clock,
        sleep=lambda seconds: clock.advance(seconds),
        interval_seconds=0.2,
    )
    assert (outcome.complete, outcome.reason) == (False, INCOMPLETE_BARRIER)
    assert clock.now == pytest.approx(0.5, abs=0.3)


def test_await_durable_stops_as_soon_as_the_archive_catches_up():
    records = [_metric(1.0, 10), _metric(2.0, 20)]
    boundary = _boundary(records)
    seen: list[int] = []

    def received():
        seen.append(1)
        return records if len(seen) > 1 else [records[0]]

    clock = _FakeClock()
    outcome = await_durable(
        boundary,
        records,
        received,
        timeout_seconds=5.0,
        clock=clock,
        sleep=lambda seconds: clock.advance(seconds),
        interval_seconds=0.1,
    )
    assert outcome.reason == COMPLETE
    assert len(seen) == 2


def test_a_lost_source_history_is_reported_not_retried():
    records = [_metric(1.0, 10)]
    boundary = _boundary(records)

    def boom():
        raise SourceHistoryLost("wal truncated")

    clock = _FakeClock()
    outcome = await_durable(
        boundary,
        records,
        boom,
        timeout_seconds=5.0,
        clock=clock,
        sleep=lambda seconds: clock.advance(seconds),
    )
    assert (outcome.complete, outcome.reason) == (False, SOURCE_HISTORY_LOST)


def test_span_records_have_no_sample_kind_and_are_reconciled_by_identity():
    span = SpanRecord(trace_id="aa" * 16, span_id="bb" * 8, payload_b64="Zm9v")
    boundary = capture_boundary(
        [span],
        source_generation="gen-1",
        committed_frontier=1,
        captured_at=1.0,
        supported_types=("span",),
    )
    assert boundary.type_set == ("span",)
    assert boundary.unsupported_types == ()
    assert reconcile(boundary, expected=[span], received=[span]).complete is True

    changed = SpanRecord(
        trace_id=span.trace_id, span_id=span.span_id, payload_b64="YmFy"
    )
    assert (
        reconcile(boundary, expected=[changed], received=[changed]).reason
        == DIGEST_MISMATCH
    )


class _SourceRecord:
    """A source-side record whose type the archive does not support."""

    def __init__(self, sample_kind: str) -> None:
        self.sample_kind = sample_kind
        self.key = ("series", 1)
        self.digest = "00"


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds
