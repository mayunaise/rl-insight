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

"""Unit tests for archive segments: lossless values, dedup, seal, recovery."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from rl_insight.archive.records import (
    FLOAT,
    MetricRecord,
    SpanRecord,
    encode_value,
)
from rl_insight.archive.writer import (
    ArchiveError,
    ArchiveStore,
    RecordConflict,
)
from rl_insight.sdk.identity import RunIdentity

DATA = "run-a"


def _identity(run_id: str = DATA) -> RunIdentity:
    return RunIdentity(id=run_id, source="managed", project="proj")


def _metric(value: float, stamp: int, **labels: str) -> MetricRecord:
    return MetricRecord(
        labels={"run_id": DATA, "job": "trainer", **labels},
        timestamp_ms=stamp,
        value_bits=encode_value(value),
    )


# --------------------------------------------------------------------------- values


def test_metric_record_round_trips_through_a_line():
    record = MetricRecord(
        labels={"a": "b"}, timestamp_ms=7, value_bits=encode_value(1.25)
    )
    assert MetricRecord.from_dict(record.to_dict()) == record
    assert record.sample_kind == FLOAT
    with pytest.raises(ValueError):
        MetricRecord(labels={}, timestamp_ms=1, value_bits="00", sample_kind="exemplar")
    with pytest.raises(ValueError):
        MetricRecord(labels={}, timestamp_ms="1", value_bits="00")  # type: ignore[arg-type]


# --------------------------------------------------------------------------- append


def test_append_assigns_monotonic_accept_seq_and_dedups_retries(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    identity = _identity()
    first = store.append(identity, "metrics", [_metric(1.0, 10), _metric(2.0, 20)])
    assert (first.accepted, first.first_seq, first.last_seq) == (2, 1, 2)

    retry = store.append(identity, "metrics", [_metric(1.0, 10), _metric(2.0, 20)])
    assert (retry.accepted, retry.duplicates) == (0, 2)

    more = store.append(identity, "metrics", [_metric(3.0, 30)])
    assert (more.accepted, more.last_seq) == (1, 3)
    assert store.coverage(identity, "metrics")["record_count"] == 3


def test_same_key_with_a_different_value_is_a_conflict_not_an_overwrite(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    identity = _identity()
    store.append(identity, "metrics", [_metric(1.0, 10)])
    conflict = store.append(identity, "metrics", [_metric(9.0, 10)])

    assert conflict.accepted == 0
    assert len(conflict.conflicts) == 1
    assert isinstance(conflict.conflicts[0], RecordConflict)
    # the original sample is untouched and the conflict is durable
    assert [record.value_bits for record in store.read(identity, "metrics")] == [
        encode_value(1.0)
    ]
    assert store.coverage(identity, "metrics")["conflict_count"] == 1
    assert (store.stream_dir(identity, "metrics") / "conflicts.jsonl").exists()


def test_a_key_repeated_inside_one_batch_is_accepted_once(tmp_path: Path):
    """The committed index only moves after the batch is durable, so the batch
    must be checked against itself too (S1 §4.2)."""
    store = ArchiveStore(tmp_path)
    identity = _identity()
    result = store.append(identity, "metrics", [_metric(1.0, 10), _metric(1.0, 10)])

    assert (result.accepted, result.duplicates, result.last_seq) == (1, 1, 1)
    assert [record.timestamp_ms for record in store.read(identity, "metrics")] == [10]


def test_a_conflict_inside_one_batch_is_recorded_not_accepted(tmp_path: Path):
    """A batch that disagrees with itself must not slip past the export gate."""
    store = ArchiveStore(tmp_path)
    identity = _identity()
    result = store.append(identity, "metrics", [_metric(1.0, 10), _metric(9.0, 10)])

    assert result.accepted == 1
    assert [conflict.key for conflict in result.conflicts]
    assert [record.value_bits for record in store.read(identity, "metrics")] == [
        encode_value(1.0)
    ]
    assert store.coverage(identity, "metrics")["conflict_count"] == 1


def test_a_batch_whose_directory_sync_failed_is_not_appended_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The directory sync is inside the rollback, so a failure there reverts too."""
    store = ArchiveStore(tmp_path)
    identity = _identity()
    real_sync = ArchiveStore._sync_dir
    calls = {"count": 0}

    def failing_sync(self: ArchiveStore, path: Path) -> None:
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("simulated directory sync failure")
        real_sync(self, path)

    monkeypatch.setattr(ArchiveStore, "_sync_dir", failing_sync)
    with pytest.raises(OSError):
        store.append(identity, "metrics", [_metric(1.0, 10)])
    monkeypatch.undo()

    segment = store.stream_dir(identity, "metrics") / "segment-000001.jsonl"
    assert segment.read_text() == ""
    assert store.coverage(identity, "metrics")["record_count"] == 0

    retry = store.append(identity, "metrics", [_metric(1.0, 10)])
    assert (retry.accepted, retry.duplicates, retry.first_seq) == (1, 0, 1)
    assert [record.timestamp_ms for record in store.read(identity, "metrics")] == [10]


def test_spans_are_deduplicated_by_trace_and_span_id(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    identity = _identity()
    span = SpanRecord(trace_id="aa" * 16, span_id="bb" * 8, payload_b64="Zm9v")
    assert store.append(identity, "traces", [span]).accepted == 1
    assert store.append(identity, "traces", [span]).duplicates == 1

    changed = SpanRecord(
        trace_id=span.trace_id, span_id=span.span_id, payload_b64="YmFy"
    )
    assert store.append(identity, "traces", [changed]).conflicts


def test_a_retried_span_batch_with_another_run_identity_is_rejected(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    record = SpanRecord(trace_id="cc" * 16, span_id="dd" * 8, payload_b64="Zm9v")
    store.append(_identity("run-a"), "traces", [record])
    # A different run has its own archive: no cross-run dedup, no clash.
    assert store.append(_identity("run-b"), "traces", [record]).accepted == 1
    assert store.stream_dir(_identity("run-b"), "traces") != store.stream_dir(
        _identity("run-a"), "traces"
    )


# --------------------------------------------------------------------------- seal


def test_seal_writes_a_manifest_and_rolls_to_a_new_segment(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    identity = _identity()
    store.append(identity, "metrics", [_metric(1.0, 10)])
    sealed = store.seal(identity, "metrics")
    assert sealed["records"] == 1
    assert (store.stream_dir(identity, "metrics") / "segment-000001.sealed").exists()

    store.append(identity, "metrics", [_metric(2.0, 20)])
    assert (store.stream_dir(identity, "metrics") / "segment-000002.jsonl").exists()
    assert store.coverage(identity, "metrics")["sealed_segments"] == [
        "segment-000001.jsonl"
    ]


def test_snapshot_subset_excludes_records_after_the_boundary(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    identity = _identity()
    store.append(identity, "metrics", [_metric(1.0, 10), _metric(2.0, 20)])
    boundary = store.append(identity, "metrics", [_metric(3.0, 30)]).last_seq - 1
    store.append(identity, "metrics", [_metric(4.0, 40)])  # arrives after the boundary

    snapshot = store.snapshot_subset(identity, "metrics", boundary)
    lines = [
        json.loads(line) for line in Path(snapshot["path"]).read_text().splitlines()
    ]
    assert [line["seq"] for line in lines] == [1, 2]
    assert snapshot["records"] == 2
    assert snapshot["sha256"]


def test_pending_forward_reports_only_unforwarded_records(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    identity = _identity()
    store.append(identity, "traces", [SpanRecord("aa" * 16, "01" * 8, "Zm9v")])
    store.append(identity, "traces", [SpanRecord("aa" * 16, "02" * 8, "YmFy")])
    assert [seq for seq, _ in store.pending_forward(identity, "traces")] == [1, 2]

    store.mark_forwarded(identity, "traces", 1)
    assert [seq for seq, _ in store.pending_forward(identity, "traces")] == [2]
    # the cursor never moves backwards
    store.mark_forwarded(identity, "traces", 1)
    assert (store.stream_dir(identity, "traces") / "forwarded_seq").read_text() == "1"


# --------------------------------------------------------------------------- recovery


def test_a_batch_whose_fsync_failed_is_not_deduplicated_on_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """A record is only deduplicated once its line really is durable (S1 §4.2)."""
    store = ArchiveStore(tmp_path)
    identity = _identity()
    real_fsync = os.fsync
    calls = {"count": 0}

    def failing_fsync(fd: int) -> None:
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("simulated fsync failure")
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", failing_fsync)
    with pytest.raises(OSError):
        store.append(identity, "metrics", [_metric(1.0, 10)])
    monkeypatch.undo()

    # Nothing was ACKed and the half-written batch was rolled back.
    segment = store.stream_dir(identity, "metrics") / "segment-000001.jsonl"
    assert segment.read_text() == ""
    assert store.coverage(identity, "metrics")["record_count"] == 0

    retry = store.append(identity, "metrics", [_metric(1.0, 10)])
    assert (retry.accepted, retry.duplicates, retry.first_seq) == (1, 0, 1)
    assert [record.timestamp_ms for record in store.read(identity, "metrics")] == [10]


def test_a_sealed_segment_missing_from_the_manifest_is_not_reopened(tmp_path: Path):
    """A crash between the seal marker and the manifest must not lose records."""
    store = ArchiveStore(tmp_path)
    identity = _identity()
    store.append(identity, "metrics", [_metric(1.0, 10)])
    directory = store.stream_dir(identity, "metrics")
    # The marker exists, the manifest never got the entry.
    (directory / "segment-000001.sealed").touch()
    (directory / "manifest.json").unlink(missing_ok=True)

    reopened = ArchiveStore(tmp_path)
    coverage = reopened.coverage(identity, "metrics")
    assert coverage["sealed_segments"] == ["segment-000001.jsonl"]
    assert [gap["reason"] for gap in coverage["gaps"]] == [
        "UNREGISTERED_SEGMENT:segment-000001.jsonl"
    ]

    reopened.append(identity, "metrics", [_metric(2.0, 20)])
    assert (directory / "segment-000002.jsonl").exists()
    assert [record.timestamp_ms for record in reopened.read(identity, "metrics")] == [
        10,
        20,
    ]


def test_recovery_discards_a_torn_tail_and_keeps_acked_records(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    identity = _identity()
    store.append(identity, "metrics", [_metric(1.0, 10), _metric(2.0, 20)])
    segment = store.stream_dir(identity, "metrics") / "segment-000001.jsonl"
    with segment.open("a", encoding="utf-8") as handle:
        handle.write('{"seq": 3, "r": {"labe')  # a crash mid-write

    reopened = ArchiveStore(tmp_path)
    coverage = reopened.coverage(identity, "metrics")
    assert coverage["record_count"] == 2
    assert coverage["last_seq"] == 2
    assert len(reopened.read(identity, "metrics")) == 2
    # the truncated tail is removed, so a new append continues at seq 3
    assert reopened.append(identity, "metrics", [_metric(3.0, 30)]).last_seq == 3


def test_a_missing_sealed_segment_shows_up_as_a_gap(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    identity = _identity()
    store.append(identity, "metrics", [_metric(1.0, 10)])
    store.seal(identity, "metrics")
    (store.stream_dir(identity, "metrics") / "segment-000001.jsonl").unlink()

    coverage = ArchiveStore(tmp_path).coverage(identity, "metrics")
    assert coverage["gaps"]
    assert coverage["gaps"][0]["reason"] == "SEGMENT_MISSING"


def test_coverage_tracks_timestamps_and_rejects_unknown_streams(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    identity = _identity()
    store.append(identity, "metrics", [_metric(1.0, 500), _metric(2.0, 100)])
    coverage = store.coverage(identity, "metrics")
    assert (coverage["min_timestamp_ms"], coverage["max_timestamp_ms"]) == (100, 500)
    with pytest.raises(ArchiveError):
        store.coverage(identity, "logs")


def test_archive_directories_keep_raw_ids_out_of_the_path(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    odd = RunIdentity(id="../../etc/passwd", source="managed")
    path = store.stream_dir(odd, "metrics")
    assert "passwd" not in str(path)
    assert "etc" not in path.parts
    store.append(odd, "metrics", [_metric(1.0, 1)])
    assert path.exists()


def test_discard_run_removes_an_aborted_install(tmp_path: Path):
    store = ArchiveStore(tmp_path)
    identity = _identity()
    store.append(identity, "metrics", [_metric(1.0, 10)])
    store.discard_run(identity)
    assert not store.run_dir(identity).exists()
    assert store.coverage(identity, "metrics")["record_count"] == 0
