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

"""Unit tests for run-scoped query planning and raw merging (S1 §7, S2 §6.2)."""

from __future__ import annotations

from rl_insight.archive.records import MetricRecord, encode_value
from rl_insight.query.planner import (
    ARCHIVE,
    ARCHIVE_QUERY_UNAVAILABLE,
    CROSS,
    CROSS_BOUNDARY_QUERY_UNSUPPORTED,
    ONLINE,
    OUT_OF_COVERAGE,
    Coverage,
    merge_raw_samples,
    plan_query,
)

RUN = "run-a"


def _coverage(**overrides) -> Coverage:
    payload = {"run_id": RUN, "sealed": [(0, 999)], "online": [(1000, 2000)]}
    payload.update(overrides)
    return Coverage.from_payload(payload)


def _metric(value: float, stamp: int, replica: str = "0") -> MetricRecord:
    return MetricRecord(
        labels={"run_id": RUN, "replica": replica},
        timestamp_ms=stamp,
        value_bits=encode_value(value),
    )


def test_a_window_inside_the_archive_is_served_from_the_archive():
    plan = plan_query((100, 200), _coverage())
    assert plan.ok is True
    assert plan.source == ARCHIVE
    assert plan.sealed_window == (100, 200)
    assert plan.to_dict()["online_window"] is None


def test_a_window_inside_the_online_region_is_served_online():
    plan = plan_query((1200, 1300), _coverage())
    assert (plan.source, plan.online_window) == (ONLINE, (1200, 1300))


def test_a_window_spanning_both_regions_needs_one_raw_evaluation():
    plan = plan_query((900, 1100), _coverage())
    assert plan.source == CROSS
    assert plan.sealed_window == (900, 1100)
    assert plan.online_window == (900, 1100)


def test_the_union_of_one_regions_intervals_covers_a_window():
    coverage = _coverage(sealed=[(0, 400), (400, 900)], online=[])
    assert plan_query((300, 500), coverage).source == ARCHIVE


def test_a_cross_boundary_window_without_online_raw_samples_is_refused():
    plan = plan_query((900, 1100), _coverage(), online_raw_available=False)
    assert plan.ok is False
    assert plan.error_code == CROSS_BOUNDARY_QUERY_UNSUPPORTED


def test_a_cross_boundary_window_without_archived_raw_samples_is_refused():
    """S1 §7.2: half an evaluation is never run against the sealed region."""
    plan = plan_query((900, 1100), _coverage(), sealed_raw_available=False)
    assert plan.ok is False
    assert plan.error_code == ARCHIVE_QUERY_UNAVAILABLE
    assert plan.source == "none"


def test_an_archive_window_without_readable_raw_samples_is_not_served_online():
    coverage = _coverage(sealed=[(0, 2000)], online=[(0, 2000)])
    plan = plan_query((900, 1100), coverage, sealed_raw_available=False)
    assert (plan.ok, plan.error_code) == (False, ARCHIVE_QUERY_UNAVAILABLE)


def test_a_window_outside_every_covered_interval_is_an_explicit_error():
    plan = plan_query((5000, 6000), _coverage())
    assert (plan.ok, plan.error_code) == (False, OUT_OF_COVERAGE)

    reversed_window = plan_query((600, 100), _coverage())
    assert (reversed_window.ok, reversed_window.error_code) == (False, OUT_OF_COVERAGE)


def test_a_window_overlapping_a_coverage_gap_is_refused_not_shown_as_empty():
    plan = plan_query((500, 600), _coverage(gaps=[(400, 700)]))
    assert (plan.ok, plan.error_code) == (False, OUT_OF_COVERAGE)
    assert "gap" in plan.message


def test_merge_deduplicates_identical_samples_across_the_boundary():
    shared = _metric(1.0, 900)
    sealed = [shared, _metric(2.0, 950)]
    online = [shared, _metric(3.0, 1050)]
    merged = merge_raw_samples(sealed, online)
    assert merged.ok is True
    assert [sample.timestamp_ms for sample in merged.samples] == [900, 950, 1050]


def test_a_counter_reset_across_the_boundary_keeps_both_raw_samples():
    # A counter at 5 before the boundary and 1 after a reset: a single
    # evaluation over the merged raw samples sees the reset, whereas two
    # independently computed rates would each be wrong.
    sealed = [_metric(4.0, 990), _metric(5.0, 999)]
    online = [_metric(1.0, 1005), _metric(2.0, 1015)]
    plan = plan_query((990, 1015), _coverage())
    assert plan.source == CROSS

    merged = merge_raw_samples(sealed, online)
    values = [round(sample_value(sample), 3) for sample in merged.samples]
    assert values == [4.0, 5.0, 1.0, 2.0]


def sample_value(record: MetricRecord) -> float:
    from rl_insight.archive.records import decode_value

    return decode_value(record.value_bits)
