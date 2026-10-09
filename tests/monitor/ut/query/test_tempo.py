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

"""Unit tests for TraceQL spanset enforcement and trace ownership (S2 §6.1/§6.3)."""

from __future__ import annotations

import pytest

from rl_insight.query.errors import QueryError
from rl_insight.query.tempo import (
    IDENTITY_SELECTOR_CONFLICT,
    UNSUPPORTED_TRACEQL,
    enforce_traceql,
    trace_ownership,
)

RUN = "run-a"
ATTRIBUTE = "rl_insight.run_id"
IDENTITY = 'resource.rl_insight.run_id = "run-a"'

DASHBOARD_QUERY = (
    '{span.state_lane_id =~ "experiment=${experiment}/${session}/traj=.*" '
    '&& span.turn >= 0 && resource.service.name="rl_insight_monitor"} '
    "| select(span.turn, span.finish_reason)"
)


def _trace(*run_ids: str) -> dict:
    return {
        "batches": [
            {
                "resource": {
                    "attributes": [{"key": ATTRIBUTE, "value": {"stringValue": run_id}}]
                }
            }
            for run_id in run_ids
        ]
    }


def test_an_empty_query_becomes_the_identity_selector():
    assert enforce_traceql("", ATTRIBUTE, RUN) == "{" + IDENTITY + "}"


def test_every_spanset_of_the_chain_is_constrained():
    assert enforce_traceql('{span.a="1"} && {span.b="2"}', ATTRIBUTE, RUN) == (
        '{span.a="1" && resource.rl_insight.run_id = "run-a"} && '
        '{span.b="2" && resource.rl_insight.run_id = "run-a"}'
    )


def test_the_dashboard_query_keeps_its_conditions_and_projection():
    enforced = enforce_traceql(DASHBOARD_QUERY, ATTRIBUTE, RUN)
    assert enforced.endswith("| select(span.turn, span.finish_reason)")
    assert "span.state_lane_id" in enforced
    assert "resource.service.name" in enforced
    assert IDENTITY in enforced
    assert enforced.index(IDENTITY) < enforced.index("| select")


def test_an_existing_identical_identity_condition_is_kept_once():
    query = f'{{resource.rl_insight.run_id="{RUN}" && span.a="1"}}'
    assert enforce_traceql(query, ATTRIBUTE, RUN) == query


@pytest.mark.parametrize(
    "query",
    [
        '{span.a="1"} // anything\n || {span.b="2"}',
        '{span.a="1" /* } && {span.b="2"} */}',
    ],
)
def test_a_traceql_comment_is_refused_instead_of_scanned_around(query):
    """Tempo skips comments, so a comment could hide a spanset from this module."""
    with pytest.raises(QueryError) as caught:
        enforce_traceql(query, ATTRIBUTE, RUN)
    assert caught.value.code == UNSUPPORTED_TRACEQL


def test_a_comment_marker_inside_a_string_is_not_a_comment():
    query = '{span.a="http://host/x"}'
    assert enforce_traceql(query, ATTRIBUTE, RUN) == (
        f'{{span.a="http://host/x" && {IDENTITY}}}'
    )


@pytest.mark.parametrize(
    ("literal", "misread_id"),
    [
        ('"run-\\101"', "run-101"),
        ('"run-\\x41"', "run-x41"),
    ],
)
def test_an_escape_is_decoded_the_way_traceql_decodes_it(literal, misread_id):
    query = f"{{resource.rl_insight.run_id = {literal}}}"
    with pytest.raises(QueryError) as caught:
        enforce_traceql(query, ATTRIBUTE, misread_id)
    assert caught.value.code == IDENTITY_SELECTOR_CONFLICT
    assert enforce_traceql(query, ATTRIBUTE, "run-A") == query


@pytest.mark.parametrize(
    "condition",
    ['resource.rl_insight.run_id="other"', 'resource.rl_insight.run_id=~"run-.*"'],
)
def test_a_conflicting_identity_condition_is_rejected(condition):
    with pytest.raises(QueryError) as caught:
        enforce_traceql("{" + condition + "}", ATTRIBUTE, RUN)
    assert caught.value.code == IDENTITY_SELECTOR_CONFLICT
    assert caught.value.status == 422


def test_a_trace_of_this_run_is_owned():
    assert trace_ownership(_trace(RUN), ATTRIBUTE, RUN) == {
        "owned": True,
        "mixed": False,
    }


def test_a_trace_of_another_run_is_not_owned():
    assert trace_ownership(_trace("run-b"), ATTRIBUTE, RUN) == {
        "owned": False,
        "mixed": False,
    }


def test_a_trace_mixing_runs_is_a_conflict():
    assert trace_ownership(_trace(RUN, "run-b"), ATTRIBUTE, RUN) == {
        "owned": False,
        "mixed": True,
    }
