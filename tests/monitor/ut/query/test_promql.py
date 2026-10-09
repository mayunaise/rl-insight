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

"""Unit tests for run-scoped PromQL selector enforcement (S2 §6.1/§6.2)."""

from __future__ import annotations

import pytest

from rl_insight.query.errors import QueryError
from rl_insight.query.promql import (
    IDENTITY_SELECTOR_CONFLICT,
    UNSUPPORTED_EXPRESSION,
    enforce_match_selector,
    enforce_selector,
    identity_selector,
)

RUN = "run-a"
LABEL = "run_id"


def test_every_selector_of_a_binary_expression_is_constrained():
    enforced = enforce_selector("sum(rate(a[1m])) / sum(rate(b[1m]))", RUN, LABEL)
    assert enforced == (
        'sum(rate(a{run_id="run-a"}[1m])) / sum(rate(b{run_id="run-a"}[1m]))'
    )


def test_a_bare_metric_name_gains_the_identity_selector():
    assert (
        enforce_selector("rl_insight_monitor_train_step_total", RUN, LABEL)
        == 'rl_insight_monitor_train_step_total{run_id="run-a"}'
    )


def test_an_existing_identical_identity_matcher_is_kept_once():
    selector = f'a{{{LABEL}="{RUN}"}}'
    assert enforce_selector(selector, RUN, LABEL) == selector


@pytest.mark.parametrize("matcher", ['run_id=~"run-.*"'])
def test_a_conflicting_identity_matcher_is_rejected(matcher):
    with pytest.raises(QueryError) as caught:
        enforce_selector(f"a{{{matcher}}}", RUN, LABEL)
    assert caught.value.code == IDENTITY_SELECTOR_CONFLICT
    assert caught.value.status == 422


def test_a_selector_only_expression_and_durations_keep_their_semantics():
    # ``le``/``replica`` are a label list, the durations and the Grafana
    # variables are not metric names, and the recording-rule name keeps its
    # colon.
    query = (
        "histogram_quantile(0.99, sum by (le, replica) "
        '(rate({__name__=~"sglang[:_]e2e_request_latency_seconds_bucket", '
        'model_name=~"$sglang_model_name"}[$__rate_interval])))'
    )
    assert enforce_selector(query, RUN, LABEL) == (
        "histogram_quantile(0.99, sum by (le, replica) "
        '(rate({__name__=~"sglang[:_]e2e_request_latency_seconds_bucket", '
        'model_name=~"$sglang_model_name", run_id="run-a"}[$__rate_interval])))'
    )
    assert enforce_selector("rate(a[5m:1m] offset 1h)", RUN, LABEL) == (
        'rate(a{run_id="run-a"}[5m:1m] offset 1h)'
    )


def test_string_literals_are_never_rewritten():
    assert enforce_selector('a{x="met{ric}", y="a,b"}', RUN, LABEL) == (
        'a{x="met{ric}", y="a,b", run_id="run-a"}'
    )


def test_a_line_comment_never_hides_a_selector_from_the_identity_selector():
    """PromQL's ``#`` comment is copied, not scanned: it cannot swallow a selector."""
    query = 'sum(a[1m]) # " unclosed quote hides rate(b[1m])\n + rate(b[1m])'
    assert enforce_selector(query, RUN, LABEL) == (
        'sum(a{run_id="run-a"}[1m]) # " unclosed quote hides rate(b[1m])\n'
        ' + rate(b{run_id="run-a"}[1m])'
    )


def test_a_comment_inside_a_matcher_set_is_ignored_not_treated_as_a_matcher():
    assert enforce_selector('a{b="c", # note\n d="e"}', RUN, LABEL) == (
        'a{b="c", d="e", run_id="run-a"}'
    )


def test_a_binary_expression_operator_is_never_read_as_a_metric():
    # ``atan2`` is a binary operator in the frozen Prometheus 2.54.1, so
    # injecting a selector after it would be a syntax error the backend answers
    # with 400.
    assert enforce_selector("a atan2 b", RUN, LABEL) == (
        'a{run_id="run-a"} atan2 b{run_id="run-a"}'
    )


@pytest.mark.parametrize(
    "expression", ["count", "start", "sum", "offset", "and", "unless", "smoothed"]
)
def test_a_metric_named_like_a_keyword_is_scoped(expression):
    """A keyword token the frozen grammar also accepts as ``metric_identifier``.

    ``sum``/``count``/``start``/``offset``/``and`` are keywords *and* legal
    metric names in Prometheus 2.54.1, so a bare one is a vector selector over
    every run until the identity matcher is added. ``smoothed`` is not even a
    keyword in that version, so it is an ordinary metric name too.
    """
    assert enforce_selector(expression, RUN, LABEL) == f'{expression}{{run_id="run-a"}}'


def test_a_keyword_position_is_decided_by_grammar_not_by_the_word_alone():
    # The same word is an operator between two operands and a metric at the
    # start of one; only the position decides.
    assert enforce_selector("sum and count", RUN, LABEL) == (
        'sum{run_id="run-a"} and count{run_id="run-a"}'
    )
    assert enforce_selector("rate(start[5m])", RUN, LABEL) == (
        'rate(start{run_id="run-a"}[5m])'
    )
    assert enforce_selector("rate(a[5m]) offset 1h", RUN, LABEL) == (
        'rate(a{run_id="run-a"}[5m]) offset 1h'
    )
    assert enforce_selector("sum by (le) (rate(a[5m]))", RUN, LABEL) == (
        'sum by (le) (rate(a{run_id="run-a"}[5m]))'
    )


@pytest.mark.parametrize(
    ("literal", "misread_id"),
    [
        ('"run-\\101"', "run-101"),
        ('"run-\\x41"', "run-x41"),
        ('"run-\\u0041"', "run-u0041"),
    ],
)
def test_an_escape_is_decoded_the_way_prometheus_decodes_it(literal, misread_id):
    """A half-decoded literal must not pin *another* run as if it were this one."""
    with pytest.raises(QueryError) as caught:
        enforce_selector(f"a{{run_id={literal}}}", misread_id, LABEL)
    assert caught.value.code == IDENTITY_SELECTOR_CONFLICT

    # Decoded the way Prometheus decodes it, the same literal is this run.
    assert enforce_selector(f"a{{run_id={literal}}}", "run-A", LABEL) == (
        f"a{{run_id={literal}}}"
    )


def test_a_literal_with_an_unknown_escape_is_refused():
    with pytest.raises(QueryError) as caught:
        enforce_selector('a{run_id="run-\\q"}', RUN, LABEL)
    assert caught.value.code == UNSUPPORTED_EXPRESSION
    assert caught.value.status == 422


def test_the_escaped_identity_matcher_the_sdk_generates_compares_equal():
    assert enforce_selector(identity_selector('a"b\\c', LABEL), 'a"b\\c', LABEL) == (
        identity_selector('a"b\\c', LABEL)
    )


def test_an_unparsable_expression_is_rejected():
    for broken in ("a{", 'a{x=~"y"'):
        with pytest.raises(QueryError) as caught:
            enforce_selector(broken, RUN, LABEL)
        assert caught.value.code == UNSUPPORTED_EXPRESSION
        assert caught.value.status == 422


def test_an_empty_matcher_set_still_gains_the_identity_selector():
    assert enforce_selector("foo{}", RUN, LABEL) == 'foo{run_id="run-a"}'


def test_a_match_selector_is_scoped_and_must_be_one_selector():
    assert enforce_match_selector('a{b="c"}', RUN, LABEL) == (
        'a{b="c", run_id="run-a"}'
    )
    assert enforce_match_selector('{__name__=~"a:b"}', RUN, LABEL) == (
        '{__name__=~"a:b", run_id="run-a"}'
    )
    for unsupported in ("", "sum(a)", "a and b"):
        with pytest.raises(QueryError) as caught:
            enforce_match_selector(unsupported, RUN, LABEL)
        assert caught.value.code == UNSUPPORTED_EXPRESSION
        assert caught.value.status == 422


def test_the_identity_selector_escapes_its_value():
    assert identity_selector('a"b\\c', LABEL) == '{run_id="a\\"b\\\\c"}'
