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

"""Unit tests for PushSink naming and the built-in logging driver."""

from __future__ import annotations

import logging

from omegaconf import OmegaConf

from rl_insight.client.push.sinks import LoggingSink
from rl_insight.client.push.sinks.base import PushSink
from rl_insight.client.push.sinks.logging import create_sink


class _MetricsOnlySink(PushSink):
    """Sink implementing only the three abstract metric primitives."""

    def __init__(self) -> None:
        super().__init__("root")
        self.calls: list[tuple[str, str, float, dict]] = []

    def emit_counter(self, name, value, tags):
        self.calls.append(("counter", name, value, dict(tags)))

    def emit_store(self, name, value, tags):
        self.calls.append(("store", name, value, dict(tags)))

    def emit_timer(self, name, value_us, tags):
        self.calls.append(("timer", name, value_us, dict(tags)))


def test_default_emit_histogram_should_fall_back_to_timer() -> None:
    sink = _MetricsOnlySink()
    sink.emit_histogram("lat", 900, {"a": "1"})
    assert sink.calls == [("timer", "lat", 900, {"a": "1"})]


def test_default_emit_span_should_fall_back_to_duration_timer() -> None:
    sink = _MetricsOnlySink()
    sink.emit_span(
        "trace.generate",
        1_000_000,
        3_500_000,
        {"state_lane_id": "replica_0", "seq": [1, 2], "empty": None},
    )
    assert sink.calls == [
        ("timer", "trace.generate", 2500.0, {"state_lane_id": "replica_0"})
    ]


def test_metric_name_should_join_prefix_and_suffix() -> None:
    sink = LoggingSink(prefix="seed.alphaseed")
    assert sink.metric_name("trainer.reward") == "seed.alphaseed.trainer.reward"


def test_metric_name_should_return_suffix_when_prefix_empty() -> None:
    sink = LoggingSink(prefix="")
    assert sink.metric_name("TTFT") == "TTFT"


def test_metric_name_should_normalize_dots() -> None:
    sink = LoggingSink(prefix=".xgpt.server.infer.")
    assert sink.metric_name(".TTFT") == "xgpt.server.infer.TTFT"


def test_logging_sink_should_emit_three_kinds(caplog) -> None:
    sink = LoggingSink(prefix="p")
    with caplog.at_level(logging.INFO, logger="rl_insight.push.logging"):
        sink.emit_counter("c", 3, {"a": "1"})
        sink.emit_store("g", 1.5, {})
        sink.emit_timer("t", 2500, {})
    messages = [r.getMessage() for r in caplog.records]
    assert any("counter p.c=3" in m and "'a': '1'" in m for m in messages)
    assert any("store p.g=1.5" in m for m in messages)
    assert any("timer p.t=2500us" in m for m in messages)


def test_logging_sink_should_emit_histogram_and_span(caplog) -> None:
    sink = LoggingSink(prefix="p")
    with caplog.at_level(logging.INFO, logger="rl_insight.push.logging"):
        sink.emit_histogram("h", 7, {})
        sink.emit_span("trace.op", 1000, 2000, {"state_name": "gen"})
    messages = [r.getMessage() for r in caplog.records]
    assert any("histogram p.h=7" in m for m in messages)
    assert any("span p.trace.op" in m and "state_name" in m for m in messages)


def test_create_sink_factory_should_read_prefix() -> None:
    sink = create_sink(OmegaConf.create({"prefix": "seed.alphaseed"}))
    assert isinstance(sink, LoggingSink)
    assert sink.prefix == "seed.alphaseed"
