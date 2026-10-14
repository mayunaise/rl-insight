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

"""Unit tests for the built-in OTLP span sink (no network: in-memory exporter)."""

from __future__ import annotations

import pytest
from omegaconf import OmegaConf

from rl_insight.client.push.driver import build_sink
from rl_insight.client.push.sinks.otlp import OtlpSpanSink, create_sink

pytest.importorskip(
    "opentelemetry.exporter.otlp.proto.http.trace_exporter",
    reason="otel extra required for the OTLP span sink",
)
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)


def _sink(**kwargs) -> tuple[OtlpSpanSink, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    sink = OtlpSpanSink("http://collector:4318/v1/traces", exporter=exporter, **kwargs)
    return sink, exporter


def test_emit_span_exports_backdated_span_with_clean_attributes() -> None:
    sink, exporter = _sink()
    sink.emit_span(
        "trace.generate",
        1_000_000,
        3_500_000,
        {
            "state_name": "generate",
            "seq": [1, 2],
            "flag": True,
            "drop_dict": {"a": 1},
            "drop_none": None,
        },
    )
    sink.flush()

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "generate"  # default "trace." prefix stripped
    assert span.start_time == 1_000_000
    assert span.end_time == 3_500_000
    attrs = dict(span.attributes)
    assert attrs["state_name"] == "generate"
    assert tuple(attrs["seq"]) == (1, 2)
    assert attrs["flag"] is True
    assert "drop_dict" not in attrs
    assert "drop_none" not in attrs
    assert span.resource.attributes["service.name"] == "rl_insight_monitor"
    sink.close()


def test_custom_strip_prefix_and_service_name() -> None:
    sink, exporter = _sink(strip_name_prefix="", service_name="trainer-job")
    sink.emit_span("trace.op", 0, 1000, {})
    sink.flush()
    span = exporter.get_finished_spans()[0]
    assert span.name == "trace.op"  # no stripping
    assert span.resource.attributes["service.name"] == "trainer-job"
    sink.close()


def test_metric_primitives_are_no_ops() -> None:
    sink, exporter = _sink()
    sink.emit_counter("c", 1, {})
    sink.emit_store("g", 2, {})
    sink.emit_timer("t", 3, {})
    sink.flush()
    assert not exporter.get_finished_spans()
    sink.close()


def test_factory_reads_config_and_requires_endpoint() -> None:
    conf = OmegaConf.create(
        {
            "driver": "rl_insight.client.push.sinks.otlp:create_sink",
            "name": "tempo",
            "endpoint": "http://tempo:4318/v1/traces",
            "service_name": "svc",
        }
    )
    sink = create_sink(conf)
    try:
        assert isinstance(sink, OtlpSpanSink)
    finally:
        sink.close()

    with pytest.raises(ValueError):
        create_sink(OmegaConf.create({"endpoint": ""}))


def test_driver_loads_otlp_sink() -> None:
    conf = OmegaConf.create(
        {
            "driver": "rl_insight.client.push.sinks.otlp:create_sink",
            "name": "tempo",
            "endpoint": "http://tempo:4318/v1/traces",
        }
    )
    sink = build_sink(conf)
    try:
        assert isinstance(sink, OtlpSpanSink)
    finally:
        if sink is not None:
            sink.close()
