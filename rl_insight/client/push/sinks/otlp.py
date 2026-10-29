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

"""Built-in sink that exports spans to an external OTLP/HTTP collector.

It subscribes to the ``trace`` stream and keeps the full span structure
(name, caller-supplied start/end time, attributes) instead of collapsing it to
a duration, so an external Tempo/Jaeger backend renders the same timeline as
the managed Ray backend. Metric events are no-ops here: pair this sink with a
metrics sink (logging, remote-write, ...) when both signals are needed.

OpenTelemetry is imported at module load time. The push driver loader
(:func:`rl_insight.client.push.driver.build_sink`) treats the resulting
ImportError as "sink unavailable", so installs without the ``otel`` extra keep
working as long as this sink is not configured.
"""

from __future__ import annotations

from typing import Any, Mapping

from omegaconf import OmegaConf

from ....server.network import normalize_http_headers, normalize_http_url

from .base import PushSink

# OTLP attribute values: scalars or homogeneous sequences of scalars.
_SCALAR = (str, bool, int, float)
_DEFAULT_SERVICE_NAME = "rl_insight_monitor"
_DEFAULT_STRIP_PREFIX = "trace."


def _clean_attributes(attributes: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep only OpenTelemetry-compatible attribute values."""
    out: dict[str, Any] = {}
    for key, value in (attributes or {}).items():
        if isinstance(value, bool) or isinstance(value, (str, int, float)):
            out[str(key)] = value
        elif (
            isinstance(value, (list, tuple))
            and value
            and all(isinstance(item, _SCALAR) for item in value)
        ):
            out[str(key)] = list(value)
    return out


class OtlpSpanSink(PushSink):
    """Export completed spans through an OTLP/HTTP ``OTLPSpanExporter``."""

    def __init__(
        self,
        endpoint: str,
        *,
        service_name: str = _DEFAULT_SERVICE_NAME,
        strip_name_prefix: str = _DEFAULT_STRIP_PREFIX,
        headers: Mapping[str, str] | None = None,
        exporter: Any | None = None,
    ) -> None:
        super().__init__("")
        # A missing otel install raises ImportError at module load; the push
        # driver loader catches it and disables only this sink.
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        self._strip = strip_name_prefix
        self._exporter = exporter or OTLPSpanExporter(
            endpoint=endpoint, headers=dict(headers or {}) or None
        )
        self._provider = TracerProvider(
            resource=Resource.create({"service.name": service_name})
        )
        self._provider.add_span_processor(BatchSpanProcessor(self._exporter))
        self._tracer = self._provider.get_tracer("rl-insight-push")

    # Metrics are not exported by this sink; subscribe it to the trace stream.
    def emit_counter(self, name: str, value: float, tags: Mapping[str, str]) -> None:
        return None

    def emit_store(self, name: str, value: float, tags: Mapping[str, str]) -> None:
        return None

    def emit_timer(self, name: str, value_us: float, tags: Mapping[str, str]) -> None:
        return None

    def emit_span(
        self,
        name: str,
        start_time_ns: int,
        end_time_ns: int,
        attributes: Mapping[str, Any],
    ) -> None:
        span_name = name
        if self._strip and span_name.startswith(self._strip):
            span_name = span_name[len(self._strip) :]
        span = self._tracer.start_span(
            span_name,
            attributes=_clean_attributes(attributes),
            start_time=int(start_time_ns),
        )
        span.end(end_time=int(end_time_ns))

    def flush(self) -> None:
        self._provider.force_flush()

    def close(self) -> None:
        self._provider.shutdown()


def create_sink(conf: Any) -> OtlpSpanSink:
    """Dotted-path factory; reads the OTLP endpoint from the sink config node."""
    endpoint = normalize_http_url(OmegaConf.select(conf, "endpoint"), "OTLP endpoint")
    service_name = str(OmegaConf.select(conf, "service_name") or _DEFAULT_SERVICE_NAME)
    strip_raw = OmegaConf.select(conf, "strip_name_prefix")
    strip_prefix = _DEFAULT_STRIP_PREFIX if strip_raw is None else str(strip_raw)
    headers = normalize_http_headers(OmegaConf.select(conf, "headers"), "OTLP headers")
    return OtlpSpanSink(
        endpoint,
        service_name=service_name,
        strip_name_prefix=strip_prefix,
        headers=headers,
    )
