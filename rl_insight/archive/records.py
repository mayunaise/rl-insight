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

"""Archive record types, canonical keys and lossless value encoding (S1 §4.1).

Metrics keep the raw IEEE-754 bit pattern, so the Prometheus stale marker is
never confused with an ordinary NaN. Spans keep the serialized OTLP payload
instead of a query-shaped summary.
"""

from __future__ import annotations

import hashlib
import json
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

METRICS = "metrics"
TRACES = "traces"
STREAMS = (METRICS, TRACES)

FLOAT = "float"
HISTOGRAM = "histogram"
SPAN = "span"
SAMPLE_KINDS = (FLOAT, HISTOGRAM)

# Trace record versions (S2 §5). V1 stored the span body only: it stays
# readable, but Resource/Scope/schema URL cannot be invented from it, so it is
# not losslessly replayable. V2 stores a single-span
# ``ExportTraceServiceRequest`` envelope.
SPAN_RECORD_V1 = 1
SPAN_RECORD_V2 = 2
SPAN_RECORD_VERSION = SPAN_RECORD_V2


def encode_value(value: float) -> str:
    """Encode a float as its IEEE-754 big-endian bit pattern (hex)."""
    return struct.pack(">d", float(value)).hex()


def decode_value(bits: str) -> float:
    """Decode a bit pattern produced by :func:`encode_value`."""
    return struct.unpack(">d", bytes.fromhex(bits))[0]


@dataclass(frozen=True)
class MetricRecord:
    """One archived metric sample.

    ``run_id`` is implicit: a store directory belongs to exactly one run, and
    the identity label is part of ``labels`` so query routing can re-check it.
    """

    labels: Mapping[str, str]
    timestamp_ms: int
    value_bits: str
    sample_kind: str = FLOAT

    def __post_init__(self) -> None:
        if self.sample_kind not in SAMPLE_KINDS:
            raise ValueError(f"unsupported sample kind {self.sample_kind!r}")
        if not isinstance(self.timestamp_ms, int):
            raise ValueError("timestamp_ms must be an integer millisecond value")

    @property
    def key(self) -> tuple[Any, ...]:
        """Canonical identity of this sample (S1 §4.1)."""
        return (tuple(sorted(self.labels.items())), self.timestamp_ms, self.sample_kind)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.value_bits.encode("ascii")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "labels": dict(self.labels),
            "timestamp_ms": self.timestamp_ms,
            "value_bits": self.value_bits,
            "sample_kind": self.sample_kind,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MetricRecord:
        return cls(
            labels={str(k): str(v) for k, v in dict(data["labels"]).items()},
            timestamp_ms=int(data["timestamp_ms"]),
            value_bits=str(data["value_bits"]),
            sample_kind=str(data.get("sample_kind", FLOAT)),
        )


@dataclass(frozen=True)
class SpanRecord:
    """One accepted OTLP span, kept as its serialized protobuf payload."""

    #: Sample type reported on the boundary; a span is never float/histogram.
    sample_kind: ClassVar[str] = SPAN

    trace_id: str
    span_id: str
    payload_b64: str
    #: ``SPAN_RECORD_V2`` carries the full envelope, V1 only the span body.
    version: int = SPAN_RECORD_V1

    def __post_init__(self) -> None:
        if self.version not in (SPAN_RECORD_V1, SPAN_RECORD_V2):
            raise ValueError(f"unsupported span record version {self.version!r}")

    @property
    def replayable(self) -> bool:
        """Only the envelope version can be replayed without inventing identity."""
        return self.version >= SPAN_RECORD_V2

    @property
    def key(self) -> tuple[str, str]:
        return (self.trace_id, self.span_id)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.payload_b64.encode("ascii")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "payload": self.payload_b64,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SpanRecord:
        return cls(
            trace_id=str(data["trace_id"]),
            span_id=str(data["span_id"]),
            payload_b64=str(data["payload"]),
            version=int(data.get("version", SPAN_RECORD_V1)),
        )


def record_from_dict(stream: str, data: Mapping[str, Any]) -> MetricRecord | SpanRecord:
    if stream == METRICS:
        return MetricRecord.from_dict(data)
    if stream == TRACES:
        return SpanRecord.from_dict(data)
    raise ValueError(f"unknown archive stream {stream!r}")


def encode_line(seq: int, record: MetricRecord | SpanRecord) -> str:
    return json.dumps({"seq": seq, "r": record.to_dict()}, sort_keys=True) + "\n"


def decode_line(stream: str, line: str) -> tuple[int, MetricRecord | SpanRecord]:
    payload = json.loads(line)
    return int(payload["seq"]), record_from_dict(stream, payload["r"])
