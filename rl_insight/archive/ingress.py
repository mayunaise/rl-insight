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

"""OTLP/HTTP trace ingress for managed runs (S1 §5).

The managed SDK pushes OTLP/HTTP protobuf to the discovery-provided URL. This
module decodes one ``ExportTraceServiceRequest`` exactly once, checks every
``ResourceSpans`` against the routing headers, and archives the accepted spans
before acknowledging: an ACK means "durably accepted", never "queryable".
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from google.protobuf.message import DecodeError  # type: ignore[import-untyped]
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

from ..sdk.identity import (
    EPOCH_HEADER,
    MANAGED,
    RUN_ID_HEADER,
    SERVER_INSTANCE_HEADER,
    IdentityError,
    decode_run_id_header,
    encode_run_id_header,
    run_key,
)
from ..server.run_registry import RegistryError
from .records import SPAN_RECORD_VERSION, TRACES, SpanRecord
from .writer import ArchiveStore

RUN_ATTRIBUTE = "rl_insight.run_id"
PROTOBUF_CONTENT_TYPE = "application/x-protobuf"

INVALID_IDENTITY = "INVALID_IDENTITY"
INVALID_CONFIG = "INVALID_CONFIG"
UNSUPPORTED_ENCODING = "UNSUPPORTED_ENCODING"
MIXED_IDENTITY = "MIXED_IDENTITY"
STALE_EPOCH = "STALE_EPOCH"
WRONG_INSTANCE = "WRONG_INSTANCE"
RUN_ARCHIVED = "RUN_ARCHIVED"
RUN_NOT_FOUND = "RUN_NOT_FOUND"
MALFORMED_PROTOBUF = "MALFORMED_PROTOBUF"
SPAN_CONFLICT = "SPAN_CONFLICT"
CAPACITY_EXCEEDED = "CAPACITY_EXCEEDED"

_STATUS = {
    INVALID_IDENTITY: 400,
    INVALID_CONFIG: 400,
    UNSUPPORTED_ENCODING: 415,
    MALFORMED_PROTOBUF: 400,
    MIXED_IDENTITY: 400,
    SPAN_CONFLICT: 409,
    STALE_EPOCH: 409,
    WRONG_INSTANCE: 409,
    RUN_ARCHIVED: 410,
    RUN_NOT_FOUND: 404,
    CAPACITY_EXCEEDED: 429,
}

#: Errors an OTLP client should retry with backoff instead of dropping data.
RETRYABLE_CODES = (CAPACITY_EXCEEDED,)


@dataclass(frozen=True)
class IngressResult:
    """Outcome of one OTLP/HTTP trace batch."""

    accepted: bool
    code: str = ""
    message: str = ""
    span_count: int = 0
    accept_seq: int = 0
    duplicates: int = 0

    @property
    def status(self) -> int:
        return 200 if self.accepted else _STATUS.get(self.code, 400)

    @property
    def retryable(self) -> bool:
        return self.code in RETRYABLE_CODES

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "code": self.code,
            "message": self.message,
            "span_count": self.span_count,
            "accept_seq": self.accept_seq,
            "duplicates": self.duplicates,
        }


def encode_run_id(run_id: str) -> str:
    """Wire form of the routing header (RFC v4.1 §5)."""
    return encode_run_id_header(run_id)


def decode_run_id(value: str) -> str:
    """Decode the routing header exactly once; the decoded value is opaque."""
    return decode_run_id_header(value)


def span_records(request: ExportTraceServiceRequest) -> list[SpanRecord]:
    """Flatten accepted spans into lossless single-span envelopes (S2 §5).

    Each record keeps the Resource, Scope, both schema URLs and the whole span,
    so replaying it cannot invent identity that the request did not carry.
    """
    records: list[SpanRecord] = []
    for resource_spans in request.resource_spans:
        for scope_spans in resource_spans.scope_spans:
            for span in scope_spans.spans:
                records.append(
                    SpanRecord(
                        trace_id=span.trace_id.hex(),
                        span_id=span.span_id.hex(),
                        payload_b64=base64.b64encode(
                            _span_envelope(resource_spans, scope_spans, span)
                        ).decode("ascii"),
                        version=SPAN_RECORD_VERSION,
                    )
                )
    return records


def _span_envelope(resource_spans: Any, scope_spans: Any, span: Any) -> bytes:
    """Serialize one ``ExportTraceServiceRequest`` containing exactly ``span``."""
    envelope = ExportTraceServiceRequest()
    target_resource = envelope.resource_spans.add()
    target_resource.resource.CopyFrom(resource_spans.resource)
    target_resource.schema_url = resource_spans.schema_url
    target_scope = target_resource.scope_spans.add()
    target_scope.scope.CopyFrom(scope_spans.scope)
    target_scope.schema_url = scope_spans.schema_url
    target_scope.spans.add().CopyFrom(span)
    return envelope.SerializeToString()


def resource_run_ids(request: ExportTraceServiceRequest) -> list[str | None]:
    """The ``rl_insight.run_id`` value of every ResourceSpans, in order."""
    values: list[str | None] = []
    for resource_spans in request.resource_spans:
        found: list[str] = [
            attribute.value.string_value
            for attribute in resource_spans.resource.attributes
            if attribute.key == RUN_ATTRIBUTE
        ]
        values.append(found[0] if len(found) == 1 else None)
    return values


def ingest_trace_request(
    *,
    body: bytes,
    content_type: str,
    headers: Mapping[str, str],
    registry: Any,
    store: ArchiveStore,
    server_instance_id: str,
) -> IngressResult:
    """Validate one OTLP/HTTP trace batch and archive it before acknowledging."""
    if content_type.split(";")[0].strip().lower() != PROTOBUF_CONTENT_TYPE:
        return IngressResult(
            accepted=False,
            code=UNSUPPORTED_ENCODING,
            message=f"only {PROTOBUF_CONTENT_TYPE} is accepted; no compression or JSON yet",
        )

    lowered = {key.lower(): value for key, value in headers.items()}
    raw_id = lowered.get(RUN_ID_HEADER.lower(), "")
    if not raw_id:
        return IngressResult(
            accepted=False,
            code=INVALID_IDENTITY,
            message=f"{RUN_ID_HEADER} is required",
        )
    try:
        header_id = decode_run_id(raw_id)
    except (ValueError, IdentityError) as exc:
        return IngressResult(accepted=False, code=INVALID_IDENTITY, message=str(exc))

    try:
        epoch = int(lowered[EPOCH_HEADER.lower()])
    except (KeyError, ValueError):
        return IngressResult(
            accepted=False,
            code=INVALID_CONFIG,
            message=f"{EPOCH_HEADER} must be an integer",
        )
    instance = lowered.get(SERVER_INSTANCE_HEADER.lower(), "")
    if instance != server_instance_id:
        return IngressResult(
            accepted=False,
            code=WRONG_INSTANCE,
            message="request targets a different server instance",
        )

    request = ExportTraceServiceRequest()
    try:
        request.ParseFromString(body)
    except DecodeError as exc:  # a malformed batch is a protocol error, never a 500
        return IngressResult(accepted=False, code=MALFORMED_PROTOBUF, message=str(exc))

    # Mixed identity is rejected as a whole: no partial acceptance, no legacy write.
    for value in resource_run_ids(request):
        if value != header_id:
            return IngressResult(
                accepted=False,
                code=MIXED_IDENTITY,
                message="every ResourceSpans must carry exactly the routing run id",
            )

    key = run_key(MANAGED, header_id)
    try:
        identity = registry.writable_identity(key, epoch=epoch)
    except RegistryError as exc:
        return IngressResult(accepted=False, code=exc.code, message=exc.message)

    records = span_records(request)
    result = store.append(identity, TRACES, records)
    if result.conflicts:
        return IngressResult(
            accepted=False,
            code=SPAN_CONFLICT,
            message="the same span id arrived with different content",
        )
    return IngressResult(
        accepted=True,
        span_count=result.accepted,
        accept_seq=result.last_seq,
        duplicates=result.duplicates,
    )
