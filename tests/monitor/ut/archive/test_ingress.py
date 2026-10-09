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

"""Unit tests for the OTLP/HTTP trace ingress (S1 §5)."""

from __future__ import annotations

from pathlib import Path

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource

from rl_insight.archive.ingress import (
    EPOCH_HEADER,
    INVALID_IDENTITY,
    MIXED_IDENTITY,
    RUN_ID_HEADER,
    SERVER_INSTANCE_HEADER,
    WRONG_INSTANCE,
    decode_run_id,
    encode_run_id,
    ingest_trace_request,
)
from rl_insight.archive.writer import ArchiveStore
from rl_insight.server.run_registry import RegistryError

SERVER_INSTANCE = "server-1"


class _Registry:
    """Registry stand-in: only the write gate matters to the ingress."""

    def __init__(
        self, *, code: str = "", epoch: int = 1, identity: object | None = None
    ):
        self.code = code
        self.epoch = epoch
        self.identity = identity
        self.seen: list[tuple[str, int | None]] = []

    def writable_identity(self, key: str, *, epoch: int | None = None):
        self.seen.append((key, epoch))
        if self.code:
            raise RegistryError(self.code, self.code)
        return self.identity

    @property
    def server_instance_id(self) -> str:
        return SERVER_INSTANCE


def _request(
    run_id: str | None, spans: int = 1, extra_run_ids: tuple[str, ...] = ()
) -> bytes:
    request = ExportTraceServiceRequest()
    values = ([run_id] if run_id else []) + list(extra_run_ids)
    for index, value in enumerate(values):
        resource_spans = request.resource_spans.add()
        if value is not None:
            resource_spans.resource.CopyFrom(
                Resource(
                    attributes=[
                        KeyValue(
                            key="rl_insight.run_id", value=AnyValue(string_value=value)
                        )
                    ]
                )
            )
        scope_spans = resource_spans.scope_spans.add()
        for span_index in range(spans):
            scope_spans.spans.add(
                trace_id=bytes([index]) * 16,
                span_id=bytes([span_index + 1]) * 8,
                name=f"span-{index}-{span_index}",
            )
    if not values:
        scope_spans = request.resource_spans.add().scope_spans.add()
        for span_index in range(spans):
            scope_spans.spans.add(
                trace_id=b"\x00" * 16, span_id=bytes([span_index + 1]) * 8
            )
    return request.SerializeToString()


def _headers(
    run_id: str, *, epoch: int = 1, instance: str = SERVER_INSTANCE
) -> dict[str, str]:
    return {
        RUN_ID_HEADER: encode_run_id(run_id),
        EPOCH_HEADER: str(epoch),
        SERVER_INSTANCE_HEADER: instance,
    }


def _call(
    tmp_path: Path,
    body: bytes,
    headers: dict[str, str],
    registry=None,
    content_type=None,
):
    store = ArchiveStore(tmp_path)
    identity = _identity()
    registry = registry or _Registry(identity=identity)
    return store, ingest_trace_request(
        body=body,
        content_type=content_type or "application/x-protobuf",
        headers=headers,
        registry=registry,
        store=store,
        server_instance_id=SERVER_INSTANCE,
    )


def _identity(run_id: str = "run-a"):
    from rl_insight.sdk.identity import RunIdentity

    return RunIdentity(id=run_id, source="managed", project="proj")


def test_run_id_header_is_base64url_without_padding():
    encoded = encode_run_id("run/with+odd=chars")
    assert "=" not in encoded and "/" not in encoded and "+" not in encoded
    assert decode_run_id(encoded) == "run/with+odd=chars"
    with pytest.raises(ValueError):
        decode_run_id("not*base64")
    with pytest.raises(ValueError):
        decode_run_id(encode_run_id(""))


def test_an_accepted_batch_is_archived_and_reports_its_accept_seq(tmp_path: Path):
    store, outcome = _call(tmp_path, _request("run-a", spans=2), _headers("run-a"))
    assert outcome.accepted is True
    assert (outcome.span_count, outcome.accept_seq) == (2, 2)
    assert store.coverage(_identity(), "traces")["record_count"] == 2


def test_a_retried_batch_is_idempotent_and_never_duplicates_spans(tmp_path: Path):
    body = _request("run-a", spans=2)
    store, first = _call(tmp_path, body, _headers("run-a"))
    _, retry = _call(tmp_path, body, _headers("run-a"))
    assert retry.accepted is True
    assert retry.duplicates == 2
    assert store.coverage(_identity(), "traces")["record_count"] == 2


def test_a_missing_or_undecodable_routing_header_is_refused(tmp_path: Path):
    _, outcome = _call(tmp_path, _request("run-a"), {})
    assert (outcome.accepted, outcome.code) == (False, INVALID_IDENTITY)

    bad = {**_headers("run-a"), RUN_ID_HEADER: "%%%"}
    _, outcome = _call(tmp_path, _request("run-a"), bad)
    assert (outcome.accepted, outcome.code) == (False, INVALID_IDENTITY)


def test_a_request_for_another_server_instance_is_refused(tmp_path: Path):
    _, outcome = _call(tmp_path, _request("run-a"), _headers("run-a", instance="other"))
    assert (outcome.accepted, outcome.code, outcome.status) == (
        False,
        WRONG_INSTANCE,
        409,
    )


def test_a_batch_carrying_two_run_identities_is_rejected_as_a_whole(tmp_path: Path):
    body = _request("run-a", spans=1, extra_run_ids=("run-b",))
    store, outcome = _call(tmp_path, body, _headers("run-a"))
    assert (outcome.accepted, outcome.code) == (False, MIXED_IDENTITY)
    assert store.coverage(_identity(), "traces")["record_count"] == 0


def test_a_resource_without_the_identity_attribute_is_rejected(tmp_path: Path):
    body = _request(None, spans=1)
    store, outcome = _call(tmp_path, body, _headers("run-a"))
    assert (outcome.accepted, outcome.code) == (False, MIXED_IDENTITY)
    assert store.coverage(_identity(), "traces")["record_count"] == 0


def test_the_registry_write_gate_decides_archived_and_epoch_failures(tmp_path: Path):
    store, outcome = _call(
        tmp_path,
        _request("run-a"),
        _headers("run-a"),
        registry=_Registry(code="RUN_ARCHIVED"),
    )
    assert (outcome.accepted, outcome.code, outcome.status) == (
        False,
        "RUN_ARCHIVED",
        410,
    )
    assert store.coverage(_identity(), "traces")["record_count"] == 0

    _, outcome = _call(
        tmp_path,
        _request("run-a"),
        _headers("run-a"),
        registry=_Registry(code="STALE_EPOCH"),
    )
    assert (outcome.accepted, outcome.code, outcome.status) == (
        False,
        "STALE_EPOCH",
        409,
    )


def test_ingress_writes_into_the_managed_run_resolved_by_the_registry(tmp_path: Path):
    # The header id is the routing key; the registry decides which identity owns it.
    _, outcome = _call(tmp_path, _request("run-a"), _headers("run-a"))
    assert outcome.accepted is True
    store = ArchiveStore(tmp_path)
    assert store.coverage(_identity(), "traces")["record_count"] == 1
    assert store.coverage(_identity("run-b"), "traces")["record_count"] == 0


def test_garbage_bytes_are_a_protocol_error_not_a_partial_accept(tmp_path: Path):
    _, outcome = _call(tmp_path, b"\xff\xff\xff\xff", _headers("run-a"))
    assert outcome.accepted is False
    assert outcome.code in ("MALFORMED_PROTOBUF", "MIXED_IDENTITY")
