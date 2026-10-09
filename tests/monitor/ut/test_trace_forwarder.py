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

"""Lossless trace envelopes and forwarding only the accepted prefix (S2 §5)."""

from __future__ import annotations

import base64
import time
from pathlib import Path
from typing import Any

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue

from rl_insight.archive.forwarder import (
    FORWARD_ROUND_FAILED,
    LOSSLESS_REPLAY_UNAVAILABLE,
    PARTIAL_SUCCESS,
    TEMPO_UNAVAILABLE,
    TraceForwarder,
)
from rl_insight.archive.ingress import (
    EPOCH_HEADER,
    RUN_ID_HEADER,
    SERVER_INSTANCE_HEADER,
    encode_run_id,
    ingest_trace_request,
)
from rl_insight.archive.records import SpanRecord
from rl_insight.archive.writer import ArchiveStore
from rl_insight.sdk.identity import RunIdentity
from rl_insight.server.run_registry import RunRegistry

SERVER_INSTANCE = "server-1"
RESOURCE_ATTRIBUTES = (
    ("rl_insight.run_id", "run-a"),
    ("service.name", "rollout"),
)


def _registry(tmp_path: Path) -> RunRegistry:
    return RunRegistry(tmp_path / "registry.sqlite", server_instance_id=SERVER_INSTANCE)


def _request(run_id: str, spans: int = 2) -> bytes:
    request = ExportTraceServiceRequest()
    resource_spans = request.resource_spans.add()
    resource_spans.schema_url = "https://schema.example/resource/1.0"
    for key, value in RESOURCE_ATTRIBUTES:
        if key == "rl_insight.run_id":
            value = run_id
        resource_spans.resource.attributes.add(
            key=key, value=AnyValue(string_value=value)
        )
    scope_spans = resource_spans.scope_spans.add()
    scope_spans.schema_url = "https://schema.example/scope/1.0"
    scope_spans.scope.name = "verl.trainer"
    scope_spans.scope.version = "1.2.3"
    for index in range(spans):
        span = scope_spans.spans.add(
            trace_id=bytes([index + 1]) * 16,
            span_id=bytes([index + 1]) * 8,
            name=f"span-{index}",
        )
        span.attributes.add(key="rank", value=AnyValue(int_value=index))
    return request.SerializeToString()


def _ingest(
    registry: RunRegistry, store: ArchiveStore, run: dict[str, Any], body: bytes
):
    return ingest_trace_request(
        body=body,
        content_type="application/x-protobuf",
        headers={
            RUN_ID_HEADER: encode_run_id(run["identity"]["id"]),
            EPOCH_HEADER: str(run["lifecycle_epoch"]),
            SERVER_INSTANCE_HEADER: SERVER_INSTANCE,
        },
        registry=registry,
        store=store,
        server_instance_id=SERVER_INSTANCE,
    )


class _TempoResponse:
    def __init__(self, status_code: int = 200, payload: bytes | None = None) -> None:
        self.status_code = status_code
        self.content = payload if payload is not None else b""
        self.headers = {"content-type": "application/x-protobuf"}


class _Tempo:
    def __init__(self, response: _TempoResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []
        self.decoded: list[ExportTraceServiceRequest] = []

    def post(self, url: str, **kwargs: Any) -> _TempoResponse:
        self.calls.append({"url": url, **kwargs})
        envelope = ExportTraceServiceRequest()
        envelope.ParseFromString(kwargs["data"])
        self.decoded.append(envelope)
        return self.response


def _accepted() -> _TempoResponse:
    return _TempoResponse(200, ExportTraceServiceResponse().SerializeToString())


def _partial() -> _TempoResponse:
    response = ExportTraceServiceResponse()
    response.partial_success.rejected_spans = 1
    response.partial_success.error_message = "too large"
    return _TempoResponse(200, response.SerializeToString())


def test_the_archive_keeps_resource_scope_and_schema_urls(tmp_path: Path):
    registry = _registry(tmp_path)
    store = ArchiveStore(tmp_path / "data")
    run = registry.create_run(project="proj", idempotency_key="k")

    outcome = _ingest(registry, store, run, _request(run["identity"]["id"]))
    assert outcome.accepted is True

    identity = RunIdentity.from_dict(run["identity"])
    pending = store.pending_forward(identity, "traces")
    assert len(pending) == 2
    seq, record = pending[0]
    assert seq == 1
    assert isinstance(record, SpanRecord) and record.version == 2 and record.replayable

    envelope = ExportTraceServiceRequest()
    envelope.ParseFromString(base64.b64decode(record.payload_b64))
    resource_spans = envelope.resource_spans[0]
    assert resource_spans.schema_url == "https://schema.example/resource/1.0"
    attributes = {
        item.key: item.value.string_value for item in resource_spans.resource.attributes
    }
    assert attributes["service.name"] == "rollout"
    assert attributes["rl_insight.run_id"] == run["identity"]["id"]
    scope_spans = resource_spans.scope_spans[0]
    assert scope_spans.schema_url == "https://schema.example/scope/1.0"
    assert (scope_spans.scope.name, scope_spans.scope.version) == (
        "verl.trainer",
        "1.2.3",
    )
    assert scope_spans.spans[0].name == "span-0"


def test_forwarding_advances_only_the_prefix_tempo_accepted(tmp_path: Path):
    registry = _registry(tmp_path)
    store = ArchiveStore(tmp_path / "data")
    run = registry.create_run(project="proj", idempotency_key="k")
    _ingest(registry, store, run, _request(run["identity"]["id"]))
    identity = RunIdentity.from_dict(run["identity"])

    tempo = _Tempo(_accepted())
    forwarder = TraceForwarder(
        registry, store, tempo_endpoint="http://tempo.invalid/v1/traces", session=tempo
    )
    result = forwarder.run_once()

    assert result == {"runs": 1, "forwarded": 2, "errors": {}}
    assert store.pending_forward(identity, "traces") == []
    assert [call["url"] for call in tempo.calls] == [
        "http://tempo.invalid/v1/traces",
        "http://tempo.invalid/v1/traces",
    ]
    assert tempo.calls[0]["headers"] == {"Content-Type": "application/x-protobuf"}
    assert registry.run_health(run["run_key"])["channels"]["trace_online"] == {
        "status": "ready",
        "reason_code": "",
    }


def test_a_failed_batch_keeps_the_backlog_and_reports_the_reason(tmp_path: Path):
    registry = _registry(tmp_path)
    store = ArchiveStore(tmp_path / "data")
    run = registry.create_run(project="proj", idempotency_key="k")
    _ingest(registry, store, run, _request(run["identity"]["id"]))
    identity = RunIdentity.from_dict(run["identity"])

    tempo = _Tempo(_TempoResponse(500, b""))
    forwarder = TraceForwarder(
        registry, store, tempo_endpoint="http://tempo.invalid/v1/traces", session=tempo
    )
    result = forwarder.run_once()

    assert result["forwarded"] == 0
    assert result["errors"][run["run_key"]] == TEMPO_UNAVAILABLE
    assert len(store.pending_forward(identity, "traces")) == 2
    health = registry.run_health(run["run_key"])["channels"]["trace_online"]
    assert health == {"status": "degraded", "reason_code": TEMPO_UNAVAILABLE}


def test_a_partial_success_is_degraded_and_the_batch_is_replayed(tmp_path: Path):
    registry = _registry(tmp_path)
    store = ArchiveStore(tmp_path / "data")
    run = registry.create_run(project="proj", idempotency_key="k")
    _ingest(registry, store, run, _request(run["identity"]["id"]))

    tempo = _Tempo(_partial())
    forwarder = TraceForwarder(
        registry, store, tempo_endpoint="http://tempo.invalid/v1/traces", session=tempo
    )
    result = forwarder.run_once()

    assert result["errors"][run["run_key"]] == PARTIAL_SUCCESS
    assert (
        len(store.pending_forward(RunIdentity.from_dict(run["identity"]), "traces"))
        == 2
    )


def test_a_legacy_span_record_is_reported_instead_of_fabricating_identity(
    tmp_path: Path,
):
    registry = _registry(tmp_path)
    store = ArchiveStore(tmp_path / "data")
    run = registry.create_run(project="proj", idempotency_key="k")
    identity = RunIdentity.from_dict(run["identity"])
    store.append(
        identity,
        "traces",
        [SpanRecord(trace_id="aa" * 16, span_id="bb" * 8, payload_b64="Zm9v")],
    )

    tempo = _Tempo(_accepted())
    forwarder = TraceForwarder(
        registry, store, tempo_endpoint="http://tempo.invalid/v1/traces", session=tempo
    )
    result = forwarder.run_once()

    assert result["errors"][run["run_key"]] == LOSSLESS_REPLAY_UNAVAILABLE
    assert tempo.calls == []
    assert len(store.pending_forward(identity, "traces")) == 1


def test_a_failing_round_reports_and_retries_instead_of_killing_the_worker(
    tmp_path: Path,
):
    """A raised store/Registry error is not allowed to stop forwarding silently."""
    registry = _registry(tmp_path)
    store = ArchiveStore(tmp_path / "data")
    calls = {"count": 0}
    real_list_runs = registry.list_runs

    def flaky_list_runs(*args: Any, **kwargs: Any):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("simulated store outage")
        return real_list_runs(*args, **kwargs)

    registry.list_runs = flaky_list_runs  # type: ignore[method-assign]
    forwarder = TraceForwarder(
        registry, store, tempo_endpoint="http://tempo.invalid/v1/traces", interval=0.01
    )
    forwarder.start()
    try:
        deadline = time.monotonic() + 2.0
        while forwarder.last_error != FORWARD_ROUND_FAILED:
            assert time.monotonic() < deadline, "the failure was never reported"
            time.sleep(0.01)
        # the worker survived the failure and ran the next round
        while calls["count"] < 2:
            assert time.monotonic() < deadline, "the worker stopped after failing"
            time.sleep(0.01)
        assert forwarder.running is True
    finally:
        forwarder.stop()
