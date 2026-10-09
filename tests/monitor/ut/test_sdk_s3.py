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

"""Unit tests for the S3 explicit session APIs (S3 §4–§6).

The trace tests bind a real loopback OTLP/HTTP sink and decode the protobuf the
exporter sends, but no Tempo, Prometheus or trainer process is involved.
"""

from __future__ import annotations

import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

from rl_insight.sdk import publisher as publisher_module
from rl_insight.sdk import session as session_module
from rl_insight.sdk.identity import (
    EPOCH_HEADER,
    INCARNATION_HEADER,
    RUN_ID_HEADER,
    SERVER_INSTANCE_HEADER,
    Context,
    ContextError,
    RunIdentity,
    decode_run_id_header,
)
from rl_insight.sdk.publisher import PublicationResult


class FakeManagedClient:
    """Minimal managed protocol stand-in: endpoints are accepted locally."""

    def __init__(self) -> None:
        self.upserts: list[dict[str, Any]] = []
        self.renewed: list[str] = []
        self.deleted: list[str] = []

    def upsert_endpoint(self, run_key: str, slot: str, payload: Any) -> dict[str, Any]:
        self.upserts.append({"run_key": run_key, "slot": slot, **dict(payload)})
        return {
            "endpoint_id": f"ep-{len(self.upserts)}",
            "lease_id": f"lease-{len(self.upserts)}",
            "revision": len(self.upserts),
            "lifecycle_epoch": 3,
            "expires_at": time.time() + 30,
            "renew_after": 10,
        }

    def renew_lease(self, run_key: str, lease_id: str, payload: Any) -> dict[str, Any]:
        self.renewed.append(lease_id)
        return {"lease_id": lease_id, "revision": payload["revision"]}

    def delete_lease(
        self, run_key: str, lease_id: str, payload: Any, *, timeout: float | None = None
    ) -> dict[str, Any]:
        self.deleted.append(lease_id)
        return {"deleted": True, "endpoint_id": "ep-1"}


class RecordingPublisher:
    """Platform publisher stand-in recording the unified facts it receives."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.upserts: list[tuple[RunIdentity, dict[str, Any]]] = []
        self.deleted: list[str] = []

    def upsert(self, identity: RunIdentity, facts: Any) -> PublicationResult:
        self.upserts.append((identity, dict(facts)))
        return PublicationResult(
            accepted=True,
            endpoint_id="ep-1",
            lease_id="lease-1",
            revision=1,
            # A real platform registry may have no managed epoch at all.
            lifecycle_epoch=None,
            expires_at=time.time() + 30,
            renew_after=10,
        )

    def renew(self, lease: Any) -> PublicationResult:
        return PublicationResult(accepted=True, lease_id=lease.lease_id, revision=1)

    def delete(self, lease: Any, *, timeout: float | None = None) -> PublicationResult:
        self.deleted.append(lease.lease_id)
        return PublicationResult(accepted=True, lease_id=lease.lease_id)


def _managed_context(run_id: str = "run-1", otlp: str | None = None) -> Context:
    services = {"server": "http://s:1"}
    if otlp:
        services["otlp_http"] = otlp
    return Context(
        identity=RunIdentity(
            id=run_id, source="managed", project="proj", experiment_name="exp"
        ),
        services=services,
        capabilities=("identity_v1", "session_context_v1"),
        publisher={"adapter": "managed"},
        server_instance_id="srv-1",
        lifecycle_epoch=3,
    )


def _platform_context(
    otlp: str | None = None, adapter: str = "fake-platform"
) -> Context:
    return Context(
        identity=RunIdentity(id="task-9", source="platform", project="p"),
        services={"otlp_http": otlp} if otlp else {},
        publisher={"adapter": adapter},
    )


def _facts(**overrides: Any) -> dict[str, Any]:
    return {
        "target_kind": "metrics",
        "component": "rollout",
        "instance_key": "r0",
        "incarnation": "inc-1",
        "advertised_host": "10.0.0.5",
        "actual_port": 9100,
        **overrides,
    }


class TraceSink:
    """Loopback OTLP/HTTP sink that decodes the protobuf requests it receives."""

    def __init__(self, *, respond: bool = True) -> None:
        self.requests: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        sink = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                with sink._lock:
                    sink.requests.append(
                        {
                            "path": self.path,
                            # HTTP header names are case-insensitive; urllib3 sends
                            # them lower-cased.
                            "headers": {
                                key.lower(): value
                                for key, value in self.headers.items()
                            },
                            "body": body,
                        }
                    )
                if not respond:
                    # Hold the request open: a hung collector, not a refused one.
                    time.sleep(5.0)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/x-protobuf")
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args: object) -> None:
                return None

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.port = int(self._server.server_address[1])
        self.url = f"http://127.0.0.1:{self.port}/v1/traces"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def _wait(self, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if self.requests:
                    return
            time.sleep(0.01)
        raise AssertionError("the OTLP sink received no request before the timeout")

    def request(self) -> ExportTraceServiceRequest:
        self._wait()
        with self._lock:
            return ExportTraceServiceRequest.FromString(self.requests[0]["body"])

    def headers(self) -> dict[str, str]:
        self._wait()
        with self._lock:
            return dict(self.requests[0]["headers"])

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()


def _resource_attributes(resource: Any) -> dict[str, Any]:
    return {item.key: item.value.string_value for item in resource.attributes}


# --------------------------------------------------------------- attach freezing


def test_attach_freezes_mode_identity_and_addresses_from_the_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _managed_context(otlp="http://ctx:4318/v1/traces")
    worker_env = (
        "RL_INSIGHT_SERVER_URL",
        "RL_INSIGHT_TASK_ID",
        "RL_INSIGHT_OTLP_HTTP_ENDPOINT",
        "RL_INSIGHT_PUBLISHER_ADAPTER",
    )
    for key in worker_env:
        monkeypatch.setenv(key, "http://other:9")
    client = FakeManagedClient()

    session = session_module.attach(context.to_json(), client=client)

    assert session.config.mode == "managed"
    assert session.config.server_url == "http://s:1"
    assert session.config.trace_endpoint == "http://ctx:4318/v1/traces"
    assert session.config.task_id is None
    assert session.config.publisher_adapter == "managed"
    assert session.identity == context.identity
    diagnostics = " ".join(session.config.diagnostics)
    for env_key in worker_env:
        assert env_key in diagnostics
    session.close()


def test_attach_ignores_overrides_that_would_change_identity_or_mode() -> None:
    context = _managed_context()
    session = session_module.attach(
        context,
        config={"server.url": "http://evil:1", "platform.task_id": "task-x"},
        env={},
        client=FakeManagedClient(),
    )

    assert session.config.server_url == "http://s:1"
    assert session.config.task_id is None
    assert session.identity == context.identity
    assert len(session.config.diagnostics) == 2
    session.close()


# --------------------------------------------------------- platform publisher factory


def test_platform_session_uses_the_registered_publisher_adapter() -> None:
    publisher_module.register_publisher_adapter("fake-platform", RecordingPublisher)
    session = session_module.open_session(
        {},
        config={
            "platform.task_id": "task-9",
            "publisher.adapter": "fake-platform",
            "publisher.config_ref": "secret://platform",
        },
    )
    publisher = session.publisher
    assert isinstance(publisher, RecordingPublisher)

    assert session.publish_endpoint(_facts()).accepted is True
    identity, facts = publisher.upserts[0]
    assert identity == session.identity
    assert facts["actual_port"] == 9100

    report = session.close()
    assert report.revoked_leases == ("lease-1",)
    assert publisher.deleted == ["lease-1"]
    assert session.leases == {}


def test_unknown_platform_publisher_adapter_stays_explicitly_unsupported() -> None:
    session = session_module.attach(
        Context(
            identity=RunIdentity(id="task-9", source="platform"),
            publisher={"adapter": "not-registered"},
        )
    )
    result = session.publish_endpoint(_facts())
    assert result.accepted is False
    assert result.code == "UNSUPPORTED_CAPABILITY"
    assert result.diagnostic == "PUBLISHER_ADAPTER_NOT_REGISTERED"


# ------------------------------------------------------------------- emit isolation


def test_session_metrics_endpoint_serves_only_its_own_registry() -> None:
    managed = FakeManagedClient()
    session_a = session_module.attach(_managed_context("run-a"), client=managed)
    session_b = session_module.attach(
        _managed_context("run-b"), client=FakeManagedClient()
    )
    try:
        session_a.metric_gauge("a_reward", 1.0)
        session_b.metric_gauge("b_reward", 2.0)
        endpoint = session_a.start_metrics_endpoint(
            component="rollout", instance_key="r0"
        )
        with urllib.request.urlopen(endpoint.server.url, timeout=5) as response:
            body = response.read().decode()
        assert "a_reward" in body and "b_reward" not in body
        assert managed.upserts[0]["actual_port"] == endpoint.server.port
    finally:
        session_a.close()
        session_b.close()


def test_platform_metrics_endpoint_does_not_fabricate_a_managed_epoch() -> None:
    publisher_module.register_publisher_adapter("fake-platform", RecordingPublisher)
    session = session_module.attach(_platform_context(adapter="fake-platform"))
    try:
        assert session.context is not None
        assert session.context.lifecycle_epoch is None
        endpoint = session.start_metrics_endpoint(
            component="engine", instance_key="rank/0"
        )
        assert endpoint.publication.accepted is True
        assert EPOCH_HEADER not in endpoint.server.headers
        with urllib.request.urlopen(endpoint.server.url, timeout=5) as response:
            assert response.headers[RUN_ID_HEADER] == "dGFzay05"
            assert response.headers[INCARNATION_HEADER] == endpoint.server.incarnation
            assert EPOCH_HEADER not in response.headers
    finally:
        session.close()


def test_managed_metrics_endpoint_still_requires_an_epoch() -> None:
    context = Context(
        identity=RunIdentity(id="run-1", source="managed"),
        services={"server": "http://s:1"},
        publisher={"adapter": "managed"},
    )
    session = session_module.attach(context, client=FakeManagedClient())
    try:
        with pytest.raises(ContextError):
            session.start_metrics_endpoint(component="rollout", instance_key="r0")
    finally:
        session.close()


# ------------------------------------------------------------------------- traces


def test_managed_trace_span_carries_identity_resource_and_headers() -> None:
    sink = TraceSink()
    session = session_module.attach(
        _managed_context(otlp=sink.url), client=FakeManagedClient()
    )
    try:
        session.trace_span(
            "rollout",
            start_time_ns=10,
            end_time_ns=25,
            attributes={"state_lane_id": "replica-0", "step": 3},
        )
        report = session.close()
        assert report.traces_flushed is True
        assert report.errors == ()

        request = sink.request()
        attributes = _resource_attributes(request.resource_spans[0].resource)
        assert attributes["rl_insight.run_id"] == "run-1"
        assert attributes["service.name"] == "rl_insight_monitor"
        assert attributes["rl_insight.project"] == "proj"
        assert attributes["rl_insight.experiment_name"] == "exp"
        span = request.resource_spans[0].scope_spans[0].spans[0]
        assert span.name == "rollout"
        assert {item.key for item in span.attributes} == {"state_lane_id", "step"}

        headers = sink.headers()
        assert decode_run_id_header(headers[RUN_ID_HEADER.lower()]) == "run-1"
        assert headers[EPOCH_HEADER.lower()] == "3"
        assert headers[SERVER_INSTANCE_HEADER.lower()] == "srv-1"
    finally:
        sink.stop()


def test_platform_trace_span_has_no_managed_control_headers() -> None:
    sink = TraceSink()
    session = session_module.attach(_platform_context(otlp=sink.url))
    try:
        session.trace_span("train", start_time_ns=1, end_time_ns=2)
        session.close()

        attributes = _resource_attributes(sink.request().resource_spans[0].resource)
        assert attributes["rl_insight.task_id"] == "task-9"
        assert "rl_insight.run_id" not in attributes
        headers = sink.headers()
        assert RUN_ID_HEADER.lower() not in headers
        assert EPOCH_HEADER.lower() not in headers
        assert SERVER_INSTANCE_HEADER.lower() not in headers
    finally:
        sink.stop()


def test_two_sessions_keep_their_tracers_and_identities_separate() -> None:
    sink_a, sink_b = TraceSink(), TraceSink()
    session_a = session_module.attach(
        _managed_context("run-a", otlp=sink_a.url), client=FakeManagedClient()
    )
    session_b = session_module.attach(
        _managed_context("run-b", otlp=sink_b.url), client=FakeManagedClient()
    )
    try:
        session_a.trace_span("a-span", start_time_ns=1, end_time_ns=2)
        session_a.close()
        session_b.trace_span("b-span", start_time_ns=1, end_time_ns=2)
        assert session_a.tracer is not session_b.tracer
        assert (
            _resource_attributes(sink_a.request().resource_spans[0].resource)[
                "rl_insight.run_id"
            ]
            == "run-a"
        )
        assert (
            _resource_attributes(sink_b.request().resource_spans[0].resource)[
                "rl_insight.run_id"
            ]
            == "run-b"
        )
    finally:
        session_a.close()
        session_b.close()
        sink_a.stop()
        sink_b.stop()


def test_close_keeps_the_total_budget_when_the_trace_backend_hangs() -> None:
    sink = TraceSink(respond=False)
    session = session_module.attach(
        _managed_context(otlp=sink.url), client=FakeManagedClient()
    )
    try:
        session.trace_span("slow", start_time_ns=1, end_time_ns=2)
        started = time.monotonic()
        report = session.close(deadline_seconds=0.3)
        elapsed = time.monotonic() - started
        assert elapsed < 2.0, f"close ignored its budget: {elapsed:.2f}s"
        assert report.traces_flushed is False
        assert "TRACE_FLUSH_TIMEOUT" in report.errors
    finally:
        sink.stop()


def test_emit_after_close_is_a_counted_no_op() -> None:
    session = session_module.attach(_managed_context(), client=FakeManagedClient())
    session.close()
    session.metric_count("steps_total")
    session.metric_gauge("reward", 1.0)
    session.metric_histogram("latency", 1.0)
    session.trace_span("late", start_time_ns=1, end_time_ns=2)
    assert session.dropped_events == 4


# ------------------------------------------------------------------ renewal bounds


def _drive_renewal(session: Any) -> None:
    """Restart the renewal loop with a test-sized interval (it ticks every 10s)."""
    session.stop_renewal()
    session.start_renewal(interval=0.01)


def test_a_slot_that_cannot_prove_liveness_is_never_renewed() -> None:
    """S3 §5.4: publishing for another process is not a licence to keep it alive."""
    client = FakeManagedClient()
    session = session_module.attach(_managed_context(), client=client)
    session.publish_endpoint(_facts())
    _drive_renewal(session)
    time.sleep(0.2)

    assert client.renewed == []
    session.close()


def test_close_is_not_held_up_by_a_probe_that_never_answers() -> None:
    """A hung liveness probe must not block the training shutdown."""
    client = FakeManagedClient()
    session = session_module.attach(_managed_context(), client=client)
    session.probe_timeout = 30.0
    blocked = threading.Event()
    session.publish_endpoint(
        _facts(), keepalive=lambda: blocked.wait(30.0) and None, renewable=True
    )
    _drive_renewal(session)

    started = time.monotonic()
    report = session.close(deadline_seconds=0.5)
    elapsed = time.monotonic() - started

    assert elapsed < 3.0, f"close waited for the probe: {elapsed:.2f}s"
    assert report.closed is True
    assert session.leases == {}
    assert client.renewed == []


def test_close_spends_its_remaining_budget_on_the_lease_revocation() -> None:
    """S3 §4.3: the revocation is bounded by the same close deadline."""
    client = FakeManagedClient()
    seen: list[float | None] = []

    def slow_delete(
        run_key: str, lease_id: str, payload: Any, *, timeout: float | None = None
    ) -> dict[str, Any]:
        seen.append(timeout)
        if timeout is not None:
            time.sleep(timeout)
        return {"deleted": True, "endpoint_id": "ep-1"}

    client.delete_lease = slow_delete  # type: ignore[method-assign]
    session = session_module.attach(_managed_context(), client=client)
    session.publish_endpoint(_facts())

    started = time.monotonic()
    report = session.close(deadline_seconds=0.3)
    elapsed = time.monotonic() - started

    assert seen and seen[0] is not None and 0 < seen[0] <= 0.3
    assert elapsed < 2.0, f"close outlived its deadline: {elapsed:.2f}s"
    assert report.closed is True


def test_a_closed_session_reports_every_channel_not_ready() -> None:
    session = session_module.attach(_managed_context(), client=FakeManagedClient())
    session.close()

    readiness = session.readiness()
    assert readiness
    for channel in readiness.values():
        assert channel.status == "not_ready"
        assert channel.reason_code == "SESSION_CLOSED"
