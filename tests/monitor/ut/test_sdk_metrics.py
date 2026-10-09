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

"""Unit tests for the S2 per-session metrics endpoint and lease renewal.

The metrics server binds a real loopback socket, but no Prometheus, Tempo or
Grafana is involved: this is still a unit test. The session tests use a fake
registry client, following ``test_sdk_server_contract.py``.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.request
from typing import Any, Callable

import pytest
from prometheus_client import CollectorRegistry, Counter

from rl_insight.sdk import session as session_module
from rl_insight.sdk.identity import (
    EPOCH_HEADER,
    INCARNATION_HEADER,
    RUN_ID_HEADER,
    RunIdentity,
    decode_run_id_header,
)
from rl_insight.sdk.metrics import IdentityMetricsServer
from rl_insight.sdk.publisher import ProtocolError, slot_key

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before the timeout")


# ------------------------------------------------------------------- metrics server


def test_identity_metrics_server_serves_its_registry_with_identity_headers():
    registry = CollectorRegistry()
    Counter("probe_total", "probe", registry=registry).inc()
    other_registry = CollectorRegistry()
    Counter("other_total", "other", registry=other_registry).inc()

    identity = RunIdentity(
        id="run/A ünïcode", source="managed", project="proj", experiment_name="exp"
    )
    server = IdentityMetricsServer(
        registry, identity=identity, epoch=7, incarnation="inc-1"
    )
    assert server.incarnation == "inc-1"
    server.start()
    try:
        with urllib.request.urlopen(server.url, timeout=5) as response:
            body = response.read().decode("utf-8")
            assert response.status == 200
            assert response.headers["Content-Type"] == CONTENT_TYPE
            assert "probe_total 1.0" in body
            assert "other_total" not in body
            assert response.headers[RUN_ID_HEADER] == "cnVuL0Egw7xuw69jb2Rl"
            assert decode_run_id_header(response.headers[RUN_ID_HEADER]) == identity.id
            assert response.headers[EPOCH_HEADER] == "7"
            assert response.headers[INCARNATION_HEADER] == "inc-1"
        assert server.url == f"http://127.0.0.1:{server.port}/metrics"
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            urllib.request.urlopen(f"http://127.0.0.1:{server.port}/other", timeout=5)
        assert excinfo.value.code == 404
    finally:
        server.stop()

    # stop is idempotent and the port is no longer claimable as running
    server.stop()
    with pytest.raises(RuntimeError):
        _ = server.port


# ------------------------------------------------------------------- session wiring


class FakeClient:
    """ServerClient stand-in that records protocol calls and injected errors."""

    def __init__(self, *, renew_errors: dict[str, ProtocolError] | None = None) -> None:
        self.renew_errors = dict(renew_errors or {})
        self.upserts: list[dict[str, Any]] = []
        self.renews: list[str] = []
        self.deleted: list[str] = []

    def create_run(
        self, *, project: str, experiment_name: str | None, idempotency_key: str
    ) -> dict[str, Any]:
        return {
            "identity": {
                "id": "run-1",
                "source": "managed",
                "project": project or "uncategorized",
                "experiment_name": experiment_name,
            },
            "services": {},
            "server_instance_id": "server-1",
            "lifecycle_epoch": 3,
        }

    def services(self) -> dict[str, Any]:
        return {"services": {}}

    def upsert_endpoint(self, run_key: str, slot: str, payload: Any) -> dict[str, Any]:
        self.upserts.append({"run_key": run_key, "slot": slot, **dict(payload)})
        index = len(self.upserts)
        return {
            "endpoint_id": f"ep-{index}",
            "lease_id": f"lease-{index}",
            "revision": index,
            "lifecycle_epoch": 3,
            "expires_at": 100.0,
            "renew_after": 0.01,
        }

    def renew_lease(self, run_key: str, lease_id: str, payload: Any) -> dict[str, Any]:
        self.renews.append(lease_id)
        error = self.renew_errors.get(lease_id)
        if error is not None:
            raise error
        return {
            "lease_id": lease_id,
            "endpoint_id": "ep-1",
            "revision": payload["revision"],
            "lifecycle_epoch": 3,
            "expires_at": 100.0,
            "renew_after": 0.01,
        }

    def delete_lease(
        self, run_key: str, lease_id: str, payload: Any, *, timeout: float | None = None
    ) -> dict[str, Any]:
        self.deleted.append(lease_id)
        return {"deleted": True, "endpoint_id": "ep-1"}


def _open(client: FakeClient) -> session_module.Session:
    return session_module.open_session(
        {}, config={"server.url": "http://s:1"}, client=client
    )


def test_start_metrics_endpoint_publishes_the_real_bound_port():
    client = FakeClient()
    session = _open(client)
    try:
        endpoint = session.start_metrics_endpoint(
            component="rollout", instance_key="replica-0"
        )
    except BaseException:
        session.close()
        raise
    try:
        assert endpoint.publication.accepted is True
        assert endpoint.publication.lease_id == "lease-1"
        server = endpoint.server
        assert server.port > 0
        published = client.upserts[0]
        assert published["actual_port"] == server.port
        assert published["advertised_host"] == "127.0.0.1"
        assert published["incarnation"] == server.incarnation
        # the incarnation advertised in the facts is the one on the wire
        with urllib.request.urlopen(server.url, timeout=5) as response:
            assert response.headers[INCARNATION_HEADER] == server.incarnation
            assert response.headers[EPOCH_HEADER] == "3"
        assert list(session.leases) == [
            slot_key(session.identity, "metrics", "rollout", "replica-0")
        ]
        assert [item.server for item in session.metrics_endpoints] == [server]
    finally:
        session.close()


def test_a_rejected_publication_is_returned_explicitly_and_keeps_no_socket():
    session = _open(FakeClient())
    try:
        endpoint = session.start_metrics_endpoint(
            advertise_host="0.0.0.0", component="rollout", instance_key="replica-0"
        )
        assert endpoint.publication.accepted is False
        assert endpoint.publication.code == "INVALID_CONFIG"
        assert session.metrics_endpoints == []
        assert session.leases == {}
        with pytest.raises(RuntimeError):
            _ = endpoint.server.port
    finally:
        session.close()


@pytest.mark.parametrize(
    ("code", "retryable", "dropped"),
    [
        ("LEASE_EXPIRED", False, True),
        ("BACKEND_UNAVAILABLE", True, False),
    ],
)
def test_start_renewal_drops_only_non_retryable_leases(
    code: str, retryable: bool, dropped: bool
):
    client = FakeClient(
        renew_errors={
            "lease-2": ProtocolError(code, "renew failed", retryable=retryable)
        }
    )
    session = _open(client)
    try:
        session.start_metrics_endpoint(component="rollout", instance_key="r0")
        session.start_metrics_endpoint(component="rollout", instance_key="r1")
        session.start_renewal(interval=0.01)
        _wait_until(lambda: "lease-1" in client.renews and "lease-2" in client.renews)
        _wait_until(lambda: (len(session.leases) == 1) is dropped)
        session.stop_renewal()
        observed = len(client.renews)
        session.stop_renewal()  # idempotent, and the loop is really gone
        time.sleep(0.05)
        assert len(client.renews) == observed
    finally:
        session.close()


def test_close_stops_its_own_endpoint_and_renewal_and_leaves_other_sessions_alone():
    client_a, client_b = FakeClient(), FakeClient()
    session_a, session_b = _open(client_a), _open(client_b)
    try:
        Counter("a_total", "a", registry=session_a.metrics_registry).inc()
        Counter("b_total", "b", registry=session_b.metrics_registry).inc()
        endpoint_a = session_a.start_metrics_endpoint(
            component="rollout", instance_key="r0"
        )
        endpoint_b = session_b.start_metrics_endpoint(
            component="rollout", instance_key="r0"
        )
        with urllib.request.urlopen(endpoint_a.server.url, timeout=5) as response:
            body_a = response.read()
        with urllib.request.urlopen(endpoint_b.server.url, timeout=5) as response:
            body_b = response.read()
        assert b"a_total" in body_a and b"b_total" not in body_a
        assert b"b_total" in body_b and b"a_total" not in body_b

        session_a.start_renewal(interval=0.01)
        _wait_until(lambda: "lease-1" in client_a.renews)

        report = session_a.close()
        assert report.closed is True
        assert report.revoked_leases == ("lease-1",)
        assert report.errors == ()
        assert client_a.deleted == ["lease-1"]
        assert session_a.metrics_endpoints == []
        with pytest.raises(RuntimeError):
            _ = endpoint_a.server.port

        # the other session still serves and keeps its own lease
        with urllib.request.urlopen(endpoint_b.server.url, timeout=5) as response:
            assert response.status == 200
        assert list(session_b.leases)
        assert client_b.deleted == []

        # close is idempotent and does not revoke again
        assert session_a.close().revoked_leases == ()
        assert client_a.deleted == ["lease-1"]
    finally:
        session_a.close()
        session_b.close()
