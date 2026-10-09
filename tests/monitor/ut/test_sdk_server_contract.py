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

"""Wire-contract tests: the SDK client against the real run API application.

No Prometheus, Tempo or Grafana is involved, so this is a unit test. It proves
the protocol itself: two same-named runs through one SDK, real HTTP routing,
real Registry state, and a close that only releases its own leases.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rl_insight.archive.writer import ArchiveStore
from rl_insight.sdk import session as session_module
from rl_insight.sdk.publisher import ServerClient
from rl_insight.server.run_api import create_run_router, server_readiness
from rl_insight.server.run_registry import RunRegistry

PREFIX = "/api/v1"
SERVER_INSTANCE = "server-1"


class _BridgedSession(requests.Session):
    """A ``requests`` session whose transports are the in-process ASGI app."""

    def __init__(self, app: FastAPI) -> None:
        super().__init__()
        self.client = TestClient(app)

    def request(self, method: str, url: str, **kwargs: Any):  # type: ignore[override]
        path = url.split(PREFIX, 1)[1]
        headers = dict(kwargs.get("headers") or {})
        response = self.client.request(
            method,
            f"{PREFIX}{path}",
            json=kwargs.get("json"),
            headers=headers,
        )
        response.raise_for_status = _noop  # type: ignore[attr-defined]
        return response


def _noop() -> None:
    return None


@pytest.fixture()
def server(tmp_path: Path):
    app = FastAPI()
    registry = RunRegistry(
        tmp_path / "registry.sqlite", server_instance_id=SERVER_INSTANCE
    )
    store = ArchiveStore(tmp_path / "data")
    app.include_router(
        create_run_router(
            registry,
            store,
            readiness=server_readiness(
                prometheus_enabled=True,
                tempo_enabled=True,
                query_enabled=True,
                archive_dir=tmp_path / "data",
            ),
        ),
        prefix=PREFIX,
    )
    return registry, ServerClient("http://server.invalid", session=_BridgedSession(app))


def _facts(
    host: str = "10.0.0.4", port: int = 8080, **overrides: Any
) -> dict[str, Any]:
    facts: dict[str, Any] = {
        "target_kind": "metrics",
        "component": "rollout",
        "instance_key": "replica-0",
        "incarnation": "pid-1",
        "advertised_host": host,
        "actual_port": port,
        "scheme": "http",
        "path": "/metrics",
        "expected_revision": 0,
    }
    facts.update(overrides)
    return facts


def test_two_runs_named_the_same_go_through_one_sdk_without_mixing(server):
    registry, client = server
    first = session_module.open_session(
        {"project": "proj", "experiment_name": "exp", "idempotency_key": "k1"},
        config={"server.url": "http://server.invalid"},
        client=client,
    )
    second = session_module.open_session(
        {"project": "proj", "experiment_name": "exp", "idempotency_key": "k2"},
        config={"server.url": "http://server.invalid"},
        client=client,
    )

    assert first.identity.id != second.identity.id
    assert first.identity.experiment_name == second.identity.experiment_name == "exp"

    # A worker attaches to the driver's context instead of creating a run
    worker = session_module.attach(first.context.to_json(), client=client)
    assert worker.identity == first.identity

    published = worker.publish_endpoint(_facts())
    assert published.accepted is True
    assert published.endpoint_id and published.lease_id
    assert published.lifecycle_epoch == first.context.lifecycle_epoch

    other = second.publish_endpoint(_facts(host="10.0.0.9"))
    assert other.accepted is True
    assert other.endpoint_id != published.endpoint_id

    # Each run owns exactly one endpoint; the ids stay separate
    assert len(registry.endpoints_for(first.identity.run_key)) == 1
    assert len(registry.endpoints_for(second.identity.run_key)) == 1


def test_closing_one_session_leaves_the_other_run_untouched(server):
    registry, client = server
    first = session_module.open_session(
        {"project": "proj", "idempotency_key": "k1"},
        config={"server.url": "http://server.invalid"},
        client=client,
    )
    second = session_module.open_session(
        {"project": "proj", "idempotency_key": "k2"},
        config={"server.url": "http://server.invalid"},
        client=client,
    )
    first.publish_endpoint(_facts())
    second_lease = second.publish_endpoint(_facts(host="10.0.0.9"))
    assert second_lease.accepted is True

    report = first.close()
    assert report.closed is True
    assert report.errors == ()
    assert report.revoked_leases

    # B keeps its own lease and can still renew it
    renewed = second.publisher.client.renew_lease(
        second.identity.run_key,
        second_lease.lease_id,
        {"incarnation": "pid-1", "revision": 1},
    )
    assert renewed["lease_id"] == second_lease.lease_id
    # and closing a session never archives or deletes the run
    assert registry.get_run(first.identity.run_key)["state"] == "active"
    assert registry.get_run(second.identity.run_key)["state"] == "active"


def test_the_same_endpoint_address_cannot_be_claimed_by_two_runs(server):
    _, client = server
    first = session_module.open_session(
        {"project": "proj", "idempotency_key": "k1"},
        config={"server.url": "http://server.invalid"},
        client=client,
    )
    second = session_module.open_session(
        {"project": "proj", "idempotency_key": "k2"},
        config={"server.url": "http://server.invalid"},
        client=client,
    )
    assert first.publish_endpoint(_facts()).accepted is True
    clash = second.publish_endpoint(_facts())
    assert clash.accepted is False
    assert clash.code == "OWNERSHIP_CONFLICT"


def test_a_stale_incarnation_is_refused_after_a_worker_restart(server):
    _, client = server
    session = session_module.open_session(
        {"project": "proj", "idempotency_key": "k1"},
        config={"server.url": "http://server.invalid"},
        client=client,
    )
    first = session.publish_endpoint(_facts())
    moved = session.publish_endpoint(
        _facts(port=9999, incarnation="pid-2", expected_revision=1)
    )
    assert moved.accepted is True
    assert moved.revision == 2

    # the replaced worker cannot renew its old lease any more
    old = session.publisher.client
    with pytest.raises(Exception) as excinfo:
        old.renew_lease(
            session.identity.run_key,
            first.lease_id,
            {"incarnation": "pid-1", "revision": 1},
        )
    assert getattr(excinfo.value, "code", "") in ("LEASE_EXPIRED", "OWNERSHIP_CONFLICT")
