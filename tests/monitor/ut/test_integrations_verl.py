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

"""Unit tests for the verl integration glue (S3 §3–§7).

These tests exercise the glue against the public session API with fake
registry/platform stand-ins and a raw ASGI harness. No Ray, verl, Prometheus or
Tempo process is involved.
"""

from __future__ import annotations

import time
import urllib.request
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from omegaconf import OmegaConf
from prometheus_client import generate_latest

from rl_insight.integrations import verl as glue
from rl_insight.sdk import session as session_module
from rl_insight.sdk.identity import (
    INCARNATION_HEADER,
    RUN_ID_HEADER,
    Context,
    RunIdentity,
    decode_run_id_header,
)
from rl_insight.sdk.session import open_session
from rl_insight.sdk.traces import managed_trace_headers
from rl_insight.server.scrape import create_scrape_router
from rl_insight.utils.constants import PrometheusScrape


class FakeManagedClient:
    """Minimal managed protocol stand-in: endpoints are accepted locally."""

    def __init__(self) -> None:
        self.upserts: list[dict[str, Any]] = []
        self.renewed: list[str] = []
        self.deleted: list[str] = []

    def create_run(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "identity": {
                "id": "run-verl-1",
                "source": "managed",
                "project": kwargs.get("project") or "uncategorized",
                "experiment_name": kwargs.get("experiment_name"),
            },
            "services": {"otlp_http": "http://ingest:4318/v1/traces"},
            "capabilities": ["identity_v1", "session_context_v1"],
            "server_instance_id": "srv-1",
            "lifecycle_epoch": 7,
        }

    def resume_run(self, run_key: str, **kwargs: Any) -> dict[str, Any]:
        return self.create_run(**kwargs)

    def services(self) -> dict[str, Any]:
        return {"services": {}}

    def upsert_endpoint(self, run_key: str, slot: str, payload: Any) -> dict[str, Any]:
        self.upserts.append({"run_key": run_key, "slot": slot, **dict(payload)})
        return {
            "endpoint_id": f"ep-{len(self.upserts)}",
            "lease_id": f"lease-{len(self.upserts)}",
            "revision": len(self.upserts),
            "lifecycle_epoch": 7,
            "expires_at": time.time() + 30,
            "renew_after": 30,
        }

    def renew_lease(self, run_key: str, lease_id: str, payload: Any) -> dict[str, Any]:
        self.renewed.append(lease_id)
        return {"lease_id": lease_id, "revision": payload["revision"]}

    def delete_lease(
        self, run_key: str, lease_id: str, payload: Any, *, timeout: float | None = None
    ) -> dict[str, Any]:
        self.deleted.append(lease_id)
        return {"deleted": True, "endpoint_id": "ep-1"}


@pytest.fixture(autouse=True)
def clean_process_registry():
    """The process registry is process-wide; keep tests independent."""
    with glue._PROCESS_LOCK:
        saved = dict(glue._PROCESS_SESSIONS)
        glue._PROCESS_SESSIONS.clear()
    yield
    with glue._PROCESS_LOCK:
        glue._PROCESS_SESSIONS.clear()
        glue._PROCESS_SESSIONS.update(saved)


def _managed_session(client: FakeManagedClient | None = None):
    return open_session(
        {"project": "proj", "experiment_name": "exp"},
        config={"server.url": "http://rl-insight:8080"},
        client=client or FakeManagedClient(),
    )


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before the timeout")


def _drive_renewal(session) -> None:
    """Restart the renewal loop with a test-sized interval (it ticks every 10s)."""
    session.stop_renewal()
    session.start_renewal(interval=0.01)


def _context_json(run_id: str = "run-verl-1", source: str = "managed") -> str:
    return Context(
        identity=RunIdentity(id=run_id, source=source, project="proj"),
        services={
            "server": "http://rl-insight:8080",
            "otlp_http": "http://ingest/v1/traces",
        },
        publisher={"adapter": "managed"},
        lifecycle_epoch=7,
    ).to_json()


def test_open_driver_session_registers_and_exports_context(monkeypatch):
    config = OmegaConf.create(
        {
            "trainer": {
                "project_name": "proj",
                "experiment_name": "exp",
                "rl_insight": {"server.url": "http://rl-insight:8080"},
            }
        }
    )
    client = FakeManagedClient()
    monkeypatch.setattr(session_module, "ServerClient", lambda *args, **kwargs: client)

    driver = glue.open_driver_session(config)

    assert driver.enabled
    assert driver.identity.id == "run-verl-1"
    assert glue.process_session(driver.context_json) is driver.session
    # The context carries the run identity, not the local deployment config.
    assert "run-verl-1" in driver.context_json
    assert driver.close()["closed"] is True
    assert glue.process_session(driver.context_json) is None


def test_attach_derives_mode_and_identity_from_context(monkeypatch):
    monkeypatch.setenv("RL_INSIGHT_SERVER_URL", "http://other-server:9999")
    context_json = _context_json()

    worker = glue.attach(context_json, component="trainer")

    assert worker is not None
    assert worker.identity.id == "run-verl-1"
    assert worker.config.mode == "managed"
    # The worker's conflicting environment only produces a diagnostic.
    assert worker.config.server_url == "http://rl-insight:8080"
    assert any("RL_INSIGHT_SERVER_URL" in item for item in worker.config.diagnostics)
    assert glue.process_session(context_json) is worker
    glue.close(worker)


def test_attach_from_rollout_config_uses_declared_component():
    rollout = OmegaConf.create({"name": "vllm"})
    glue.write_rollout_context(
        rollout, _context_json(), component="actor_rollout_ref.rollout"
    )

    assert glue.read_rollout_context(rollout) == _context_json()
    assert glue.read_rollout_component(rollout) == "actor_rollout_ref.rollout"

    session = glue.attach_rollout_session(rollout)
    assert session is not None
    glue.close(session)


def test_context_round_trips_through_omegaconf_struct_config():
    config = OmegaConf.create({"trainer": {"project_name": "proj"}})
    OmegaConf.set_struct(config, True)

    glue.write_trainer_context(config, _context_json())

    assert glue.read_trainer_context(config) == _context_json()


def test_log_scalars_writes_into_the_session_registry():
    session = _managed_session()

    written = glue.log_scalars(
        session, {"reward/mean": 1.25, "step": 3, "bad": object()}
    )

    assert written == 2
    body = generate_latest(session.metrics_registry).decode("utf-8")
    assert "rl_insight_monitor_reward_mean 1.25" in body
    assert "rl_insight_monitor_step 3.0" in body
    glue.close(session)


def test_trace_state_merges_lane_and_drops_shadowed(monkeypatch):
    session = _managed_session()
    spans: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        session_module.Session,
        "trace_span",
        lambda self, name, *, start_time_ns, end_time_ns, attributes=None: spans.append(
            (name, dict(attributes or {}))
        ),
    )

    with glue.trace_state(None, "not-recorded"):
        pass
    with glue.trace_state(session, "rollout", state_lane_id="replica_0"):
        with glue.trace_state(
            session, "rollout", state_lane_id="replica_0", extra="kept"
        ):
            with glue.trace_state(session, "sync_weights", state_lane_id="replica_0"):
                pass  # shadowed by the occupied lane, never reported

    assert [name for name, _ in spans] == ["rollout"]
    attributes = spans[0][1]
    assert attributes["monitor.trace_segment"] == "state_interval"
    assert attributes["state_name"] == "rollout"
    assert attributes["state_lane_id"] == "replica_0"
    glue.close(session)


def test_publish_metrics_endpoint_uses_bound_address_and_renews():
    client = FakeManagedClient()
    session = _managed_session(client)

    result = glue.publish_metrics_endpoint(
        session,
        component="actor_rollout_ref.rollout/vllm",
        instance_key="replica/0",
        advertised_host="10.0.0.5",
        actual_port=32100,
        incarnation="inc-a",
        replica=0,
    )

    assert result.accepted
    request = client.upserts[0]
    assert request["advertised_host"] == "10.0.0.5"
    assert request["actual_port"] == 32100
    assert request["incarnation"] == "inc-a"
    assert request["replica"] == "0"
    assert request["path"] == "/metrics"
    assert session._renewal_thread is not None
    glue.close(session)


def test_publish_metrics_endpoint_without_session_reports_the_gap():
    result = glue.publish_metrics_endpoint(
        None,
        component="rollout",
        instance_key="replica/0",
        advertised_host="10.0.0.5",
        actual_port=32100,
    )

    assert not result.accepted
    assert result.code == "NO_SESSION"


class _ScrapeTargetRegistry:
    """The two Registry calls the controlled scrape route makes."""

    def __init__(self, target: dict[str, Any]) -> None:
        self.target = target
        self.scraped: list[str] = []

    def scrape_target(self, endpoint_id: str) -> dict[str, Any]:
        return self.target

    def note_scrape(self, endpoint_id: str, incarnation: str) -> None:
        self.scraped.append(incarnation)


def test_the_trainer_serves_the_registry_its_scalars_are_logged_into():
    """S3 §5.2: a logged scalar reaches the body the controlled scrape ingests.

    The scrape route is the reader that checks the identity headers on the same
    response it ingests, so going through it — not the exporter directly — is
    what proves the trainer's endpoint is usable. A second session stands for
    the driver: it has its own registry, and serving it would yield an empty
    body.
    """
    client = FakeManagedClient()
    session = _managed_session(client)
    driver = _managed_session(client)

    endpoint = glue.serve_trainer_metrics(session, advertise_host="127.0.0.1")

    assert endpoint is not None
    assert endpoint.publication.accepted is True
    assert client.upserts[0]["component"] == "trainer"
    assert client.upserts[0]["advertised_host"] == "127.0.0.1"
    assert client.upserts[0]["actual_port"] == endpoint.server.port
    assert driver.metrics_registry is not session.metrics_registry

    registry = _ScrapeTargetRegistry(
        {
            "address": endpoint.server.url,
            "run_id": "run-verl-1",
            "lifecycle_epoch": 7,
            "incarnation": endpoint.server.incarnation,
            "lease_id": endpoint.publication.lease_id,
            "revision": endpoint.publication.revision,
        }
    )
    app = FastAPI()
    app.include_router(create_scrape_router(registry))
    scrape_url = f"{PrometheusScrape.SCRAPE_ROUTE}/{endpoint.publication.endpoint_id}"
    before = TestClient(app).get(scrape_url)
    assert before.status_code == 200
    assert "train_reward" not in before.text

    assert glue.log_scalars(session, {"train/reward": 3.5}, step=1) == 1

    scraped = TestClient(app).get(scrape_url)
    assert scraped.status_code == 200
    assert "train_reward 3.5" in scraped.text
    assert registry.scraped == [endpoint.server.incarnation] * 2
    # the identity headers the route accepted are the ones the exporter stamps
    with urllib.request.urlopen(endpoint.server.url, timeout=5) as response:
        headers = {key.lower(): value for key, value in response.headers.items()}
    assert decode_run_id_header(headers[RUN_ID_HEADER.lower()]) == "run-verl-1"
    assert headers[INCARNATION_HEADER.lower()] == endpoint.server.incarnation
    glue.close(session)
    glue.close(driver)


def test_serve_trainer_metrics_without_a_session_publishes_nothing():
    assert glue.serve_trainer_metrics(None) is None


def test_publish_transfer_queue_endpoint_renews_only_while_the_controller_answers():
    """S3 §5.4: the driver proxies the lease, so it renews only while it can prove the Controller is alive."""
    client = FakeManagedClient()
    session = _managed_session(client)
    probe = {"address": "10.0.0.9:9101"}

    result = glue.publish_transfer_queue_endpoint(
        session,
        "10.0.0.9:9101",
        probe=lambda: probe["address"],
        identity_headers_stamped=True,
    )

    assert result.accepted
    _drive_renewal(session)
    _wait_until(lambda: "lease-1" in client.renewed)

    # The Controller actor is unreachable: the probe stops answering.
    probe["address"] = None
    _wait_until(lambda: session.leases == {})
    sustained = len(client.renewed)
    time.sleep(0.05)
    assert len(client.renewed) == sustained
    glue.close(session)


def test_publish_transfer_queue_endpoint_replaces_a_rebound_controller():
    """A Controller that rebinds publishes its new address in the same slot."""
    client = FakeManagedClient()
    session = _managed_session(client)
    probe = {"address": "10.0.0.9:9101"}
    glue.publish_transfer_queue_endpoint(
        session,
        "10.0.0.9:9101",
        probe=lambda: probe["address"],
        identity_headers_stamped=True,
    )

    _drive_renewal(session)
    probe["address"] = "10.0.0.9:9202"
    _wait_until(lambda: len(client.upserts) == 2)

    replacement = client.upserts[1]
    assert replacement["actual_port"] == 9202
    # the replacement is conditional on the revision the driver holds
    assert replacement["expected_revision"] == 1
    assert replacement["instance_key"] == client.upserts[0]["instance_key"]
    glue.close(session)


def test_a_transfer_queue_exporter_without_identity_headers_is_not_published():
    """S2 §4.2: a target whose response cannot prove the run is not registered."""
    client = FakeManagedClient()
    session = _managed_session(client)

    result = glue.publish_transfer_queue_endpoint(
        session, "10.0.0.9:9101", probe=lambda: "10.0.0.9:9101"
    )

    assert not result.accepted
    assert result.code == "IDENTITY_HEADERS_UNSUPPORTED"
    assert client.upserts == []
    assert session.leases == {}
    glue.close(session)


def test_publish_transfer_queue_endpoint_without_endpoint():
    session = _managed_session()

    result = glue.publish_transfer_queue_endpoint(session, None)

    assert not result.accepted
    assert result.code == "NO_ENDPOINT"
    glue.close(session)


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        ("10.0.0.9:9101", ("10.0.0.9", 9101, "/metrics")),
        ("http://10.0.0.9:9101/metrics", ("10.0.0.9", 9101, "/metrics")),
        ("http://[fd00::1]:9101/custom", ("fd00::1", 9101, "/custom")),
    ],
)
def test_split_address(address, expected):
    assert glue.split_address(address) == expected


class _App:
    """Tiny ASGI app whose /metrics response carries a business identity header."""

    def __init__(self) -> None:
        self.middlewares: list[tuple[type, dict[str, Any]]] = []

    def add_middleware(self, middleware_class: type, **kwargs: Any) -> None:
        self.middlewares.append((middleware_class, kwargs))

    async def __call__(self, scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"text/plain"),
                    (RUN_ID_HEADER.lower().encode(), b"business-value"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": b"metric 1\n"})


async def _call(app: Any, path: str) -> dict[str, str]:
    messages: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b""}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    await app({"type": "http", "path": path, "headers": []}, receive, send)
    start = next(item for item in messages if item["type"] == "http.response.start")
    return {
        name.decode("latin-1").lower(): value.decode("latin-1")
        for name, value in start["headers"]
    }


def test_identity_middleware_replaces_business_header_on_metrics_only():
    from rl_insight.sdk.metrics import IdentityHeaderMiddleware, identity_headers

    identity = RunIdentity(id="run-verl-1", source="managed", project="proj")
    headers = identity_headers(identity, epoch=7, incarnation="inc-a")
    app = IdentityHeaderMiddleware(_App(), headers=headers, path="/metrics")

    import asyncio

    metrics_headers = asyncio.run(_call(app, "/metrics"))
    other_headers = asyncio.run(_call(app, "/v1/completions"))

    assert metrics_headers[RUN_ID_HEADER.lower()] != "business-value"
    assert metrics_headers[INCARNATION_HEADER.lower()] == "inc-a"
    assert other_headers[RUN_ID_HEADER.lower()] == "business-value"


def test_install_metrics_identity_records_the_owned_instance():
    session = _managed_session()
    app = _App()

    assert glue.install_metrics_identity(app, session, incarnation="inc-a") is True

    middleware_class, kwargs = app.middlewares[0]
    assert middleware_class.__name__ == "IdentityHeaderMiddleware"
    assert kwargs["headers"][INCARNATION_HEADER] == "inc-a"
    assert kwargs["path"] == "/metrics"
    glue.close(session)


class _StartedApp:
    """A framework app whose middleware stack was already built (vLLM + SageMaker)."""

    def __init__(self) -> None:
        self.inner = _App()
        self.middleware_stack = self.build_middleware_stack()

    def add_middleware(self, middleware_class: type, **kwargs: Any) -> None:
        raise RuntimeError("Cannot add middleware after an application has started")

    def build_middleware_stack(self) -> _App:
        return self.inner

    async def __call__(self, scope, receive, send):
        await self.middleware_stack(scope, receive, send)


def test_an_already_started_app_still_gets_the_identity_on_metrics():
    import asyncio

    session = _managed_session()
    app = _StartedApp()

    assert glue.install_metrics_identity(app, session, incarnation="inc-a") is True

    headers = asyncio.run(_call(app, "/metrics"))

    assert headers[INCARNATION_HEADER.lower()] == "inc-a"
    assert headers[RUN_ID_HEADER.lower()] != "business-value"
    glue.close(session)


def test_managed_trace_headers_travel_with_the_context():
    # The context the glue hands to the rollout actors keeps the managed
    # identity headers the OTLP ingress route verifies.
    context = Context.from_json(_context_json())

    headers = managed_trace_headers(context)

    assert headers[RUN_ID_HEADER]
