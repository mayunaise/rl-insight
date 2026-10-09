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

"""Unit tests for the run-scoped HTTP API and discovery payload (S0 §4.3/§5)."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from omegaconf import OmegaConf
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource

from rl_insight.archive.ingress import encode_run_id
from rl_insight.archive.writer import ArchiveStore
from rl_insight.integrations.verl import split_address
from rl_insight.server.http_api import create_app
from rl_insight.server.run_api import (
    CAPABILITIES,
    PROTOCOL_VERSION,
    create_run_router,
    server_readiness,
    services_payload,
)
from rl_insight.server.run_registry import RunRegistry
from rl_insight.utils.monitor_config_loader import load_server_config_file

PREFIX = "/api/v1"
SERVER_INSTANCE = "server-1"


def _readiness(tmp_path: Path) -> dict:
    return server_readiness(
        prometheus_enabled=True,
        tempo_enabled=True,
        query_enabled=True,
        archive_dir=tmp_path / "data",
    )


def _app(tmp_path: Path, **registry_kwargs) -> FastAPI:
    app = FastAPI()
    registry = RunRegistry(
        tmp_path / "registry.sqlite",
        server_instance_id=SERVER_INSTANCE,
        **registry_kwargs,
    )
    app.include_router(
        create_run_router(
            registry,
            ArchiveStore(tmp_path / "data"),
            readiness=_readiness(tmp_path),
        ),
        prefix=PREFIX,
    )
    return app


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    return TestClient(_app(tmp_path))


def _create(client: TestClient, key: str = "key-1", **payload) -> dict:
    body = {"project": "proj", "experiment_name": "exp", **payload}
    return client.post(f"{PREFIX}/runs", json=body, headers={"Idempotency-Key": key})


def _facts(address: str = "http://10.0.0.4:8080/metrics", revision: int = 0) -> dict:
    host, port, path = split_address(address)
    return {
        "target_kind": "metrics",
        "component": "rollout",
        "instance_key": "replica-0",
        "incarnation": "pid-1",
        "advertised_host": host,
        "actual_port": port,
        "scheme": "http",
        "path": path,
        "expected_revision": revision,
    }


def _span_request(run_id: str) -> bytes:
    request = ExportTraceServiceRequest()
    resource_spans = request.resource_spans.add()
    resource_spans.resource.CopyFrom(
        Resource(
            attributes=[
                KeyValue(key="rl_insight.run_id", value=AnyValue(string_value=run_id))
            ]
        )
    )
    resource_spans.scope_spans.add().spans.add(
        trace_id=b"\x01" * 16, span_id=b"\x02" * 8, name="reward"
    )
    return request.SerializeToString()


# --------------------------------------------------------------------------- runs


def test_create_returns_identity_and_run_key(client: TestClient):
    response = _create(client)
    assert response.status_code == 201
    payload = response.json()
    assert payload["identity"]["experiment_name"] == "exp"
    assert payload["state"] == "active"
    assert len(payload["run_key"]) == 64

    retried = _create(client)
    assert retried.status_code == 201
    assert retried.json()["identity"]["id"] == payload["identity"]["id"]


def test_create_with_the_same_key_but_another_body_is_a_conflict(client: TestClient):
    _create(client)
    response = _create(client, project="other")
    assert response.status_code == 409
    assert response.json()["code"] == "IDEMPOTENCY_CONFLICT"


def test_list_and_get_runs_use_the_opaque_run_key(client: TestClient):
    created = _create(client).json()
    listed = client.get(f"{PREFIX}/runs").json()
    assert [row["identity"]["id"] for row in listed["runs"]] == [
        created["identity"]["id"]
    ]

    fetched = client.get(f"{PREFIX}/runs/{created['run_key']}")
    assert fetched.status_code == 200
    assert fetched.json()["identity"]["id"] == created["identity"]["id"]

    missing = client.get(f"{PREFIX}/runs/{'0' * 64}")
    assert missing.status_code == 404
    assert missing.json()["code"] == "RUN_NOT_FOUND"
    assert set(missing.json()) == {
        "code",
        "message",
        "retryable",
        "request_id",
        "details",
    }


def test_capacity_is_a_429_with_the_protocol_shape(tmp_path: Path):
    client = TestClient(_app(tmp_path, active_run_limit=1))
    assert _create(client).status_code == 201
    response = _create(client, key="key-2", experiment_name="other")
    assert response.status_code == 429
    assert response.json()["code"] == "CAPACITY_EXCEEDED"
    assert response.json()["retryable"] is True


def test_resume_checks_project_and_revision(client: TestClient):
    created = _create(client).json()
    key = created["run_key"]
    assert (
        client.post(f"{PREFIX}/runs/{key}/resume", json={"project": "proj"}).status_code
        == 200
    )
    conflict = client.post(f"{PREFIX}/runs/{key}/resume", json={"project": "elsewhere"})
    assert (conflict.status_code, conflict.json()["code"]) == (
        409,
        "OWNERSHIP_CONFLICT",
    )


# --------------------------------------------------------------------------- leases


def test_endpoint_and_lease_lifecycle_over_http(client: TestClient):
    key = _create(client).json()["run_key"]
    published = client.put(f"{PREFIX}/runs/{key}/endpoints/slot-1", json=_facts())
    assert published.status_code == 200
    lease = published.json()
    assert lease["lease_id"] and lease["renew_after"] == 10.0

    renewed = client.post(
        f"{PREFIX}/runs/{key}/leases/{lease['lease_id']}/renew",
        json={"incarnation": "pid-1", "revision": lease["revision"]},
    )
    assert renewed.status_code == 200

    deleted = client.request(
        "DELETE",
        f"{PREFIX}/runs/{key}/leases/{lease['lease_id']}",
        json={"incarnation": "pid-1", "revision": lease["revision"]},
    )
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True


def test_two_runs_cannot_share_one_endpoint_address(client: TestClient):
    first = _create(client, key="key-1").json()
    second = _create(client, key="key-2", experiment_name="other").json()
    assert (
        client.put(
            f"{PREFIX}/runs/{first['run_key']}/endpoints/slot-1", json=_facts()
        ).status_code
        == 200
    )

    conflict = client.put(
        f"{PREFIX}/runs/{second['run_key']}/endpoints/slot-1", json=_facts()
    )
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "OWNERSHIP_CONFLICT"

    # a different address on the second run is fine: the runs stay independent
    moved = client.put(
        f"{PREFIX}/runs/{second['run_key']}/endpoints/slot-1",
        json=_facts(address="http://10.0.0.9:8080/metrics"),
    )
    assert moved.status_code == 200

    # the first run keeps its own endpoint: the two runs never share one
    again = client.put(
        f"{PREFIX}/runs/{first['run_key']}/endpoints/slot-1", json=_facts()
    )
    assert again.status_code == 200
    assert again.json()["endpoint_id"] != moved.json()["endpoint_id"]


# --------------------------------------------------------------------------- discovery


def test_services_payload_keeps_legacy_ports_and_adds_capabilities(tmp_path: Path):
    payload = services_payload(
        server_instance_id=SERVER_INSTANCE,
        readiness=_readiness(tmp_path),
        legacy={"otlp_port": 4318, "prometheus_port": 9090, "grafana_port": 3000},
    )
    assert payload["protocol_version"] == PROTOCOL_VERSION
    assert payload["server_instance_id"] == SERVER_INSTANCE
    assert payload["capabilities"] == list(CAPABILITIES)
    assert payload["services"]["otlp_http"].endswith("/otlp/v1/traces")
    assert payload["otlp_port"] == 4318  # legacy fields stay for old clients
    # Readiness follows the real component configuration, not the run state.
    assert payload["readiness"]["trace_accept"]["status"] == "ready"
    assert payload["readiness"]["archive_metrics"] == {
        "status": "not_ready",
        "reason_code": "REMOTE_WRITE_UNAVAILABLE",
    }
    assert payload["readiness"]["export"] == {
        "status": "not_ready",
        "reason_code": "EXPORT_REQUIRES_BARRIER",
    }
    disabled = server_readiness(
        prometheus_enabled=False,
        tempo_enabled=False,
        query_enabled=False,
        archive_dir=tmp_path / "data",
    )
    assert disabled["metrics_scrape"] == {
        "status": "unsupported",
        "reason_code": "PROMETHEUS_DISABLED",
    }
    assert disabled["trace_online"] == {
        "status": "unsupported",
        "reason_code": "TEMPO_DISABLED",
    }
    assert set(payload["readiness"]) == {
        "registration",
        "metrics_scrape",
        "trace_accept",
        "trace_online",
        "archive_metrics",
        "archive_traces",
        "query",
        "export",
    }


# --------------------------------------------------------------------------- otlp


def test_otlp_batch_is_archived_and_acknowledged(client: TestClient):
    created = _create(client).json()
    run_id = created["identity"]["id"]
    response = client.post(
        f"{PREFIX}/otlp/v1/traces",
        content=_span_request(run_id),
        headers={
            "content-type": "application/x-protobuf",
            "X-RLInsight-Run-ID": encode_run_id(run_id),
            "X-RLInsight-Epoch": str(created["lifecycle_epoch"]),
            "X-RLInsight-Server-Instance": SERVER_INSTANCE,
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-protobuf")
    # The ACK body is the OTLP response, not an ad-hoc JSON envelope.
    acknowledgement = ExportTraceServiceResponse()
    acknowledgement.ParseFromString(response.content)
    assert acknowledgement.partial_success.rejected_spans == 0


def test_otlp_for_an_unknown_run_is_404_and_writes_nothing(client: TestClient):
    response = client.post(
        f"{PREFIX}/otlp/v1/traces",
        content=_span_request("ghost"),
        headers={
            "content-type": "application/x-protobuf",
            "X-RLInsight-Run-ID": encode_run_id("ghost"),
            "X-RLInsight-Epoch": "1",
            "X-RLInsight-Server-Instance": SERVER_INSTANCE,
        },
    )
    assert response.status_code == 404


# --------------------------------------------------------------------------- wiring


def test_the_server_app_mounts_the_run_api_beside_the_legacy_endpoints(tmp_path: Path):
    """The real entry point keeps its legacy routes and gains the run protocol."""
    conf = load_server_config_file(None)
    OmegaConf.update(conf, "server.data_dir", str(tmp_path), merge=True)
    client = TestClient(create_app(conf))

    assert client.get("/healthz").json() == {"status": "ok"}
    services = client.get(f"{PREFIX}/services").json()
    assert services["protocol_version"] == PROTOCOL_VERSION
    assert services["otlp_port"] is not None  # legacy discovery fields stay
    assert services["readiness"]["trace_accept"]["status"] == "ready"
    assert services["services"]["otlp_http"].endswith("/api/v1/otlp/v1/traces")

    created = client.post(
        f"{PREFIX}/runs", json={"project": "proj"}, headers={"Idempotency-Key": "k"}
    )
    assert created.status_code == 201
    run_id = created.json()["identity"]["id"]
    assert client.get(f"{PREFIX}/runs/{created.json()['run_key']}").status_code == 200

    accepted = client.post(
        f"{PREFIX}/otlp/v1/traces",
        content=_span_request(run_id),
        headers={
            "content-type": "application/x-protobuf",
            "X-RLInsight-Run-ID": encode_run_id(run_id),
            "X-RLInsight-Epoch": str(created.json()["lifecycle_epoch"]),
            "X-RLInsight-Server-Instance": created.json()["server_instance_id"],
        },
    )
    assert accepted.status_code == 200
