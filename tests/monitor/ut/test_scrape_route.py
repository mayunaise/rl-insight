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

"""The controlled scrape route proves ownership on the response it ingests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rl_insight.sdk.identity import (
    EPOCH_HEADER,
    INCARNATION_HEADER,
    RUN_ID_HEADER,
    RunIdentity,
    encode_run_id_header,
)
from rl_insight.sdk.publisher import slot_key
from rl_insight.server.run_registry import RunRegistry
from rl_insight.server.scrape import create_scrape_router

SERVER_INSTANCE = "server-1"


def _facts(**overrides: Any) -> dict[str, Any]:
    facts: dict[str, Any] = {
        "target_kind": "metrics",
        "component": "rollout",
        "instance_key": "replica-0",
        "incarnation": "pid-1",
        "advertised_host": "10.0.0.4",
        "actual_port": 8080,
        "scheme": "http",
        "path": "/metrics",
        "expected_revision": 0,
    }
    facts.update(overrides)
    return facts


class _Response:
    def __init__(
        self, body: bytes = b"metric 1\n", headers: dict[str, str] | None = None
    ):
        self._body = body
        self.headers = headers or {}
        self.closed = False

    def iter_content(self, chunk_size: int):
        for start in range(0, len(self._body), chunk_size):
            yield self._body[start : start + chunk_size]

    def close(self) -> None:
        self.closed = True


class _Session:
    """A requests-like session returning one prepared response."""

    def __init__(
        self, response: Any = None, error: Exception | None = None, on_get=None
    ):
        self.response = response
        self.error = error
        self.on_get = on_get
        self.calls: list[str] = []
        self.last_response: _Response | None = None

    def get(self, url: str, **kwargs: Any) -> Any:
        self.calls.append(url)
        if self.on_get is not None:
            self.on_get()
        if self.error is not None:
            raise self.error
        self.last_response = self.response
        return self.response


def _setup(tmp_path: Path, **facts: Any):
    registry = RunRegistry(
        tmp_path / "registry.sqlite", server_instance_id=SERVER_INSTANCE
    )
    run = registry.create_run(project="proj", idempotency_key="k")
    identity = RunIdentity.from_dict(run["identity"])
    slot = slot_key(identity, "metrics", "rollout", "replica-0")
    published = registry.upsert_endpoint(run["run_key"], slot, _facts(**facts))
    return registry, run, published


def _headers(run: dict[str, Any], published: dict[str, Any]) -> dict[str, str]:
    return {
        RUN_ID_HEADER: encode_run_id_header(run["identity"]["id"]),
        EPOCH_HEADER: str(run["lifecycle_epoch"]),
        INCARNATION_HEADER: "pid-1",
    }


def _client(registry: RunRegistry, session: _Session) -> TestClient:
    app = FastAPI()
    app.include_router(create_scrape_router(registry, session=session))
    return TestClient(app)


def test_a_verified_response_is_returned_and_observed(tmp_path: Path):
    registry, run, published = _setup(tmp_path)
    session = _Session(
        _Response(
            b"metric 1\n", {"content-type": "text/plain", **_headers(run, published)}
        )
    )
    response = _client(registry, session).get(
        f"/internal/scrape/{published['endpoint_id']}"
    )

    assert response.status_code == 200
    assert response.text == "metric 1\n"
    assert session.calls == ["http://10.0.0.4:8080/metrics"]
    # The scrape observation is what makes the health projection claim scrape.
    assert (
        registry.run_health(run["run_key"])["channels"]["metrics_scrape"]["status"]
        == "ready"
    )


def test_a_response_without_the_current_identity_is_refused(tmp_path: Path):
    registry, run, published = _setup(tmp_path)
    stale = {**_headers(run, published), INCARNATION_HEADER: "pid-0"}
    session = _Session(
        _Response(b"metric 1\n", {"content-type": "text/plain", **stale})
    )
    client = _client(registry, session)

    response = client.get(f"/internal/scrape/{published['endpoint_id']}")
    assert response.status_code == 409
    assert response.json()["code"] == "IDENTITY_HEADER_MISMATCH"
    assert registry.run_health(run["run_key"])["channels"]["metrics_scrape"] == {
        "status": "degraded",
        "reason_code": "SCRAPE_NOT_OBSERVED",
    }

    missing = _Session(_Response(b"metric 1\n", {"content-type": "text/plain"}))
    response = _client(registry, missing).get(
        f"/internal/scrape/{published['endpoint_id']}"
    )
    assert response.status_code == 409
    assert response.json()["code"] == "IDENTITY_HEADER_MISMATCH"


def test_an_unknown_or_expired_endpoint_is_refused(tmp_path: Path):
    registry, run, published = _setup(tmp_path)
    client = _client(registry, _Session(_Response()))

    assert client.get("/internal/scrape/ghost").status_code == 404
    registry.delete_lease(
        run["run_key"],
        published["lease_id"],
        {"incarnation": "pid-1", "revision": 1},
    )
    expired = client.get(f"/internal/scrape/{published['endpoint_id']}")
    assert expired.status_code == 410
    assert expired.json()["code"] == "LEASE_EXPIRED"


def test_a_lease_that_moved_during_the_fetch_discards_the_body(tmp_path: Path):
    registry, run, published = _setup(tmp_path)

    def revoke() -> None:
        registry.delete_lease(
            run["run_key"],
            published["lease_id"],
            {"incarnation": "pid-1", "revision": 1},
        )

    session = _Session(
        _Response(
            b"metric 1\n", {"content-type": "text/plain", **_headers(run, published)}
        ),
        on_get=revoke,
    )
    response = _client(registry, session).get(
        f"/internal/scrape/{published['endpoint_id']}"
    )
    assert response.status_code == 410
    assert response.json()["code"] == "LEASE_EXPIRED"


def test_an_oversized_or_unreachable_endpoint_is_a_backend_error(tmp_path: Path):
    registry, run, published = _setup(tmp_path)
    big = _Session(
        _Response(b"x" * 32, {"content-type": "text/plain", **_headers(run, published)})
    )
    app = FastAPI()
    app.include_router(create_scrape_router(registry, session=big, max_bytes=16))
    oversized = TestClient(app).get(f"/internal/scrape/{published['endpoint_id']}")
    assert oversized.status_code == 502
    assert oversized.json()["code"] == "SCRAPE_RESPONSE_TOO_LARGE"
    assert big.last_response is not None and big.last_response.closed is True

    broken = _Session(error=requests.ConnectionError("refused"))
    refused = _client(registry, broken).get(
        f"/internal/scrape/{published['endpoint_id']}"
    )
    assert refused.status_code == 502
    assert refused.json()["code"] == "SCRAPE_BACKEND_UNAVAILABLE"


def test_the_route_never_accepts_a_client_supplied_url(tmp_path: Path):
    registry, run, published = _setup(tmp_path)
    session = _Session(
        _Response(
            b"metric 1\n", {"content-type": "text/plain", **_headers(run, published)}
        )
    )
    # An extra query parameter cannot redirect the fetch: the Registry decides.
    _client(registry, session).get(
        f"/internal/scrape/{published['endpoint_id']}?url=http://evil.example/"
    )
    assert session.calls == ["http://10.0.0.4:8080/metrics"]
