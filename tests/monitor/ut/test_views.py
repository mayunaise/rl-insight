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

"""Unit tests for the run list, run page, compare page and view API (S2 §7)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rl_insight.server.grafana import (
    GrafanaClient,
    RunViewService,
    run_datasource_uids,
)
from rl_insight.server.run_registry import RegistryError, RunRegistry
from rl_insight.server.views import create_view_router, run_health

RUN_A = "a" * 32
RUN_B = "b" * 32
RUNS = [
    {
        "run_key": RUN_A,
        "state": "active",
        "revision": 1,
        "lifecycle_epoch": 1,
        "created_at": 1.0,
        "identity": {
            "id": "run-id-11",
            "source": "managed",
            "project": "proj-a",
            "experiment_name": "exp <script>alert(1)</script>",
        },
    },
    {
        "run_key": RUN_B,
        "state": "archived",
        "revision": 1,
        "lifecycle_epoch": 1,
        "created_at": 2.0,
        "identity": {
            "id": "run-id-22",
            "source": "managed",
            "project": "proj-b",
            "experiment_name": "exp-b",
        },
    },
]


class FakeResponse:
    def __init__(self, status_code: int, payload: Any = None, path_url: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = "" if payload is None else str(payload)
        self.request = requests.Request("GET", path_url).prepare()

    def json(self) -> Any:
        return self._payload


class FakeSession:
    """requests-like session with an in-memory Grafana datasource store."""

    def __init__(self, store: dict | None = None):
        self.store = dict(store or {})
        self.calls: list[dict] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        if method == "GET":
            uid = url.rsplit("/", 1)[-1]
            if uid in self.store:
                return FakeResponse(200, self.store[uid], url)
            return FakeResponse(404, None, url)
        payload = kwargs["json"]
        self.store[payload["uid"]] = payload
        return FakeResponse(200, {"id": 1, **payload}, url)

    def writes(self) -> list[str]:
        return [call["method"] for call in self.calls if call["method"] != "GET"]


class FakeRegistry:
    def __init__(self, runs: list[dict] | None = None):
        self.runs = {
            run["run_key"]: run for run in (runs if runs is not None else RUNS)
        }

    def get_run(self, run_key: str) -> dict:
        if run_key not in self.runs:
            raise RegistryError(
                "RUN_NOT_FOUND", f"unknown run key {run_key!r}", status=404
            )
        return self.runs[run_key]

    def list_runs(
        self,
        *,
        project: str | None = None,
        state: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> list[dict]:
        runs = [
            run
            for run in self.runs.values()
            if (project is None or run["identity"]["project"] == project)
            and (state is None or run["state"] == state)
        ]
        return runs[:limit] if limit else runs

    def run_health(self, run_key: str) -> dict:
        run = self.get_run(run_key)
        return {
            "state": run["state"],
            "channels": {
                "registration": {"status": "ready", "reason_code": ""},
                "scrape": {"status": "degraded", "reason_code": "TARGET_MISSING"},
            },
            "updated_at": run["created_at"],
        }


# The real Registry always implements run_health; the fake mirrors it.
FakeHealthRegistry = FakeRegistry


def _app(registry=None, store: dict | None = None, dashboard_uid: str = "board-1"):
    registry = registry if registry is not None else FakeRegistry()
    session = FakeSession(store)
    service = RunViewService(
        registry,
        GrafanaClient("http://grafana.invalid", session=session),
        router_base="http://router.invalid",
        grafana_base="http://grafana.invalid",
        dashboard_uid=dashboard_uid,
    )
    app = FastAPI()
    app.include_router(create_view_router(registry, service))
    return TestClient(app), session


def test_list_page_shows_identity_lifecycle_and_health() -> None:
    client, _ = _app()

    response = client.get("/ui/runs")

    assert response.status_code == 200
    assert "proj-a" in response.text and "proj-b" in response.text
    assert "run-id-1" in response.text and "run-id-2" in response.text
    assert "active" in response.text and "archived" in response.text
    assert "scrape: degraded (TARGET_MISSING)" in response.text
    assert f"href='/ui/runs/{RUN_A}#metrics'" in response.text


def test_health_projection_uses_registry_channels() -> None:
    health = run_health(FakeRegistry(), RUN_A)

    assert health["state"] == "active"
    assert health["channels"]["scrape"] == {
        "status": "degraded",
        "reason_code": "TARGET_MISSING",
    }


def test_view_route_ensures_datasources_and_binds_run() -> None:
    client, session = _app()

    response = client.get(f"/runs/{RUN_A}/view", params={"from_ms": 10, "to_ms": 20})

    assert response.status_code == 200
    body = response.json()
    metrics_uid, traces_uid = run_datasource_uids(RUN_A)
    assert body["status"] == "ready"
    assert body["run_key"] == RUN_A
    assert session.writes() == ["POST", "POST"]
    assert body["metrics_url"].endswith(
        f"?var-metrics_datasource={metrics_uid}&var-traces_datasource={traces_uid}"
        "&from=10&to=20&kiosk"
    )
    assert body["traces_url"] == body["metrics_url"]
    assert body["compare_url"] == f"/ui/compare?left={RUN_A}"


def test_view_route_uses_the_real_registry_errors(tmp_path: Path) -> None:
    registry = RunRegistry(tmp_path / "registry.sqlite", server_instance_id="srv")
    try:
        client, _ = _app(registry)
        assert client.get("/runs/nope/view").status_code == 404
        created = registry.create_run(project="proj-a", experiment_name="exp-a")
        view = client.get(
            f"/runs/{created['run_key']}/view", params={"from_ms": 1, "to_ms": 2}
        )
        assert view.status_code == 200
        assert view.json()["identity"]["id"] == created["identity"]["id"]
    finally:
        registry.close()


def test_run_page_renders_panels_and_health() -> None:
    client, _ = _app(FakeHealthRegistry())

    response = client.get(f"/ui/runs/{RUN_A}")

    assert response.status_code == 200
    assert response.text.count("<iframe") == 2
    assert "TARGET_MISSING" in response.text
    assert "id='metrics'" in response.text and "id='traces'" in response.text
    assert "exp &lt;script&gt;" in response.text


def test_run_page_not_ready_shows_the_reason() -> None:
    metrics_uid, _ = run_datasource_uids(RUN_A)
    client, _ = _app(
        store={
            metrics_uid: {
                "uid": metrics_uid,
                "url": "http://other:9090",
                "type": "prometheus",
            }
        }
    )

    response = client.get(f"/ui/runs/{RUN_A}")

    assert response.status_code == 200
    assert "DATASOURCE_UID_CONFLICT" in response.text
    assert "<iframe" not in response.text


def test_compare_page_renders_two_run_scoped_panels() -> None:
    client, _ = _app(FakeHealthRegistry())

    response = client.get("/ui/compare", params={"left": RUN_A, "right": RUN_B})

    assert response.status_code == 200
    assert response.text.count("<iframe") == 2
    assert "proj-a" in response.text and "proj-b" in response.text
    metrics_a, traces_a = run_datasource_uids(RUN_A)
    metrics_b, traces_b = run_datasource_uids(RUN_B)
    bindings_a = (
        f"var-metrics_datasource={metrics_a}&amp;var-traces_datasource={traces_a}"
    )
    bindings_b = (
        f"var-metrics_datasource={metrics_b}&amp;var-traces_datasource={traces_b}"
    )
    assert bindings_a in response.text
    assert bindings_b in response.text
    windows = re.findall(r"from=(\d+)&amp;to=(\d+)", response.text)
    assert len(windows) > 1 and len(set(windows)) == 1


def test_compare_page_shows_the_reason_when_not_ready() -> None:
    metrics_uid, _ = run_datasource_uids(RUN_B)
    client, _ = _app(
        store={
            metrics_uid: {
                "uid": metrics_uid,
                "url": "http://other",
                "type": "prometheus",
            }
        }
    )

    response = client.get("/ui/compare", params={"left": RUN_A, "right": RUN_B})

    assert response.status_code == 200
    assert response.text.count("<iframe") == 1
    assert "DATASOURCE_UID_CONFLICT" in response.text
