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

"""Unit tests for per-run Grafana datasources and view links (S2 §7)."""

from __future__ import annotations

from typing import Any

import pytest
import requests

from rl_insight.server.grafana import (
    UID_MAX_CHARS,
    GrafanaClient,
    RunViewService,
    run_datasource_uids,
)
from rl_insight.server.run_registry import RegistryError

RUN_KEY = "0123456789abcdef0123456789abcdef"
RUN = {
    "run_key": RUN_KEY,
    "state": "active",
    "created_at": 1.0,
    "identity": {
        "id": "run-id-1",
        "source": "managed",
        "project": "proj",
        "experiment_name": "exp",
    },
}


class FakeResponse:
    def __init__(self, status_code: int, payload: Any = None, path_url: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = "" if payload is None else str(payload)
        self.request = requests.Request("GET", path_url).prepare()

    def json(self) -> Any:
        return self._payload


class FakeSession:
    """Records every call; reads/writes an in-memory datasource store."""

    def __init__(self, store: dict | None = None, error: Exception | None = None):
        self.store = dict(store or {})
        self.error = error
        self.calls: list[dict] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        if self.error is not None:
            raise self.error
        if method == "GET":
            uid = url.rsplit("/", 1)[-1]
            if uid in self.store:
                return FakeResponse(200, self.store[uid], url)
            return FakeResponse(404, None, url)
        if method in ("POST", "PUT"):
            payload = kwargs["json"]
            self.store[payload["uid"]] = payload
            return FakeResponse(200, {"id": 1, **payload}, url)
        return FakeResponse(500, {"message": "unexpected"}, url)

    def writes(self) -> list[str]:
        return [call["method"] for call in self.calls if call["method"] != "GET"]


class FakeRegistry:
    def __init__(self, runs: list[dict]):
        self.runs = {run["run_key"]: run for run in runs}

    def get_run(self, run_key: str) -> dict:
        if run_key not in self.runs:
            raise RegistryError(
                "RUN_NOT_FOUND", f"unknown run key {run_key!r}", status=404
            )
        return self.runs[run_key]

    def list_runs(self) -> list[dict]:
        return list(self.runs.values())


def _service(
    session: FakeSession | None = None, *, dashboard_uid: str = "board-1"
) -> tuple[RunViewService, FakeSession]:
    session = session if session is not None else FakeSession()
    client = GrafanaClient(
        "http://grafana.invalid", token="tok", session=session, timeout=1.0
    )
    return (
        RunViewService(
            FakeRegistry([RUN]),
            client,
            router_base="http://router.invalid/",
            grafana_base="http://grafana.invalid/",
            dashboard_uid=dashboard_uid,
        ),
        session,
    )


def test_run_datasource_uids_are_deterministic_and_short() -> None:
    metrics, traces = run_datasource_uids(RUN_KEY)

    assert (metrics, traces) == run_datasource_uids(RUN_KEY)
    assert metrics == "rlrun-0123456789ab-prom"
    assert traces == "rlrun-0123456789ab-tempo"
    assert metrics != traces
    assert max(len(metrics), len(traces)) <= UID_MAX_CHARS
    assert run_datasource_uids("ffffffffffffffff")[0] != metrics


def test_ensure_run_view_creates_both_datasources_idempotently() -> None:
    service, session = _service()

    view = service.ensure_run_view(RUN_KEY, from_ms=1000, to_ms=2000)

    metrics_uid, traces_uid = run_datasource_uids(RUN_KEY)
    assert view["status"] == "ready"
    assert view["reason_code"] == ""
    assert view["identity"] == RUN["identity"]
    assert (view["metrics_uid"], view["traces_uid"]) == (metrics_uid, traces_uid)
    assert [call["method"] for call in session.calls] == ["GET", "POST", "GET", "POST"]
    assert session.store[metrics_uid] == {
        "name": "RL-Insight proj/exp (run-id-1) Prometheus",
        "uid": metrics_uid,
        "type": "prometheus",
        "access": "proxy",
        "url": f"http://router.invalid/api/v1/runs/{RUN_KEY}/prometheus",
        "editable": False,
        "jsonData": {"httpMethod": "POST"},
    }
    assert session.store[traces_uid]["type"] == "tempo"
    assert session.store[traces_uid]["name"] == "RL-Insight proj/exp (run-id-1) Tempo"
    assert session.store[traces_uid]["url"].endswith(f"/api/v1/runs/{RUN_KEY}/tempo")
    assert view["compare_url"] == f"/ui/compare?left={RUN_KEY}"

    session.calls.clear()
    assert service.ensure_run_view(RUN_KEY)["status"] == "ready"
    assert session.calls == []


def test_two_runs_sharing_project_and_name_get_distinct_datasource_names() -> None:
    # Grafana rejects a datasource whose name is already taken, so two concurrent
    # runs that legitimately share project/experiment_name must still get a view.
    other_key = "f" * 32
    other = {
        "run_key": other_key,
        "state": "active",
        "created_at": 2.0,
        "identity": {**RUN["identity"], "id": "run-id-2"},
    }
    session = FakeSession()
    service = RunViewService(
        FakeRegistry([RUN, other]),
        GrafanaClient(
            "http://grafana.invalid", token="tok", session=session, timeout=1.0
        ),
        router_base="http://router.invalid/",
        grafana_base="http://grafana.invalid/",
        dashboard_uid="board-1",
    )

    assert service.ensure_run_view(RUN_KEY)["status"] == "ready"
    assert service.ensure_run_view(other_key)["status"] == "ready"

    names = [payload["name"] for payload in session.store.values()]
    assert len(names) == len(set(names)) == 4


def test_uid_conflict_is_reported_and_never_overwritten() -> None:
    metrics_uid, _ = run_datasource_uids(RUN_KEY)
    foreign = {"uid": metrics_uid, "url": "http://other:9090", "type": "prometheus"}
    service, session = _service(FakeSession({metrics_uid: foreign}))

    view = service.ensure_run_view(RUN_KEY)

    assert view["status"] == "not_ready"
    assert view["reason_code"] == "DATASOURCE_UID_CONFLICT"
    assert session.writes() == []
    assert session.store[metrics_uid] == foreign
    # The failed run is retried instead of being remembered as ensured.
    assert service._ensured == set()


def test_datasource_failure_only_makes_the_view_not_ready() -> None:
    service, _ = _service(FakeSession(error=requests.ConnectionError("down")))

    view = service.ensure_run_view(RUN_KEY)

    assert view["status"] == "not_ready"
    assert view["reason_code"] == "GRAFANA_UNREACHABLE"
    assert view["metrics_uid"] and view["traces_uid"]
    assert view["metrics_url"]


def test_links_bind_both_run_uids_and_time_range() -> None:
    service, _ = _service()
    metrics_uid, traces_uid = run_datasource_uids(RUN_KEY)
    expected = (
        f"http://grafana.invalid/d/board-1?var-metrics_datasource={metrics_uid}"
        f"&var-traces_datasource={traces_uid}&from=1000&to=2000&kiosk"
    )

    view = service.ensure_run_view(RUN_KEY, from_ms=1000, to_ms=2000)

    assert view["metrics_url"] == expected
    assert view["traces_url"] == expected


def test_missing_dashboard_stays_ready_with_reason_code() -> None:
    service, _ = _service(dashboard_uid="")

    view = service.ensure_run_view(RUN_KEY)

    assert view["status"] == "ready"
    assert view["reason_code"] == "DASHBOARD_NOT_CONFIGURED"
    assert (view["metrics_url"], view["traces_url"]) == ("", "")


def test_unknown_run_raises_registry_error() -> None:
    service, session = _service()

    with pytest.raises(RegistryError) as excinfo:
        service.ensure_run_view("nope")

    assert excinfo.value.code == "RUN_NOT_FOUND"
    assert excinfo.value.status == 404
    assert session.calls == []
