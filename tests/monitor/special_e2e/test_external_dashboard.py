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

"""Grafana provisioning tests for an external Dashboard directory."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any

import pytest
import requests

GRAFANA_URL = os.environ.get("RL_INSIGHT_GRAFANA_URL", "http://127.0.0.1:3000")
READY_TIMEOUT_SECONDS = 60
ROUTER_DASHBOARD_UID = "uni-agent-router-smoke"
ROUTER_DASHBOARD_TITLE = "Uni-Agent Router Smoke"


def _wait_for_json(
    url: str,
    *,
    params: dict[str, str] | None = None,
    ready: Callable[[Any], bool] = bool,
) -> Any:
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    last_error: Exception | None = None
    last_payload: Any = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(url, params=params, timeout=3)
            response.raise_for_status()
            payload = response.json()
            last_payload = payload
            if ready(payload):
                return payload
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
        time.sleep(1)
    raise AssertionError(
        f"{url} was not ready within {READY_TIMEOUT_SECONDS}s: "
        f"last_error={last_error} last_payload={last_payload}"
    )


def test_external_dashboard_should_be_loaded_by_grafana() -> None:
    detail = _wait_for_json(
        f"{GRAFANA_URL}/api/dashboards/uid/{ROUTER_DASHBOARD_UID}",
        ready=lambda data: data.get("dashboard", {}).get("uid") == ROUTER_DASHBOARD_UID,
    )
    dashboard = detail["dashboard"]
    panel = dashboard["panels"][0]

    assert dashboard["title"] == ROUTER_DASHBOARD_TITLE
    assert panel["title"] == "Uni-Agent Router Requests"
    assert panel["targets"][0]["expr"] == (
        "sum by (route, model) (rl_insight_monitor_uni_agent_router_requests_total)"
    )


@pytest.mark.skipif(
    os.environ.get("RL_INSIGHT_EXPECT_ROUTER_DASHBOARD_ABSENT") != "1",
    reason="runs only after the CI workflow removes the external source and restarts",
)
def test_removed_external_dashboard_should_disappear() -> None:
    search = _wait_for_json(
        f"{GRAFANA_URL}/api/search",
        params={"query": ROUTER_DASHBOARD_TITLE},
        ready=lambda data: all(
            item.get("uid") != ROUTER_DASHBOARD_UID for item in data
        ),
    )
    assert all(item.get("uid") != ROUTER_DASHBOARD_UID for item in search)
