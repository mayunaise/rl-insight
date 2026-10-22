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

"""Unit tests for the RL-Insight server HTTP API."""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock

from omegaconf import OmegaConf

from rl_insight.server.http_api import create_app
from rl_insight.utils.prometheus_utils import PrometheusTargetStore


def test_register_targets_should_persist_file_sd_targets_and_reload_prometheus(
    monkeypatch, tmp_path
) -> None:
    conf = OmegaConf.create(
        {
            "server": {
                "runtime_dir": str(tmp_path / "runtime"),
                "data_dir": str(tmp_path / "data"),
            },
            "prometheus": {"prometheus_port": 9090},
        }
    )

    reload_prometheus = MagicMock(return_value=True)
    monkeypatch.setattr(PrometheusTargetStore, "reload", reload_prometheus)
    app = create_app(conf)
    endpoint = next(
        cast(Any, route).endpoint
        for route in app.routes
        if getattr(route, "path", "") == "/api/v1/prometheus/targets"
    )

    result = endpoint(
        {
            "job_name": "node-exporter",
            "targets": ["node-a:9100"],
        }
    )

    assert result["status"] == "ok"
    assert result["prometheus_reloaded"] is True
    reload_prometheus.assert_called_once_with()
    assert (tmp_path / "data" / "targets" / "prometheus-targets.yml").exists()


def test_run_registration_preserves_caller_identity_and_retries_are_idempotent(
    tmp_path,
):
    from fastapi.testclient import TestClient

    from rl_insight import __version__

    conf = OmegaConf.create(
        {
            "server": {"data_dir": str(tmp_path)},
            "grafana": {"enable": False},
            "prometheus": {"prometheus_port": 9090},
        }
    )
    payload = {
        "run_id": "20261021T083025Z_" + "a" * 32,
        "project": "p",
        "experiment_name": "same",
    }
    with TestClient(create_app(conf)) as client:
        first = client.post("/api/v1/runs", json=payload)
        assert first.status_code == 200
        assert first.json()["run_id"] == payload["run_id"]
        assert first.json()["version"] == __version__
        assert client.post("/api/v1/runs", json=payload).json() == first.json()
        assert (
            client.post(
                "/api/v1/runs", json={**payload, "project": "other"}
            ).status_code
            == 409
        )
    with TestClient(create_app(conf)) as client:
        assert (
            client.get("/api/v1/runs/" + payload["run_id"]).json()["context"]["run_id"]
            == payload["run_id"]
        )


def test_fixed_query_gateway_scopes_every_selector_to_selected_runs(
    monkeypatch, tmp_path
):
    import json

    from fastapi.testclient import TestClient

    from rl_insight.server import http_api

    calls = []

    def backend(url, fields, **kwargs):
        calls.append((url, fields))
        return 200, json.dumps(
            {"status": "success", "data": {"resultType": "vector", "result": []}}
        ).encode()

    monkeypatch.setattr(http_api, "read_backend", backend)
    conf = OmegaConf.create(
        {
            "server": {"data_dir": str(tmp_path)},
            "grafana": {"enable": False},
            "prometheus": {"prometheus_port": 9090},
        }
    )
    ids = ["20261021T083025Z_" + c * 32 for c in "abc"]
    with TestClient(create_app(conf)) as client:
        for run_id in ids:
            assert (
                client.post(
                    "/api/v1/runs",
                    json={"run_id": run_id, "project": "p", "experiment_name": "same"},
                ).status_code
                == 200
            )
        expression = 'rate(tokens{run_id=~"(' + "|".join(ids[:2]) + ')"}[1m]) + reward'
        response = client.get(
            "/api/v1/query/prometheus/api/v1/query", params={"query": expression}
        )
        assert response.status_code == 200, response.text
        query = dict(calls[-1][1])["query"]
        assert query.count("run_id=~") == 2
        assert all(run_id in query for run_id in ids[:2]) and ids[2] not in query
        for expression in ("reward", 'reward{run_id=~".*"}', 'reward{run_id!=""}'):
            assert client.get(
                "/api/v1/query/prometheus/api/v1/query", params={"query": expression}
            ).status_code in {400, 422}
        conflicting = (
            'reward{run_id="' + ids[0] + '"} + reward{run_id="' + ids[1] + '"}'
        )
        assert (
            client.get(
                "/api/v1/query/prometheus/api/v1/query", params={"query": conflicting}
            ).status_code
            == 403
        )


def test_tempo_tag_values_and_step_projection_keep_selected_run(monkeypatch, tmp_path):
    import json

    from fastapi.testclient import TestClient

    from rl_insight.server import http_api

    calls = []
    owner = "20261021T083025Z_" + "a" * 32
    context = http_api.RunContext(owner, "p", "same", "http://server")
    timeline = http_api.trace_search(
        '{span."monitor.trace_segment"="state_interval" && span.state_lane_id!=""} | select(span.state_lane_id)',
        context,
    )
    assert "metric.name" not in timeline and "metric.value" not in timeline
    assert "global_step" not in timeline and "span.project" not in timeline

    def backend(url, fields, **kwargs):
        calls.append((url, dict(fields)))
        if url.endswith("/values"):
            return 200, b'{"tagValues":[{"type":"string","value":"loss"}]}'
        return 200, json.dumps(
            {
                "traces": [
                    {
                        "traceID": "1" * 32,
                        "spanSets": [
                            {
                                "matched": 1,
                                "spans": [
                                    {
                                        "name": "metric_log",
                                        "startTimeUnixNano": "1",
                                        "attributes": [
                                            {
                                                "key": "rl_insight.run_id",
                                                "value": {"stringValue": owner},
                                            },
                                            {
                                                "key": "metric.name",
                                                "value": {"stringValue": "loss"},
                                            },
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        ).encode()

    monkeypatch.setattr(http_api, "read_backend", backend)
    conf = OmegaConf.create(
        {"server": {"data_dir": str(tmp_path)}, "prometheus": {"prometheus_port": 9090}}
    )
    with TestClient(create_app(conf)) as client:
        assert (
            client.post(
                "/api/v1/runs",
                json={"run_id": owner, "project": "p", "experiment_name": "same"},
            ).status_code
            == 200
        )
        expression = (
            '{resource.rl_insight.run_id="' + owner + '" && span."metric.name" != nil}'
        )
        base = "/api/v1/query/tempo/api/"
        assert client.get(base + "v2/search/tags").json() == {"scopes": []}
        response = client.get(
            base + "v2/search/tag/span.metric.name/values", params={"q": expression}
        )
        assert response.status_code == 200, response.text
        assert response.json()["tagValues"][0]["value"] == "loss"
        assert owner in calls[-1][1]["q"] and "select(" not in calls[-1][1]["q"]
        response = client.get(
            "/api/v1/catalog/prometheus/api/v1/label/metric_name/values",
            params={
                "match[]": '{run_id=~"' + owner + '"}',
                "start": "1792600000.5",
                "end": "1792600300.5",
            },
        )
        assert response.json() == {"status": "success", "data": ["loss"]}
        assert owner in calls[-1][1]["q"]
        assert calls[-1][1]["start"] == "1792600000"
        assert calls[-1][1]["end"] == "1792600300"
        count = len(calls)
        assert (
            client.get(
                "/api/v1/catalog/prometheus/api/v1/label/metric_name/values"
            ).status_code
            == 422
        )
        assert (
            client.get(
                "/api/v1/catalog/prometheus/api/v1/label/metric_name/values",
                params={"match[]": '{run_id=~""}'},
            ).json()["data"]
            == []
        )
        assert len(calls) == count
        response = client.get(base + "search", params={"q": expression})
        assert response.status_code == 200, response.text
        assert (
            'span."metric.value"' in calls[-1][1]["q"]
            and "span.global_step" in calls[-1][1]["q"]
        )
        assert (
            response.json()["traces"][0]["spanSet"]["spans"][0]["durationNanos"] == "0"
        )
        assert (
            response.json()["traces"][0]["spanSet"]["spans"][0]["attributes"][0]["key"]
            == "rl_insight.run_id"
        )


def test_multirun_timeline_uses_run_lanes_without_plotting_identity_metadata():
    from rl_insight.server.http_api import project_search
    from rl_insight.utils.protocol import RunContext

    contexts = [RunContext(c * 32, "p", "same", "http://server") for c in "ab"]
    traces = []
    for context in contexts:
        traces.append(
            {
                "traceID": context.run_id,
                "spanSet": {
                    "matched": 1,
                    "spans": [
                        {
                            "name": "actor_update",
                            "startTimeUnixNano": "1",
                            "durationNanos": "10",
                            "attributes": [
                                {
                                    "key": "rl_insight.run_id",
                                    "value": {"stringValue": context.run_id},
                                },
                                {
                                    "key": "state_lane_id",
                                    "value": {"stringValue": "rank_0"},
                                },
                                {
                                    "key": "project",
                                    "value": {"stringValue": context.project},
                                },
                                {
                                    "key": "experiment_name",
                                    "value": {"stringValue": context.experiment_name},
                                },
                                {
                                    "key": "monitor.trace_segment",
                                    "value": {"stringValue": "state_interval"},
                                },
                                {"key": "global_step", "value": {"intValue": "3"}},
                                {"key": "metric.value", "value": {"doubleValue": 0.5}},
                            ],
                        }
                    ],
                },
            }
        )
    payload = {"traces": traces}
    project_search(
        payload, contexts, '{span.state_lane_id!=""} | select(span.state_lane_id)'
    )
    for trace, context in zip(payload["traces"], contexts):
        attributes = {
            a["key"]: a["value"]["stringValue"]
            for a in trace["spanSet"]["spans"][0]["attributes"]
        }
        assert attributes == {"state_lane_id": context.run_id[:8] + "/rank_0"}
