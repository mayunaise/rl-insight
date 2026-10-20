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

"""Run isolation through the real HTTP API and existing collector path."""

from __future__ import annotations

import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import pytest
import requests
import uvicorn
from fastapi.responses import JSONResponse
from omegaconf import OmegaConf

from rl_insight.server.http_api import create_app


@contextmanager
def serve(app, port=0):
    sock = socket.socket()
    sock.bind(("127.0.0.1", port))
    base = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]})
    thread.start()
    deadline = time.monotonic() + 10
    try:
        while not server.started:
            assert thread.is_alive() and time.monotonic() < deadline
            time.sleep(0.02)
        yield base
    finally:
        server.should_exit = True
        thread.join(10)
        sock.close()


@pytest.fixture
def stack(tmp_path):
    state = SimpleNamespace(
        calls=[],
        traces=[],
        results={},
        fail_writes=False,
        reject_native_once=False,
        native_rejections=0,
        series=None,
    )
    from rl_insight.utils.constants import MonitorPaths

    for template in MonitorPaths.GRAFANA_DASHBOARDS_DIR.rglob("*.json"):
        dashboard = json.loads(template.read_text(encoding="utf-8"))
        state.results[
            "/apis/dashboard.grafana.app/v2/namespaces/default/dashboards/"
            + dashboard["metadata"]["name"]
        ] = {"metadata": {**dashboard["metadata"], "resourceVersion": "1"}}
    dashboard_uid = "a4cabbce-92af-4a84-9399-4deba24ae6d1"
    dashboard_path = (
        "/apis/dashboard.grafana.app/v2/namespaces/default/dashboards/" + dashboard_uid
    )
    state.results[dashboard_path] = {
        "metadata": {
            "name": dashboard_uid,
            "resourceVersion": "1",
            "annotations": {"grafana.app/folder": "existing-verl"},
        }
    }

    class Backend(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            state.calls.append((self.path, b""))
            path = self.path.split("?")[0]
            body = json.dumps(state.results.get(path, {})).encode()
            if state.series is not None and path in {
                "/api/v1/label/run_id/values",
                "/api/v1/query",
            }:
                fields = parse_qs(urlsplit(self.path).query)
                matcher = fields.get("match[]", fields.get("query"))[0]
                pattern = json.loads(
                    re.search(r'__name__=~("(?:[^"\\]|\\.)*")', matcher)[1]
                )
                observed = [
                    row for row in state.series if re.fullmatch(pattern, row["name"])
                ]
                if path.endswith("/query"):
                    end = float(fields["time"][0])
                    start = end - int(re.search(r"\[(\d+)s\]", matcher)[1])
                    data = {
                        "resultType": "vector",
                        "result": [
                            {"metric": {"run_id": rid}, "value": [end, "1"]}
                            for rid in sorted(
                                {
                                    row["run_id"]
                                    for row in observed
                                    if start < row["time"] <= end
                                }
                            )
                        ],
                    }
                else:
                    # Prometheus metadata can include out-of-window series.
                    data = sorted({row["run_id"] for row in observed})
                body = json.dumps({"status": "success", "data": data}).encode()
            if path == "/api/v1/query" and "rl_insight_sdk_flush_sequence" in self.path:
                body = json.dumps(
                    {
                        "status": "success",
                        "data": {"result": [{"value": [time.time(), "1"]}]},
                    }
                ).encode()
            self.send_response(
                404
                if (path.startswith("/api/datasources/") or path.startswith("/apis/"))
                and path not in state.results
                else 200
            )
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            state.calls.append((self.path, body))
            if self.path == "/v1/traces":
                state.traces.append(body)
            elif self.path.startswith("/api/datasources"):
                data = json.loads(body)
                state.results["/api/datasources/uid/" + data["uid"]] = data
            elif self.path.startswith("/apis/"):
                data = json.loads(body)
                data["metadata"]["resourceVersion"] = "1"
                path = (
                    "/apis/dashboard.grafana.app/v2/namespaces/default/dashboards/"
                    + data["metadata"]["name"]
                )
                state.results[path] = data
            self.send_response(404 if state.fail_writes else 200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        do_PUT = do_POST

    backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
    thread = threading.Thread(target=backend.serve_forever)
    thread.start()
    conf = OmegaConf.create(
        {
            "server": {
                "data_dir": str(tmp_path / "data"),
                "runtime_dir": str(tmp_path / "runtime"),
            },
            "prometheus": {"prometheus_port": backend.server_port},
            "tempo": {"query_port": backend.server_port},
            "otel": {"otel_port": backend.server_port},
            "grafana": {"port": backend.server_port},
        }
    )
    app = create_app(conf)

    @app.middleware("http")
    async def reject_native_registration_once(request, call_next):
        if (
            state.reject_native_once
            and request.url.path == "/api/v1/prometheus/targets"
        ):
            payload = await request.json()
            if any(item["target"] == "127.0.0.1:9998" for item in payload["targets"]):
                state.reject_native_once = False
                state.native_rejections += 1
                return JSONResponse(
                    {"detail": "Temporary registration failure"}, status_code=503
                )
        return await call_next(request)

    try:
        with serve(app) as url:
            yield SimpleNamespace(
                url=url, conf=conf, data=tmp_path / "data", backend=state
            )
    finally:
        backend.shutdown()
        thread.join(10)
        backend.server_close()


def test_dashboard_candidates_follow_observed_metrics(stack):
    runs = [
        requests.post(
            stack.url + "/api/v1/runs",
            json={
                "request_id": uuid4().hex,
                "project": project,
                "experiment_name": 'same+name.\\"',
            },
            timeout=3,
        ).json()
        for project in ("project+1", "other", "project+1")
    ]
    catalog = stack.url + "/api/v1/catalog/prometheus/api/v1/label/"
    params = {
        "match[]": '{__name__=~"vllm:.*",project=~".*"}',
        "start": 100,
        "end": 300,
    }
    stack.backend.series = []
    response = requests.get(catalog + "execution/values", params=params, timeout=3)
    assert response.status_code == 200, response.text
    assert response.json()["data"] == []  # Registered is not the same as observed.
    stack.backend.series = [
        {"run_id": runs[0]["run_id"], "name": "vllm:num_requests_running", "time": 150},
        {"run_id": runs[1]["run_id"], "name": "vllm:num_requests_running", "time": 250},
        {"run_id": runs[2]["run_id"], "name": "sglang:gen_throughput", "time": 200},
        {"run_id": uuid4().hex, "name": "vllm:num_requests_running", "time": 200},
    ]

    def values(label="execution", **changes):
        reply = requests.get(
            catalog + label + "/values", params={**params, **changes}, timeout=3
        )
        assert reply.status_code == 200, reply.text
        return reply.json()["data"]

    choices = values()
    assert {item.split("|", 1)[0] for item in choices} == {
        r["run_id"] for r in runs[:2]
    }
    assert all(
        re.search(r'\|same\+name\.\\"_\d{8}_\d{6}_[0-9a-f]{8}$', item)
        for item in choices
    )
    assert values("project") == ["other", "project+1"]
    assert [item.split("|", 1)[0] for item in values(end=200)] == [runs[0]["run_id"]]
    assert [
        item.split("|", 1)[0]
        for item in values(
            **{
                "match[]": '{__name__=~"vllm:.*",project=~"project\\\\+1"}',
            }
        )
    ] == [runs[0]["run_id"]]
    assert [
        item.split("|", 1)[0]
        for item in values(
            **{
                "match[]": '{__name__=~"sglang[:_].*",project=~".*"}',
            }
        )
    ] == [runs[2]["run_id"]]
    assert values(**{"match[]": '{__name__=~"vllm_omni:.*",project=~".*"}'}) == []
    for pattern, selected, expected in (
        ("vllm:.*", runs[0]["run_id"], [runs[0]["run_id"]]),
        ("vllm_omni:.*", runs[0]["run_id"], []),
        ("vllm:.*", "", []),
    ):
        assert (
            values(
                "run_id",
                **{
                    "match[]": '{__name__=~"'
                    + pattern
                    + '",run_id="'
                    + selected
                    + '"}',
                },
            )
            == expected
        )
    assert (
        requests.get(
            catalog + "execution/values",
            params={
                "match[]": '{__name__=~".*",project=~"(a+)+"}',
            },
            timeout=3,
        ).status_code
        == 422
    )
    empty = stack.url + "/api/v1/catalog/"
    assert (
        requests.get(empty + "prometheus/api/v1/query", timeout=3).json()["data"][
            "result"
        ]
        == []
    )
    assert requests.get(empty + "tempo/api/search", timeout=3).json()["traces"] == []


def test_run_catalog_and_scoped_queries(stack, monkeypatch):
    payload = {
        "request_id": uuid4().hex,
        "project": "../项目",
        "experiment_name": 'trial+1.\\value" <script>alert(1)</script> 100%_',
        "config": {
            "server": {"namespace": "demo", "password": "secret"},
            "token": "secret",
        },
    }
    response = requests.post(stack.url + "/api/v1/runs", json=payload, timeout=3)
    assert response.status_code == 200, response.text
    first = response.json()
    assert (
        requests.post(
            stack.url + "/api/v1/runs",
            json=dict(payload, request_id=uuid4().hex, config=[]),
            timeout=3,
        ).status_code
        == 400
    )
    assert (
        requests.post(stack.url + "/api/v1/runs", json=payload, timeout=3).json()
        == first
    )
    changed = dict(payload, experiment_name="other")
    assert (
        requests.post(stack.url + "/api/v1/runs", json=changed, timeout=3).status_code
        == 409
    )
    second = requests.post(
        stack.url + "/api/v1/runs",
        json=dict(payload, request_id=uuid4().hex),
        timeout=3,
    ).json()
    assert first["run_id"] != second["run_id"]
    assert (stack.data / "runs.sqlite").is_file()
    assert not (stack.data / "runs").exists()
    details = requests.get(
        stack.url + "/api/v1/runs/" + first["run_id"], timeout=3
    ).json()
    assert details["context"] == first
    assert details["config"] == {"server": {"namespace": "demo"}}
    assert "secret" not in json.dumps(details)
    assert details["created_at"] > 0
    with serve(create_app(stack.conf)) as reopened:
        restored = requests.get(reopened + "/api/v1/runs/" + first["run_id"], timeout=3)
        assert restored.status_code == 200
        assert restored.json() == details
        assert not (stack.data / "runs").exists()
    run_id = first["run_id"]
    root = stack.url + "/api/v1/runs/" + run_id
    stack.backend.results["/api/v1/query"] = {
        "status": "success",
        "data": {"result": []},
    }
    response = requests.get(
        root + "/prometheus/api/v1/query",
        params={"query": "sum(rate(loss[5m])) or reward"},
        timeout=3,
    )
    assert response.status_code == 200, response.text

    def forwarded(path):
        # Catalog provisioning runs in the background and may interleave calls.
        url = next(
            url
            for url, _ in reversed(stack.backend.calls)
            if urlsplit(url).path == path
        )
        return parse_qs(urlsplit(url).query)

    sent = forwarded("/api/v1/query")["query"][0]
    assert sent.count('run_id="' + run_id + '"') == 2
    for expression, selectors in (
        ("sum(or)", 1),
        ("sum(and)", 1),
        ("sum(unless)", 1),
        ("foo + or + bar", 3),
        ("foo and on(rank) or", 2),
    ):
        assert (
            requests.get(
                root + "/prometheus/api/v1/query",
                params={"query": expression},
                timeout=3,
            ).status_code
            == 200
        )
        sent = forwarded("/api/v1/query")["query"][0]
        assert sent.count('run_id="' + run_id + '"') == selectors
    wrong = 'loss{run_id="' + second["run_id"] + '"}'
    assert (
        requests.post(
            root + "/prometheus/api/v1/query", data={"query": wrong}, timeout=3
        ).status_code
        == 403
    )
    assert (
        requests.get(root + "/prometheus/api/v1/labels", timeout=3).status_code == 200
    )
    assert "match[]" in forwarded("/api/v1/labels")
    assert (
        requests.get(
            root + "/prometheus/api/v1/admin/tsdb/delete_series", timeout=3
        ).status_code
        == 404
    )

    def attrs(owned):
        return [{"key": "rl_insight.run_id", "value": {"stringValue": owned}}]

    trace_id = "0" + "a" * 31
    stack.backend.results["/api/traces/" + trace_id] = {
        "batches": [{"resource": {"attributes": attrs(second["run_id"])}}]
    }
    assert (
        requests.get(root + "/tempo/api/traces/" + trace_id, timeout=3).status_code
        == 404
    )
    stack.backend.results["/api/traces/" + trace_id]["batches"][0]["resource"][
        "attributes"
    ] = attrs(run_id)
    assert (
        requests.get(root + "/tempo/api/traces/" + trace_id, timeout=3).status_code
        == 200
    )
    span = {
        "spanID": "1",
        "name": "train",
        "startTimeUnixNano": "1000000",
        "durationNanos": "2000000",
        "attributes": attrs(run_id)
        + [{"key": "state_lane_id", "value": {"stringValue": "rank_0"}}],
    }
    stack.backend.results["/api/search"] = {
        "traces": [
            {
                "traceID": trace_id.lstrip("0"),
                "rootTraceName": "untrusted-root",
                "spanSets": [{"matched": 1, "spans": [span]}],
            }
        ]
    }
    searched = requests.get(
        root + "/tempo/api/search",
        params={"q": '{name="train"}', "spss": "100"},
        timeout=3,
    )
    assert searched.status_code == 200, searched.text
    assert searched.json()["traces"][0]["traceID"] == trace_id
    assert searched.json()["traces"][0]["rootTraceName"] == "train"
    assert searched.json()["traces"][0]["spanSets"][0]["spans"][0]["attributes"] == [
        {"key": "state_lane_id", "value": {"stringValue": "rank_0"}}
    ]
    assert "resource.rl_insight.run_id" in forwarded("/api/search")["q"][0]
    assert forwarded("/api/search")["spss"] == ["1000"]
    selected = forwarded("/api/search")["q"][0]
    assert selected.endswith(
        " | select(resource.rl_insight.run_id, name, span.state_lane_id)"
    )
    assert (
        requests.get(
            root + "/tempo/api/search", params={"spss": "10001"}, timeout=3
        ).status_code
        == 422
    )
    # Agent Loop's field projection must preserve its fields and ownership proof.
    projected = requests.get(
        root + "/tempo/api/search",
        params={"q": '{name="train"} | select(span.turn, span.content)'},
        timeout=3,
    )
    assert projected.status_code == 200, projected.text
    selected = forwarded("/api/search")["q"][0]
    assert "resource.rl_insight.run_id = " in selected
    assert selected.endswith(
        " | select(resource.rl_insight.run_id, name, span.state_lane_id, span.turn, span.content)"
    )
    for unsupported in (
        "{true} | count()",
        "{true} | select(span.turn) | count()",
        '{true} | select(span.turn, "bad")',
    ):
        assert (
            requests.get(
                root + "/tempo/api/search", params={"q": unsupported}, timeout=3
            ).status_code
            == 422
        )
    trace_calls = sum(
        urlsplit(url).path == "/api/search" for url, _ in stack.backend.calls
    )
    assert (
        requests.get(
            root + "/tempo/api/search",
            params={"q": "{true} | select(" + "span.field," * 7000 + "span.last)"},
            timeout=3,
        ).status_code
        in {400, 413, 414}  # HTTP parsers may reject the URI before the app does.
    )
    assert (
        sum(urlsplit(url).path == "/api/search" for url, _ in stack.backend.calls)
        == trace_calls
    )
    span["attributes"] = attrs(second["run_id"])
    assert requests.get(root + "/tempo/api/search", timeout=3).status_code == 502
    from rl_insight.protocol import RunContext
    from rl_insight.server.runtime import run_dashboard

    template = json.loads(
        (
            Path(__file__).parents[3]
            / "rl_insight/config/services/grafana/dashboards/verl/verl_tainer_v1_with_vllm_engine.json"
        ).read_text(encoding="utf-8")
    )
    dashboard = run_dashboard(
        Path(__file__).parents[3]
        / "rl_insight/config/services/grafana/dashboards/verl/verl_tainer_v1_with_vllm_engine.json"
    )
    assert dashboard["spec"]["layout"] == template["spec"]["layout"]
    assert dashboard["metadata"]["name"] == template["metadata"]["name"]
    assert dashboard["spec"]["title"] == template["spec"]["title"]
    timeline = dashboard["spec"]["elements"]["panel-40"]["spec"]["vizConfig"]
    assert timeline["spec"]["fieldConfig"]["overrides"] == [
        {
            "matcher": {"id": "byName", "options": "traceName"},
            "properties": [
                {"id": "displayName", "value": "${__field.labels.state_lane_id}"}
            ],
        }
    ]
    assert len(dashboard["spec"]["elements"]) == len(template["spec"]["elements"])
    for name, element in dashboard["spec"]["elements"].items():
        assert (
            element["spec"]["vizConfig"]
            == template["spec"]["elements"][name]["spec"]["vizConfig"]
        )
    assert "rl_insight_monitor_" in json.dumps(dashboard)
    from rl_insight.utils.constants import MonitorPaths

    for file in MonitorPaths.GRAFANA_DASHBOARDS_DIR.rglob("*.json"):
        original = json.loads(file.read_text(encoding="utf-8"))
        path = (
            "/apis/dashboard.grafana.app/v2/namespaces/default/dashboards/"
            + original["metadata"]["name"]
        )
        bound = stack.backend.results[path]
        assert bound["spec"]["title"] == original["spec"]["title"]
        variables_all = {v["spec"]["name"]: v for v in bound["spec"]["variables"]}
        key = "experiment" if "experiment" in variables_all else "experiment_name"
        selection = variables_all[key]["spec"]
        assert selection["label"] == "Experiment"
        assert variables_all[key]["kind"] == "QueryVariable"
        assert selection["options"] == []
        assert selection["refresh"] == "onTimeRangeChanged"
        assert (
            original["metadata"]["annotations"]["rl-insight/metrics-selector"][:-1]
            in selection["definition"]
        )
        assert variables_all["run_id"]["spec"]["hide"] == "hideVariable"
        assert variables_all["run_id"]["spec"]["skipUrlSync"] is True
        assert (
            bound["spec"]["layout"] == original["spec"]["layout"] or key == "experiment"
        )

        def queries(value):
            if isinstance(value, dict):
                if str(value.get("kind", "")).endswith("Variable"):
                    assert value["spec"].get("hide") != ""
                if value.get("kind") == "DataQuery" and value.get("group") in {
                    "tempo",
                    "prometheus",
                }:
                    assert value["datasource"]["name"] in {
                        "rli-${run_id}-" + value["group"][0],
                        "rli-catalog",
                    }
                    expression = value["spec"].get(
                        "expr", value["spec"].get("query", "")
                    )
                    if value["datasource"]["name"] != "rli-catalog":
                        assert "$" + key not in expression
                        assert "${" + key + "}" not in expression
                for item in value.values():
                    queries(item)
            elif isinstance(value, list):
                for item in value:
                    queries(item)

        queries(bound["spec"])
        for name, element in original["spec"]["elements"].items():
            assert (
                bound["spec"]["elements"][name]["spec"]["vizConfig"]
                == element["spec"]["vizConfig"]
            )
    prepared = requests.get(root + "/view", timeout=3)
    assert prepared.status_code == 200, prepared.text
    assert all(
        "rli-" + run_id in value for value in prepared.json()["datasources"].values()
    )
    posted = json.loads(
        next(
            body
            for path, body in reversed(stack.backend.calls)
            if path.endswith(template["metadata"]["name"]) and body
        )
    )
    assert posted["metadata"]["annotations"]["grafana.app/folder"] == "existing-verl"
    assert not any(
        path == "/apis/dashboard.grafana.app/v2/namespaces/default/dashboards" and body
        for path, body in stack.backend.calls
    )
    variables = {v["spec"]["name"]: v for v in posted["spec"]["variables"]}
    assert len(variables) == len(posted["spec"]["variables"])
    assert variables["run_id"]["spec"]["hide"] == "hideVariable"
    selector = variables["experiment_name"]["spec"]
    assert selector["label"] == "Experiment"
    assert selector["current"]["value"] == ""
    assert selector["multi"] is False and selector["includeAll"] is False
    assert selector["options"] == []
    assert selector["query"]["datasource"]["name"] == "rli-catalog"
    assert "vllm:.*" in selector["definition"]
    assert variables["experiment_name"]["kind"] == "QueryVariable"
    assert variables["project"]["kind"] == "QueryVariable"
    assert "rli-${run_id}-p" in json.dumps(posted)
    assert "rli-${run_id}-t" in json.dumps(posted)
    registered = [
        json.loads(body)
        for path, body in stack.backend.calls
        if path == "/api/datasources" and body
    ]
    assert {body["uid"] for body in registered} >= {
        "rli-" + item["run_id"] + suffix
        for item in (first, second)
        for suffix in ("-p", "-t")
    }
    opened = requests.get(
        stack.url + "/ui/runs/" + run_id, allow_redirects=False, timeout=3
    )
    assert opened.status_code == 307
    assert "/d/" + template["metadata"]["name"] in opened.headers["Location"]
    assert "var-experiment_name=" + run_id in opened.headers["Location"]
    other_view = requests.get(
        stack.url + "/api/v1/runs/" + second["run_id"] + "/view", timeout=3
    ).json()
    assert other_view["url"].split("?")[0] == prepared.json()["url"].split("?")[0]
    assert (
        requests.get(
            root + "/view", params={"template": "../../config.yaml"}, timeout=3
        ).status_code
        == 404
    )
    stack.backend.fail_writes = True
    assert requests.get(root + "/view", timeout=3).status_code == 502
    stack.backend.fail_writes = False
    import rl_insight as monitor
    from rl_insight.client.base import MONITOR_CLIENT_REGISTRY, MonitorClient

    class LegacyClient(MonitorClient):
        def apply_event(self, event):
            pass

    monkeypatch.setenv("RL_INSIGHT_SERVER_URL", stack.url)
    monkeypatch.setitem(MONITOR_CLIENT_REGISTRY, "legacy", lambda conf: LegacyClient())
    try:
        monitor.init(config={"server": {"url": stack.url, "backend": "legacy"}})
        assert monitor.get_context() is None
        monitor.update_prometheus_config(["127.0.0.1:19999"])
        assert "19999" in (stack.data / "targets/prometheus-targets.yml").read_text(
            encoding="utf-8"
        )
    finally:
        monitor.finish()
    monkeypatch.delenv("RL_INSIGHT_SERVER_URL")
    monitor.update_prometheus_config(
        ["127.0.0.1:19998"], context=RunContext.from_dict(first)
    )
    assert "19998" in (stack.data / "targets/prometheus-targets.yml").read_text(
        encoding="utf-8"
    )


@pytest.mark.parametrize("context_delay", [0, 35], ids=["immediate", "late-context"])
def test_existing_ray_api_keeps_one_run_identity(stack, context_delay):
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
        ExportTraceServiceRequest,
    )

    from rl_insight.server.runtime import DEFAULT_TEMPLATE
    from rl_insight.utils.constants import MonitorPaths

    template = json.loads(
        (MonitorPaths.GRAFANA_DASHBOARDS_DIR / DEFAULT_TEMPLATE).read_text(
            encoding="utf-8"
        )
    )
    selector = next(
        variable["spec"]
        for variable in template["spec"]["variables"]
        if variable["spec"]["name"] == "experiment_name"
    )
    assert selector["label"] == "Experiment" and selector["options"] == []

    with socket.socket() as reserve:
        reserve.bind(("127.0.0.1", 0))
        port = reserve.getsockname()[1]
    stack.backend.reject_native_once = bool(context_delay)
    script = "import runpy,sys; runpy.run_path(sys.argv[1])['_ray_scenario'](sys.argv[2],int(sys.argv[3]),context_delay=float(sys.argv[4]))"
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(Path(__file__).resolve()),
            stack.url,
            str(port),
            str(context_delay),
        ],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert child.returncode == 0, child.stdout + child.stderr
    assert stack.backend.native_rejections == int(bool(context_delay))
    context = json.loads(
        next(
            line[8:]
            for line in child.stdout.splitlines()
            if line.startswith("CONTEXT ")
        )
    )
    assert stack.backend.traces
    request = ExportTraceServiceRequest.FromString(stack.backend.traces[0])
    attrs = {
        a.key: a.value.string_value
        for a in request.resource_spans[0].resource.attributes
    }
    assert attrs["rl_insight.run_id"] == context["run_id"]
    assert attrs["service.name"] == "rl_insight_monitor"
    assert request.resource_spans[0].scope_spans[0].spans[0].name == "update_actor"
    targets = (stack.data / "targets/prometheus-targets.yml").read_text(
        encoding="utf-8"
    )
    assert (
        targets.count(context["run_id"]) == 2
        and "replica: '0'" in targets
        and "replica: '1'" in targets
    )
    with sqlite3.connect(stack.data / "runs.sqlite") as db:
        assert db.execute("SELECT run_id FROM runs").fetchall() == [
            (context["run_id"],)
        ]
    path = (
        "/apis/dashboard.grafana.app/v2/namespaces/default/dashboards/"
        + template["metadata"]["name"]
    )
    selector = next(
        variable["spec"]
        for variable in stack.backend.results[path]["spec"]["variables"]
        if variable["spec"]["name"] == "experiment_name"
    )
    assert selector["options"] == []
    assert selector["query"]["datasource"]["name"] == "rli-catalog"
    # Restore the template, change ports, then inspect sources before any view request.
    for public_url in ("", "https://monitor.example.invalid/insight"):
        stack.backend.results[path] = template
        stack.conf.server.public_url = public_url
        with socket.socket() as reserve:
            reserve.bind(("127.0.0.1", 0))
            stack.conf.server.port = reserve.getsockname()[1]
        with serve(create_app(stack.conf), port=stack.conf.server.port) as restarted:
            for kind in ("prometheus", "tempo"):
                source = stack.backend.results[
                    "/api/datasources/uid/rli-" + context["run_id"] + "-" + kind[0]
                ]
                assert source["url"] == (
                    (public_url or restarted)
                    + "/api/v1/runs/"
                    + context["run_id"]
                    + "/"
                    + kind
                )
            selector = next(
                variable["spec"]
                for variable in stack.backend.results[path]["spec"]["variables"]
                if variable["spec"]["name"] == "experiment_name"
            )
            assert selector["options"] == []
            catalog_source = stack.backend.results["/api/datasources/uid/rli-catalog"]
            assert (
                catalog_source["url"]
                == (public_url or restarted) + "/api/v1/catalog/prometheus"
            )
            assert (
                requests.get(
                    restarted + "/api/v1/runs/" + context["run_id"], timeout=3
                ).status_code
                == 200
            )


def _ray_scenario(url, port, real_backends=False, context_delay=0):
    import ray

    import rl_insight as insight
    from rl_insight.client.ray_monitor_client import _current_job_actor_name
    from rl_insight.utils.constants import MonitorRayActor

    os.environ["RL_INSIGHT_SERVER_URL"] = url
    os.environ["OTEL_BSP_SCHEDULE_DELAY"] = "100"
    ray.init(num_cpus=2, include_dashboard=False, logging_level="ERROR")
    conf = {"prometheus": {"metrics_report_port": port}}
    try:

        @ray.remote
        class NativeExporter:
            def __init__(self):
                import rl_insight as monitor

                monitor.update_prometheus_config(
                    ["127.0.0.1:9998"], job_name="native", labels=[{"replica": "1"}]
                )

            def ready(self):
                return True

            def registered(self):
                from rl_insight.utils.prometheus_utils import _pending_targets

                return not _pending_targets

        if not real_backends:
            exporter = NativeExporter.remote()
            assert ray.get(exporter.ready.remote())
            insight.update_prometheus_config(
                ["127.0.0.1:9999"], job_name="native", labels=[{"replica": "0"}]
            )
            time.sleep(context_delay)
        insight.init(project="demo", experiment_name="same", config=conf)
        context = insight.get_context()
        assert context is not None
        if not real_backends:
            deadline = time.monotonic() + 10
            while not ray.get(exporter.registered.remote()):
                assert time.monotonic() < deadline
                time.sleep(0.1)

        @ray.remote(max_calls=1)
        def worker(config):
            import rl_insight as monitor

            monitor.init(project="demo", experiment_name="same", config=config)
            monitor.metric_gauge("loss", 0.25, rank="0")
            with monitor.trace_state("update_actor", state_lane_id="actor_0"):
                pass
            return monitor.get_context().to_dict()

        assert ray.get(worker.remote(conf)) == context.to_dict()
        hub = ray.get_actor(
            _current_job_actor_name(), namespace=MonitorRayActor.NAMESPACE
        )
        endpoint = ray.get(hub.get_status.remote())["metrics_endpoint"]
        deadline = time.monotonic() + 10
        while True:
            metrics = requests.get(endpoint, timeout=3).text
            if "rl_insight_monitor_loss" in metrics:
                break
            assert time.monotonic() < deadline
            time.sleep(0.05)
        assert 'run_id="' + context.run_id + '"' in metrics
        assert "0.25" in metrics
        with pytest.raises(ValueError, match="run_id"):
            insight.metric_gauge("loss", 0.5, run_id=uuid4().hex)

        if real_backends:
            root = url + "/api/v1/runs/" + context.run_id
            for path, field in (
                ("prometheus/api/v1/query", "data"),
                ("tempo/api/search", "traces"),
            ):
                deadline = time.monotonic() + 45
                while True:
                    response = requests.get(
                        root + "/" + path,
                        params={"query": "rl_insight_monitor_loss"}
                        if field == "data"
                        else {},
                        timeout=12,
                    )
                    assert response.status_code == 200, response.text
                    payload = response.json()
                    ready = (
                        payload.get("data", {}).get("result")
                        if field == "data"
                        else payload.get("traces")
                    )
                    if ready:
                        break
                    assert time.monotonic() < deadline, response.text
                    time.sleep(0.2)
            view = requests.get(root + "/view", timeout=20)
            assert view.status_code == 200, view.text

        time.sleep(0.5)  # The final metric below intentionally has no scrape delay.
        if real_backends:
            insight.metric_gauge("final_step", 2)
        print("CONTEXT " + json.dumps(context.to_dict()))
    finally:
        insight.finish()
        ray.shutdown()


@pytest.mark.skipif(
    not os.environ.get("RL_INSIGHT_RUN_E2E_URL"),
    reason="requires a managed Prometheus/Tempo/Grafana stack",
)
def test_sequential_runs_on_managed_backends():
    url = os.environ["RL_INSIGHT_RUN_E2E_URL"].rstrip("/")
    contexts = []
    start = time.time()
    for port in (19102, 19103):
        script = "import runpy,sys; runpy.run_path(sys.argv[1])['_ray_scenario'](sys.argv[2],int(sys.argv[3]),True)"
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
                str(Path(__file__).resolve()),
                url,
                str(port),
            ],
            capture_output=True,
            text=True,
            timeout=150,
        )
        assert child.returncode == 0, child.stdout + child.stderr
        contexts.append(
            json.loads(
                next(
                    line[8:]
                    for line in child.stdout.splitlines()
                    if line.startswith("CONTEXT ")
                )
            )
        )
    assert contexts[0]["run_id"] != contexts[1]["run_id"]
    trace_ids = []
    for context in contexts:
        root = url + "/api/v1/runs/" + context["run_id"]
        metrics = requests.get(
            root + "/prometheus/api/v1/query_range",
            params={
                "query": "rl_insight_monitor_loss",
                "start": start,
                "end": time.time(),
                "step": 1,
            },
            timeout=12,
        ).json()
        assert metrics["data"]["result"]
        assert all(
            item["metric"]["run_id"] == context["run_id"]
            for item in metrics["data"]["result"]
        )
        final = requests.get(
            root + "/prometheus/api/v1/query",
            params={"query": "max_over_time(rl_insight_monitor_final_step[10m])"},
            timeout=12,
        ).json()
        assert (
            final["data"]["result"]
            and float(final["data"]["result"][0]["value"][1]) == 2
        )
        traces = requests.get(root + "/tempo/api/search", timeout=12).json()["traces"]
        assert traces
        trace_ids.append(traces[0]["traceID"])
        assert (
            requests.get(
                root + "/tempo/api/traces/" + trace_ids[-1], timeout=12
            ).status_code
            == 200
        )
    for current, foreign in ((0, 1), (1, 0)):
        root = url + "/api/v1/runs/" + contexts[current]["run_id"]
        assert (
            requests.get(
                root + "/tempo/api/traces/" + trace_ids[foreign], timeout=12
            ).status_code
            == 404
        )
