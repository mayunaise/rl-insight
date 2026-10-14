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
import socket
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from uuid import uuid4

import pytest
import requests
import uvicorn
from omegaconf import OmegaConf

from rl_insight.server.http_api import create_app


@contextmanager
def serve(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
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
    state = SimpleNamespace(calls=[], traces=[], results={})

    class Backend(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            state.calls.append((self.path, b""))
            body = json.dumps(state.results.get(self.path.split("?")[0], {})).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            state.calls.append((self.path, body))
            if self.path == "/v1/traces":
                state.traces.append(body)
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

    backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
    thread = threading.Thread(target=backend.serve_forever)
    thread.start()
    conf = OmegaConf.create({
        "server": {"data_dir": str(tmp_path / "data"), "runtime_dir": str(tmp_path / "runtime")},
        "prometheus": {"prometheus_port": backend.server_port},
        "tempo": {"query_port": backend.server_port},
        "otel": {"otel_port": backend.server_port},
        "grafana": {"port": backend.server_port},
    })
    try:
        with serve(create_app(conf)) as url:
            yield SimpleNamespace(url=url, conf=conf, data=tmp_path / "data", backend=state)
    finally:
        backend.shutdown()
        thread.join(10)
        backend.server_close()


def test_run_directories_and_scoped_queries(stack):
    payload = {
        "request_id": uuid4().hex, "project": "../项目", "experiment_name": "same-name",
        "config": {"server": {"namespace": "demo", "password": "secret"}, "token": "secret"},
    }
    response = requests.post(stack.url + "/api/v1/runs", json=payload, timeout=3)
    assert response.status_code == 200, response.text
    first = response.json()
    assert requests.post(stack.url + "/api/v1/runs", json=payload, timeout=3).json() == first
    changed = dict(payload, experiment_name="other")
    assert requests.post(stack.url + "/api/v1/runs", json=changed, timeout=3).status_code == 409
    second = requests.post(stack.url + "/api/v1/runs", json=dict(payload, request_id=uuid4().hex), timeout=3).json()
    assert first["run_id"] != second["run_id"]
    directories = list(stack.data.glob("experiments/*/*/runs/*"))
    assert {p.name for p in directories} == {first["run_id"], second["run_id"]}
    for directory in directories:
        assert directory.resolve().is_relative_to(stack.data)
        assert json.loads((directory / "run.json").read_text())["context"]["run_id"] == directory.name
        assert "secret" not in (directory / "config.json").read_text()
    with serve(create_app(stack.conf)) as reopened:
        restored = requests.get(reopened + "/api/v1/runs/" + first["run_id"], timeout=3)
        assert restored.status_code == 200
        assert restored.json()["context"] == first
