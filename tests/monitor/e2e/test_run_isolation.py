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

"""Representative execution-name flows through real HTTP and Ray."""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
import requests
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from omegaconf import OmegaConf

from rl_insight.server.http_api import create_app
from rl_insight.utils.protocol import RunContext


@contextmanager
def serve(app, port=0):
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))
        url = f"http://127.0.0.1:{sock.getsockname()[1]}"
        server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
        thread = threading.Thread(
            target=server.run, kwargs={"sockets": [sock]}, daemon=True
        )
        thread.start()
        deadline = time.monotonic() + 10
        try:
            while not server.started:
                assert thread.is_alive() and time.monotonic() < deadline
                time.sleep(0.01)
            yield url
        finally:
            server.should_exit = True
            thread.join(10)


@pytest.fixture
def stack(tmp_path):
    backend = FastAPI()
    state = SimpleNamespace(
        span_attributes=[],
        traces=[],
        trace_tenants=[],
        lose_response=False,
    )

    @backend.post("/{path:path}")
    async def ingest(path: str, request: Request):
        if path == "v1/traces":
            from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
                ExportTraceServiceRequest,
            )

            body = await request.body()
            state.traces.append(body)
            state.trace_tenants.append(request.headers.get("X-Scope-OrgID"))
            batch = ExportTraceServiceRequest.FromString(body)
            for resource in batch.resource_spans:
                state.span_attributes.extend(
                    {
                        "name": span.name,
                        "attributes": {
                            attribute.key: attribute.value.string_value
                            for attribute in span.attributes
                        },
                    }
                    for scope in resource.scope_spans
                    for span in scope.spans
                )
        return {}

    with serve(backend) as backend_url:
        port = urlsplit(backend_url).port
        conf = OmegaConf.create(
            {
                "server": {
                    "data_dir": str(tmp_path / "data"),
                    "runtime_dir": str(tmp_path / "runtime"),
                },
                "prometheus": {"prometheus_port": port},
                "tempo": {"query_port": port},
                "otel": {"otel_port": port},
                "grafana": {"enable": False},
            }
        )
        app = create_app(conf)

        @app.middleware("http")
        async def lost_response(request, call_next):
            response = await call_next(request)
            if (
                request.method == "POST"
                and request.url.path == "/api/v1/prometheus/targets"
            ):
                if state.lose_response:
                    state.lose_response = False
                    return JSONResponse(
                        {"detail": "retry after target registration"},
                        status_code=503,
                    )
            return response

        with serve(app) as url:
            yield SimpleNamespace(url=url, conf=conf, backend=state, path=tmp_path)


def test_same_named_executions_register_distinct_native_labels(stack):
    from rl_insight.utils.prometheus_utils import _register_targets

    contexts = [
        RunContext.create(project, "same", stack.url) for project in ("p", "p", "other")
    ]
    assert len({context.experiment_name for context in contexts}) == 3
    for i, context in enumerate(contexts):
        assert _register_targets(
            [f"host{i}:9000"], "workers", [{"experiment_name": "same"}], context
        )
    targets_file = stack.path / "data/targets/prometheus-targets.yml"
    targets = yaml.safe_load(targets_file.read_text())
    assert {group["labels"]["experiment_name"] for group in targets} == {
        context.experiment_name for context in contexts
    }
    assert sum(group["labels"]["project"] == "p" for group in targets) == 2
    assert all("run_id" not in group["labels"] for group in targets)
    with serve(create_app(stack.conf)) as restarted:
        assert requests.get(restarted + "/healthz", timeout=5).status_code == 200
        assert yaml.safe_load(targets_file.read_text()) == targets

    server_only = OmegaConf.merge(
        stack.conf,
        {
            "server": {"backend": "local"},
            **{name: {"enable": False} for name in ("prometheus", "tempo", "grafana")},
        },
    )
    with serve(create_app(server_only)) as url:
        assert _register_targets(
            ["worker:9000"], "workers", None, RunContext.create("p", "server-only", url)
        )


@pytest.mark.parametrize(
    "bootstrap", [False, True], ids=["explicit-url", "task-config"]
)
def test_ray_workers_share_execution_name_and_registration_recovers(stack, bootstrap):
    pytest.importorskip("ray")
    stack.backend.lose_response = True
    script = r"""
import json, socket, sys, time
import requests
import ray
import rl_insight as insight
from rl_insight.client.ray_monitor_client import initialize_current_job_connection

url = sys.argv[1]
ray.init(num_cpus=2, include_dashboard=False, log_to_driver=False)
try:
    insight.init()
    with insight.trace_state("driver_prepare", state_lane_id="early_driver"):
        pass
    assert insight.get_context() is None

    @ray.remote(num_cpus=0)
    class EarlyWorker:
        def trace_before_training(self):
            insight.init()
            with insight.trace_state("sleep", state_lane_id="early_replica"):
                pass
            context = insight.get_context()
            return context.to_dict() if context else None

        def wait_for_context(self):
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                context = insight.get_context()
                if context:
                    return context.to_dict()
                time.sleep(0.1)
            raise TimeoutError("Early worker did not connect to the training Run")

    early_workers = [EarlyWorker.remote(), EarlyWorker.remote()]
    early_contexts = ray.get(
        [worker.trace_before_training.remote() for worker in early_workers], timeout=20
    )
    assert early_contexts == [None, None], (
        "Argument-free worker init must defer Run creation until training init", early_contexts
    )
    delayed_context = early_workers[1].wait_for_context.remote()

    @ray.remote
    def native():
        insight.update_prometheus_config(["127.0.0.1:19999"])
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            context = initialize_current_job_connection()
            if context:
                return context.experiment_name
            time.sleep(0.1)
        raise TimeoutError("Hub did not become ready")
    waiting = native.remote()
    time.sleep(1)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = {"enabled": True, "server": {"url": url}} if sys.argv[2] == "True" else {"server": {"url": url}, "prometheus": {"metrics_report_port": port}}
    insight.init(project="ray", experiment_name="same", config=config)
    first = insight.get_context()
    assert ray.get(delayed_context)["experiment_name"] == first.experiment_name
    assert ray.get(early_workers[0].wait_for_context.remote())["experiment_name"] == first.experiment_name
    insight.metric_gauge("reward", 2)
    @ray.remote
    def worker():
        insight.init(project="ray", experiment_name="same", config={"server": {"url": url}})
        insight.metric_gauge("reward", 3)
        now = time.time_ns()
        insight.trace_span(name="actor_update", start_time_ns=now, end_time_ns=now + 1000, attributes={})
        context = insight.get_context()
        insight.finish()
        return context.experiment_name
    experiment_name = ray.get(worker.remote())
    assert experiment_name == ray.get(waiting) == first.experiment_name
    assert insight.get_context().events_applied > first.events_applied
    snapshot = insight.get_context().to_dict()
    snapshot["metrics"] = requests.get(snapshot["metrics_endpoint"], timeout=5).text
    print("RESULT=" + json.dumps(snapshot))
    # Observe normal batch export; this test does not introduce a flush guarantee.
    time.sleep(6)
    insight.finish()
finally:
    ray.shutdown()
"""
    result = subprocess.run(
        [sys.executable, "-c", script, stack.url, str(bootstrap)],
        text=True,
        capture_output=True,
        timeout=100,
        env={**os.environ, "RL_INSIGHT_SERVER_URL": stack.url},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    context = json.loads(
        next(
            line[7:]
            for line in result.stdout.splitlines()
            if line.startswith("RESULT=")
        )
    )
    assert "run_id" not in context
    assert context["original_experiment_name"] == "same"
    assert re.fullmatch(
        r"same_[0-9]{8}-[0-9]{6}_[0-9a-f]{8}", context["experiment_name"]
    )
    metrics = context["metrics"]
    assert 'experiment_name="' + context["experiment_name"] + '"' in metrics
    assert "rl_insight_monitor_experiment_info" in metrics
    assert not re.search(r"[{,]run_id=", metrics)
    assert any(
        s["name"] == "actor_update"
        and s["attributes"]["experiment_name"] == context["experiment_name"]
        for s in stack.backend.span_attributes
    )
    for name, lane, count in (
        ("sleep", "early_replica", 2),
        ("driver_prepare", "early_driver", 1),
    ):
        spans = [s for s in stack.backend.span_attributes if s["name"] == name]
        assert len(spans) == count
        for span in spans:
            assert {
                "project": "ray",
                "experiment_name": context["experiment_name"],
                "state_lane_id": lane,
            }.items() <= span["attributes"].items()

    targets = yaml.safe_load(
        (stack.path / "data/targets/prometheus-targets.yml").read_text()
    )
    assert any(
        "127.0.0.1:19999" in group["targets"]
        and group["labels"]["experiment_name"] == context["experiment_name"]
        for group in targets
    )


@pytest.mark.parametrize("runtime", ["ray", "direct", "ray-abrupt"])
def test_sdk_starts_services_from_training_configuration(stack, tmp_path, runtime):
    """Training config covers SDK/native metrics, tenant traces and owned cleanup."""
    script = r"""import sys, time
import ray, requests
import rl_insight as monitor
from omegaconf import OmegaConf
from prometheus_client import CollectorRegistry, Gauge, Histogram, start_http_server
from prometheus_client.parser import text_string_to_metric_families
from urllib.parse import urlsplit
from rl_insight import __version__
from rl_insight.utils.protocol import SDK_VERSION_HEADER

config = OmegaConf.load(sys.argv[1])
runtime = sys.argv[2]
is_ray = runtime.startswith("ray")
if is_ray:
    ray.init(num_cpus=2, include_dashboard=False, logging_level="ERROR")
    @ray.remote(num_cpus=0)
    class EarlyWorker:
        def start(self):
            registry = CollectorRegistry()
            Gauge("native_before_init", "", registry=registry).set(9)
            self.server, _ = start_http_server(0, addr="127.0.0.1", registry=registry)
            monitor.update_prometheus_config([f"127.0.0.1:{self.server.server_port}"], "early")
            monitor.init()
            monitor.metric_count("early_sdk_events", 1)
    early = EarlyWorker.remote()
    ray.get(early.start.remote())
try:
    monitor.init(project="sdk-config", experiment_name="external", config=config.trainer.rl_insight)
    assert monitor.api._STATE.enabled and monitor.get_context() is None
    url = monitor.api._STATE.conf.server.url
    def worker(rank="0"):
        if is_ray:
            monitor.init()
        monitor.metric_gauge("config_only_loss", 0.125, rank=rank, task_id="spoof")
        monitor.metric_count("config_requests", 5, rank=rank)
        monitor.metric_histogram("config_samples", 0.5, rank=rank)
        with monitor.trace_state("actor_update"):
            pass
        registry = CollectorRegistry()
        Gauge("native_sdk_value", "", ["task_id"], registry=registry).labels("spoof").set(7)
        histogram = Histogram("native_latency_seconds", "", buckets=[1, 3], registry=registry)
        histogram.observe(0.5)
        histogram.observe(2)
        server, _ = start_http_server(0, addr="127.0.0.1", registry=registry)
        monitor.update_prometheus_config([f"127.0.0.1:{server.server_port}"], "native")
        if is_ray:
            monitor.finish()
    if is_ray:
        run_worker = ray.remote(worker)
        ray.get([run_worker.remote(str(rank)) for rank in range(2)])
    else:
        worker()
    headers = {SDK_VERSION_HEADER: __version__}
    assert requests.get(url+"/api/v1/services", headers={SDK_VERSION_HEADER:"0.3.0"}, timeout=3).status_code == 409
    services = requests.get(url+"/api/v1/services", headers=headers, timeout=3).json()
    port = services["metrics_port"]
    expected = config.trainer.rl_insight.get("metrics", {}).get("port", urlsplit(url).port)
    assert port == expected
    metrics_url = f"http://{urlsplit(url).hostname}:{port}"
    if port != urlsplit(url).port:
        assert requests.get(metrics_url+"/api/v1/services", timeout=3).status_code == 404
    required = ["native_sdk_value", "rl_insight_monitor_config_requests_total"]
    if is_ray:
        required += ["native_before_init", "rl_insight_monitor_early_sdk_events_total"]
    deadline = time.monotonic()+10
    while True:
        text = requests.get(metrics_url+"/metrics", timeout=10).text
        if all(name in text for name in required):
            break
        assert time.monotonic() < deadline, text
        time.sleep(0.1)
    assert 'task_id="sdk-only"' in text and 'task_id="spoof"' not in text
    samples = [sample for family in text_string_to_metric_families(text) for sample in family.samples]
    def values(name):
        return [sample.value for sample in samples if sample.name == name]
    count = 2 if is_ray else 1
    loss = "rl_insight_monitor_config_only_loss" if is_ray else "custom_loss"
    assert values(loss) == [0.125]*count
    assert values("rl_insight_monitor_config_requests_total") == [5]*count
    assert values("rl_insight_monitor_config_samples_count") == [1]*count
    assert values("native_latency_seconds_count") == [2]*count
    assert values("native_latency_seconds_sum") == [2.5]*count
    assert [sample.value for sample in samples if sample.name == "native_latency_seconds_bucket" and sample.labels["le"] == "1.0"] == [1]*count
    if runtime == "ray-abrupt":
        ray.shutdown()
    else:
        monitor.finish()
    deadline = time.monotonic()+20
    for endpoint in {url, metrics_url}:
        while True:
            try:
                if not requests.get(endpoint+"/healthz", timeout=1).ok:
                    break
            except requests.RequestException:
                break
            assert time.monotonic() < deadline, "owned collector survived termination"
            time.sleep(0.1)
finally:
    monitor.finish()
    ray.shutdown()
"""
    options = {
        "server": {"port": 0, "endpoint_ttl": 1},
        "metrics": {"pull": True},
        "traces": {
            "endpoint": f"http://127.0.0.1:{stack.conf.otel.otel_port}/v1/traces",
            "headers": {"X-Scope-OrgID": "sdk-tenant"},
        },
        "labels": {"task_id": "sdk-only"},
    }
    if runtime == "direct":
        options["sinks"] = [
            {
                "driver": "rl_insight.client.push.sinks.prometheus:create_sink",
                "streams": ["metric"],
                "metric_mapping": [
                    {"source": source, "name": name}
                    for source, name in (
                        ("config_only_loss", "custom_loss"),
                        ("config_requests", "rl_insight_monitor_config_requests"),
                        ("config_samples", "rl_insight_monitor_config_samples"),
                    )
                ],
            },
            {
                "driver": "rl_insight.client.push.sinks.otlp:create_sink",
                "streams": ["trace"],
                "strip_name_prefix": "",
                **options.pop("traces"),
            },
        ]
        del options["metrics"]
    if runtime == "ray":
        with socket.socket() as reserve:
            reserve.bind(("127.0.0.1", 0))
            options["metrics"]["port"] = reserve.getsockname()[1]
    config = tmp_path / "training.yaml"
    OmegaConf.save(OmegaConf.create({"trainer": {"rl_insight": options}}), config)
    result = subprocess.run(
        [sys.executable, "-c", script, str(config), runtime],
        env={k: v for k, v in os.environ.items() if not k.startswith("RL_INSIGHT_")},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    if runtime != "ray-abrupt":
        assert set(stack.backend.trace_tenants) == {"sdk-tenant"}
        spans = stack.backend.span_attributes
        name = "trace.actor_update" if runtime == "direct" else "actor_update"
        assert spans and all(
            span["name"] == name and span["attributes"]["task_id"] == "sdk-only"
            for span in spans
        )


def test_task_collector_publishers_recover_without_minting_an_identity():
    """A restarted task proxy must rediscover live exporters through their leases."""
    from urllib.parse import urlsplit

    from rl_insight.client.push.sinks.prometheus import PrometheusSink
    from rl_insight.client.push.poller import set_active_registry
    from rl_insight.utils.prometheus_utils import update_prometheus_config
    from prometheus_client import CollectorRegistry, Gauge, start_http_server

    conf = OmegaConf.create(
        {
            "prometheus": {"enable": False},
            "tempo": {"enable": False},
            "grafana": {"enable": False},
            "server": {"backend": "push", "endpoint_ttl": 1},
        }
    )
    service = serve(create_app(conf))
    url = service.__enter__()
    sink = None
    native_registry = CollectorRegistry()
    Gauge("native_recovered", "Recovery", registry=native_registry).set(9)
    native, native_thread = start_http_server(
        0, addr="127.0.0.1", registry=native_registry
    )
    try:
        sink = PrometheusSink(url, listen_host="127.0.0.1", advertise_host="127.0.0.1")
        set_active_registry(sink.target_registry)
        sink.emit_store("worker_value", 7, {})
        assert "worker_value" in requests.get(url + "/metrics", timeout=3).text
        service.__exit__(None, None, None)
        # Registration while the proxy is unavailable must not interrupt training.
        update_prometheus_config([f"127.0.0.1:{native.server_port}"], job_name="native")
        service = serve(create_app(conf), urlsplit(url).port)
        assert service.__enter__() == url
        deadline = time.monotonic() + 6
        while True:
            metrics = requests.get(url + "/metrics", timeout=3).text
            if "worker_value" in metrics and "native_recovered" in metrics:
                break
            assert time.monotonic() < deadline
            time.sleep(0.05)
        assert "run_id=" not in requests.get(url + "/metrics", timeout=3).text
        competing = requests.post(
            url + "/api/v1/prometheus/targets",
            json={
                "publisher_id": uuid4().hex,
                "targets": [{"target": f"127.0.0.1:{native.server_port}"}],
            },
            timeout=3,
        )
        assert competing.status_code == 409
        sink.close()
        sink = None
        final = requests.get(url + "/metrics", timeout=3).text
        assert "worker_value" in final and "native_recovered" in final
        time.sleep(1.1)
        expired = requests.get(url + "/metrics", timeout=3).text
        assert "worker_value" not in expired and "native_recovered" not in expired
    finally:
        set_active_registry(None)
        if sink is not None:
            sink.close()
        service.__exit__(None, None, None)
        native.shutdown()
        native.server_close()
        native_thread.join(5)


@pytest.mark.skipif(
    not os.environ.get("RL_INSIGHT_PROMETHEUS_BINARY"),
    reason="Set RL_INSIGHT_PROMETHEUS_BINARY to test real Agent remote write",
)
@pytest.mark.parametrize("ipv6", [False, True])
def test_task_collector_remote_writes_to_real_prometheus(tmp_path, monkeypatch, ipv6):
    """Native Agent sends the same SDK/native samples as the pull entry."""
    from prometheus_client import CollectorRegistry, Histogram, start_http_server

    binary = Path(os.environ["RL_INSIGHT_PROMETHEUS_BINARY"]).resolve()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        receiver_port = sock.getsockname()[1]
    receiver_url = f"http://127.0.0.1:{receiver_port}"
    receiver_config = tmp_path / "receiver.yaml"
    receiver_config.write_text("global: {scrape_interval: 1s}\nscrape_configs: []\n")
    config = tmp_path / "monitor.yaml"
    from rl_insight.utils.monitor_config_loader import expand_task_config

    generated = expand_task_config(
        {
            "rl_insight": {
                "server": {"port": 0, "endpoint_ttl": 5},
                "labels": {"task_id": "platform-task", "platform": "test"},
                "metrics": {
                    "port": 0,
                    "binary_path": str(binary),
                    "scrape_interval": "1s",
                    "scrape_timeout": "1s",
                    "remote_write": [
                        {
                            "url": receiver_url + "/api/v1/write",
                            "headers": {"X-Scope-OrgID": "tenant-test"},
                            "queue_config": {
                                "batch_send_deadline": "200ms",
                                "min_backoff": "100ms",
                                "max_backoff": "1s",
                            },
                        }
                    ],
                },
            }
        },
        runtime_dir=tmp_path,
    )
    generated.server.install_dir = str(tmp_path / "services")
    generated.server.runtime_dir = str(tmp_path / "runtime")
    del generated.server.data_dir
    generated.prometheus.min_version = None
    OmegaConf.save(generated, config)
    cli = [sys.executable, "-m", "rl_insight", "server"]
    cli_env = dict(os.environ)
    registry = CollectorRegistry()
    latency = Histogram(
        "native_push_latency_seconds", "Latency", buckets=[1, 3], registry=registry
    )
    latency.observe(0.5)
    latency.observe(2)
    native, native_thread = start_http_server(0, addr="127.0.0.1", registry=registry)
    sink = None
    log = (tmp_path / "receiver.log").open("w")
    receiver = subprocess.Popen(
        [
            str(binary),
            f"--config.file={receiver_config}",
            f"--web.listen-address=127.0.0.1:{receiver_port}",
            f"--storage.tsdb.path={tmp_path / 'receiver-data'}",
            "--web.enable-remote-write-receiver",
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
    )

    def wait_for(call, timeout=25):
        deadline = time.monotonic() + timeout
        while True:
            try:
                result = call()
                if result:
                    return result
            except requests.RequestException:
                pass
            assert receiver.poll() is None, (tmp_path / "receiver.log").read_text()
            assert time.monotonic() < deadline, list(
                (tmp_path / "services/logs").glob("*")
            )
            time.sleep(0.1)

    def query(expression):
        response = requests.get(
            receiver_url + "/api/v1/query", params={"query": expression}, timeout=3
        )
        response.raise_for_status()
        return response.json()["data"]["result"]

    try:
        wait_for(lambda: requests.get(receiver_url + "/-/ready", timeout=1).ok)
        started = subprocess.run(
            cli + ["start", "--config", str(config), "--detach", "--auto-port"],
            capture_output=True,
            text=True,
            timeout=35,
            env=cli_env,
        )
        assert started.returncode == 0, started.stdout + started.stderr
        state = json.loads((tmp_path / "state.json").read_text())
        assert {s["name"] for s in state["services"]} == {
            "prometheus",
            "rl-insight-server",
        }
        agent = next(s for s in state["services"] if s["name"] == "prometheus")
        assert any(a in {"--agent", "--enable-feature=agent"} for a in agent["command"])
        assert not any("--storage.tsdb" in a for a in agent["command"])
        assert any("--storage.agent.path" in a for a in agent["command"])
        runtime = OmegaConf.load(tmp_path / "runtime/server.yaml")
        host = "127.0.0.1"
        url = f"http://{host}:{runtime.server.port}"
        assert runtime.server.metrics_port != runtime.server.port
        scrape = OmegaConf.load(tmp_path / "runtime/prometheus.yml")
        assert scrape.scrape_configs[0].static_configs[0].targets == [
            f"{host}:{runtime.server.metrics_port}"
        ]
        from rl_insight.client.push.sinks.prometheus import create_sink

        monkeypatch.setenv("POD_IP", "::1" if ipv6 else "127.0.0.1")
        sink = create_sink(
            OmegaConf.create({"server_url": url, "advertise_host": "${oc.env:POD_IP}"})
        )
        sink.emit_store("sdk_push_loss", 0.25, {"rank": "0"})
        sink.emit_counter("sdk_push_requests", 3, {})
        sink.target_registry.set_targets(
            [f"127.0.0.1:{native.server_port}"], job_name="native"
        )
        assert "sdk_push_loss" in requests.get(url + "/metrics", timeout=3).text
        stored = wait_for(lambda: query('sdk_push_loss{task_id="platform-task"}'))
        assert float(stored[0]["value"][1]) == 0.25
        assert stored[0]["metric"]["platform"] == "test"
        assert not query('sdk_push_loss{task_id="another-task"}')
        assert (
            float(wait_for(lambda: query("sdk_push_requests_total"))[0]["value"][1])
            == 3
        )
        assert (
            float(
                wait_for(lambda: query("native_push_latency_seconds_count"))[0][
                    "value"
                ][1]
            )
            == 2
        )
        assert float(query("native_push_latency_seconds_sum")[0]["value"][1]) == 2.5
        assert (
            float(query('native_push_latency_seconds_bucket{le="1.0"}')[0]["value"][1])
            == 1
        )
        assert scrape.remote_write[0].headers["X-Scope-OrgID"] == "tenant-test"
        sink.emit_store("sdk_push_final", 17, {})
        sink.close()
        sink = None
        assert float(wait_for(lambda: query("sdk_push_final"))[0]["value"][1]) == 17
        assert list((tmp_path / "runtime/prometheus/agent").rglob("wal"))
    finally:
        if sink is not None:
            sink.close()
        stopped = subprocess.run(
            cli + ["stop", "--config", str(config)],
            capture_output=True,
            text=True,
            timeout=25,
            env=cli_env,
        )
        native.shutdown()
        native.server_close()
        native_thread.join(5)
        receiver.terminate()
        receiver.wait(10)
        log.close()
        assert stopped.returncode == 0, stopped.stdout + stopped.stderr
