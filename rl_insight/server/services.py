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

"""High-level server service manager."""

from __future__ import annotations

import atexit
import logging
import json
import os
import subprocess
import sys
import time
from uuid import uuid4
from pathlib import Path
from urllib.parse import urlsplit
from typing import Any

import psutil
import requests
from omegaconf import DictConfig, OmegaConf

from .. import __version__
from ..utils.protocol import SDK_VERSION_HEADER, require_version

from .dependencies import (
    DependencyManager,
    ServiceStatus,
    install_root_from_config,
    prometheus_agent_enabled,
    service_enabled,
)
from .runtime import LocalServiceRuntime, StartedStack, runtime_lock


class ServerServiceManager:
    """Coordinate dependency checks, installation, and local service runtime."""

    def __init__(
        self,
        conf: DictConfig,
        install_root: Path | None = None,
        install_dir: str | Path | None = None,
    ):
        self.conf = conf
        install_location = install_root if install_root is not None else install_dir
        self.install_root = install_root_from_config(conf, install_dir=install_location)
        self.dependencies = DependencyManager(conf, self.install_root)
        self.runtime = LocalServiceRuntime(conf, self.install_root, self.dependencies)

    def check_dependencies(
        self, *, include_versions: bool = True
    ) -> list[ServiceStatus]:
        return self.dependencies.check(include_versions=include_versions)

    def missing_dependencies(
        self, statuses: list[ServiceStatus] | None = None
    ) -> list[ServiceStatus]:
        if statuses is None:
            statuses = self.check_dependencies()
        return self.dependencies.missing(statuses)

    def install_missing_dependencies(
        self,
        *,
        force: bool = False,
        local_archive_dir: str | Path | None = None,
        planned_releases: list[dict[str, Any]] | None = None,
    ) -> list[ServiceStatus]:
        return self.dependencies.install_missing(
            force=force,
            local_archive_dir=local_archive_dir,
            planned_releases=planned_releases,
        )

    def plan_install(self, *, targets: list[str]) -> list[dict[str, Any]]:
        return self.dependencies.plan_install(targets=targets)

    def active_state(self) -> dict[str, Any] | None:
        return self.runtime.active_state()

    def start(
        self, *, detach: bool, attach_logs: bool, auto_port: bool = False
    ) -> StartedStack | None:
        return self.runtime.start(
            detach=detach, attach_logs=attach_logs, auto_port=auto_port
        )

    def wait(self, stack: StartedStack, *, attach_logs: bool) -> int:
        return self.runtime.wait(stack, attach_logs=attach_logs)

    def stop(self) -> tuple[int, list[dict[str, Any]]]:
        return self.runtime.stop()

    def service_rows(self) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        if bool(OmegaConf.select(self.conf, "server.enable", default=True)):
            rows.append(
                {
                    "service": "RL-Insight server",
                    "port": str(int(self.conf.server.port)),
                    "purpose": "service control and discovery",
                }
            )
        if service_enabled(self.conf, "prometheus"):
            agent = prometheus_agent_enabled(self.conf)
            rows.append(
                {
                    "service": "Prometheus Agent" if agent else "Prometheus",
                    "port": str(int(self.conf.prometheus.prometheus_port)),
                    "purpose": "remote write forwarding" if agent else "metrics UI",
                }
            )
        if bool(OmegaConf.select(self.conf, "tempo.enable", default=True)):
            rows.extend(
                [
                    {
                        "service": "Tempo",
                        "port": str(int(self.conf.tempo.query_port)),
                        "purpose": "trace query API",
                    },
                    {
                        "service": "OTLP traces",
                        "port": str(int(self.conf.otel.otel_port)),
                        "purpose": "trainer export URL",
                    },
                ]
            )
        if bool(OmegaConf.select(self.conf, "grafana.enable", default=True)):
            rows.append(
                {
                    "service": "Grafana",
                    "port": str(int(self.conf.grafana.port)),
                    "purpose": "dashboard UI",
                }
            )
        return rows


ObservabilityServiceManager = ServerServiceManager


class SDKServices:
    """Own the services required by an SDK configuration, independently of a framework."""

    def __init__(self, options):
        from ..utils.constants import MonitorPaths
        from ..utils.monitor_config_loader import expand_task_config
        from .commands import ServerConfigValidator
        from .network import format_host_port, local_addresses

        root = MonitorPaths.STATE_ROOT / "tasks" / uuid4().hex
        self.conf = expand_task_config({"rl_insight": options}, runtime_dir=root)
        self.manager = None
        self.stack = None
        self.closed = False
        self.watcher = None
        self.external = self.conf.server.backend == "push"
        ray = sys.modules.get("ray")
        if not self.external and not (ray and ray.is_initialized()):
            raise RuntimeError(
                "Bundled SDK monitoring requires an initialized Ray runtime"
            )
        self.sdk_config = OmegaConf.create(
            {
                "server": {
                    "backend": "push" if self.external else "ray",
                    "namespace": self.conf.server.namespace,
                    "url": self.conf.server.url,
                },
                "labels": self.conf.labels,
                "endpoint_ttl": self.conf.server.endpoint_ttl,
            }
        )
        if self.external:
            self.sdk_config.push = self.conf.push
            atexit.register(self.close)
            return
        try:
            with (
                requests.Session() as session,
                runtime_lock(
                    MonitorPaths.STATE_ROOT
                    / ("sdk-start-" + str(self.conf.server.port))
                ),
            ):
                session.trust_env = False
                if self.conf.server.enable:
                    url = str(self.conf.server.url)
                    address = local_addresses()
                    host = address["host"] or address["loopback"]
                    candidate = url or (
                        "http://" + format_host_port(host, self.conf.server.port)
                    )
                    try:
                        response = session.get(
                            candidate + "/api/v1/services",
                            headers={SDK_VERSION_HEADER: __version__},
                            timeout=2,
                        )
                        response.raise_for_status()
                        services = response.json()
                    except (requests.RequestException, ValueError):
                        if url:
                            raise ValueError(
                                "Configured server URL is unavailable"
                            ) from None
                        services = None
                    if services is not None:
                        require_version(services.get("server_version"), "Server")
                        self.conf.server.url = candidate
                    else:
                        root.mkdir(parents=True, mode=0o700)
                        self.conf.grafana.username = os.environ.get(
                            "GF_SECURITY_ADMIN_USER", "admin"
                        )
                        self.conf.grafana.password = os.environ.get(
                            "GF_SECURITY_ADMIN_PASSWORD", "admin"
                        )
                        self.manager = ServerServiceManager(self.conf)
                        ServerConfigValidator().validate_start(self.conf)
                        self.stack = self.manager.start(
                            detach=True,
                            attach_logs=False,
                            auto_port=True,
                        )
                        self.conf.server.url = "http://" + format_host_port(
                            host, self.conf.server.port
                        )
                        logging.getLogger(__name__).warning(
                            "[rl-insight] SDK services ready: %s; config: %s",
                            self.conf.server.url,
                            root / "services/server.yaml",
                        )
            self.sdk_config.server.url = self.conf.server.url
            atexit.register(self.close)
        except BaseException:
            if self.stack:
                self.manager.stop()
            raise

    def start_forwarder(self, endpoint):
        """Start the existing Agent only after its sink has bound an actual port."""
        if not self.external:
            return
        if (
            OmegaConf.select(self.conf, "server.metrics_port") is not None
            and not endpoint
        ):
            raise ValueError("Configured Prometheus metrics endpoint did not start")
        self.sdk_config.server.metrics_endpoint = endpoint
        if prometheus_agent_enabled(self.conf):
            self.conf.prometheus.metrics_endpoint = endpoint
            self.conf.server.metrics_port = urlsplit(endpoint).port
            self.manager = ServerServiceManager(self.conf)
            self.stack = self.manager.start(
                detach=True, attach_logs=False, auto_port=True
            )
            owner = psutil.Process()
            metadata = {
                "owner": [owner.pid, owner.create_time()],
                "services": [
                    [s.process.pid, psutil.Process(s.process.pid).create_time()]
                    for s in self.stack.services
                ],
                "state_file": str(self.stack.state_file),
            }
            root = Path(self.conf.server.log_dir)
            with (root / "watchdog.log").open("ab") as log:
                self.watcher = subprocess.Popen(
                    [
                        sys.executable,
                        "-c",
                        "from rl_insight.server.services import _watch_sdk_services;import sys;_watch_sdk_services(sys.argv[1])",
                        json.dumps(metadata),
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=os.name == "posix",
                )
        if endpoint:
            logging.getLogger(__name__).warning(
                "[rl-insight] SDK metrics ready: %s", endpoint
            )

    def prepare_collector(self, conf):
        """Resolve one job's backend and expose credential-free worker config."""
        from ..client import create_monitor_client
        from ..utils.monitor_config_loader import load_monitor_config

        resolved = OmegaConf.merge(
            load_monitor_config(),
            self.sdk_config,
            {
                "run_metadata": conf.get("run_metadata", {}),
                "task_signature": conf.task_signature,
            },
        )
        backend = str(resolved.server.backend)
        delegate = None
        if backend != "ray":
            delegate = create_monitor_client(resolved)
            if delegate is None:
                raise RuntimeError("No monitoring client was created for task outputs")
            try:
                self.start_forwarder(getattr(delegate, "metrics_endpoint", ""))
            except BaseException:
                delegate.close()
                raise
            resolved.server.metrics_endpoint = self.sdk_config.server.metrics_endpoint
            resolved.server.backend = "ray"
            resolved.pop("push", None)
        resolved.collector_backend = backend
        if resolved.server.url:
            os.environ["RL_INSIGHT_SERVER_URL"] = str(resolved.server.url)
        return resolved, delegate

    def close(self, *, failed=False):
        if self.closed:
            return
        self.closed = True
        atexit.unregister(self.close)
        try:
            if self.stack and (self.external or failed):
                self.manager.stop()
            # Full storage/query backends remain available for historical dashboards.
        finally:
            if self.watcher is not None:
                self.watcher.terminate()
                self.watcher.wait(timeout=5)


def _watch_sdk_services(raw):
    """Reap SDK-owned task services if their owner is killed without running finish."""
    metadata = json.loads(raw)

    def process(identity):
        try:
            p = psutil.Process(identity[0])
            return (
                p
                if p.create_time() == identity[1]
                and p.is_running()
                and p.status() != psutil.STATUS_ZOMBIE
                else None
            )
        except psutil.Error:
            return None

    while process(metadata["owner"]) is not None:
        time.sleep(0.5)
    children = [
        p for identity in metadata["services"] if (p := process(identity)) is not None
    ]
    for p in reversed(children):
        try:
            p.terminate()
        except psutil.Error:
            pass
    _, pending = psutil.wait_procs(children, timeout=10)
    for p in pending:
        try:
            p.kill()
        except psutil.Error:
            pass
    path = Path(metadata["state_file"])
    if path.exists():
        state = json.loads(path.read_text())
        if {s["pid"] for s in state.get("services", [])} == {
            s[0] for s in metadata["services"]
        }:
            path.unlink(missing_ok=True)
