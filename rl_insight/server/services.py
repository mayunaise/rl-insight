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
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

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
    """Start or connect the bundled monitoring services on the Hub node."""

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
        self.sdk_config = OmegaConf.create(
            {
                "server": {
                    "backend": "ray",
                    "namespace": self.conf.server.namespace,
                    "url": self.conf.server.url,
                },
                "labels": self.conf.labels,
                "endpoint_ttl": self.conf.server.endpoint_ttl,
            }
        )
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

    def close(self, *, failed=False):
        """Clean up a failed start; retain running backends for historical data."""
        if self.closed:
            return
        self.closed = True
        atexit.unregister(self.close)
        if self.stack and failed:
            self.manager.stop()
