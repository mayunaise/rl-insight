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

"""Expose process metrics and register raw endpoints with one task collector."""

from __future__ import annotations

import re
import threading
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from omegaconf import OmegaConf
from prometheus_client import CollectorRegistry, start_http_server

from ....server.network import format_host_port, local_addresses
from ....utils.prometheus_utils import (
    EndpointPublisher,
    MetricRegistry,
    metrics_wsgi_app,
)
from ..poller import Target, TargetRegistry
from ..tags import collect_sink_tags
from .base import PushSink


class PrometheusSink(PushSink):
    """A private metrics registry per process; the task collector handles fan-in."""

    def __init__(
        self,
        server_url: str,
        *,
        listen_host="0.0.0.0",
        port=0,
        advertise_host=None,
        prefix="",
        labels=None,
    ):
        super().__init__(prefix)
        self._server_url = server_url
        self._labels = dict(labels or {})
        self._lock = threading.RLock()
        self._closed = False
        self._publishers: dict[str, EndpointPublisher] = {}
        self.target_registry = TargetRegistry(self._publish_target)
        registry = CollectorRegistry()
        self._metrics = MetricRegistry(registry=registry)
        self._server, self._thread = start_http_server(
            port, addr=listen_host, registry=registry
        )
        incarnation = uuid4().hex
        self._server.set_app(
            metrics_wsgi_app(self._server.get_app(), None, incarnation)
        )
        host = advertise_host or (
            listen_host
            if listen_host not in {"", "0.0.0.0", "::"}
            else local_addresses()["host"] or "127.0.0.1"
        )
        try:
            self._publisher = EndpointPublisher(
                server_url,
                {
                    "component": "trainer",
                    "instance": incarnation,
                    "incarnation": incarnation,
                    "metrics_url": "http://"
                    + format_host_port(host, self._server.server_port)
                    + "/metrics",
                    "labels": {"job": "trainer_metrics", **self._labels},
                },
                alive=self._thread.is_alive,
            )
        except Exception:
            self._server.shutdown()
            self._server.server_close()
            self._thread.join(5)
            raise

    def _publish_target(self, target: Target):
        with self._lock:
            if self._closed:
                return
            labels = {**target.labels, **self._labels}
            if target.address in self._publishers:
                publisher = self._publishers[target.address]
                if any(
                    publisher.description["labels"].get(k) != v
                    for k, v in labels.items()
                ):
                    # Reuse registration to attach labels; ownership stays unchanged.
                    publisher.add_labels(labels)
                return
            url = (
                target.address
                if target.address.startswith(("http://", "https://"))
                else "http://" + target.address
            )
            self._publishers[target.address] = EndpointPublisher(
                self._server_url,
                {
                    "component": target.labels.get("job", "native"),
                    "instance": target.address,
                    "metrics_url": url.rstrip("/")
                    if url.rstrip("/").endswith("/metrics")
                    else url.rstrip("/") + "/metrics",
                    "labels": labels,
                },
                retry_initial=True,
            )

    def _emit(self, method, name, value, tags):
        with self._lock:
            if self._closed:
                return
            name = re.sub(r"[^a-zA-Z0-9_:]", "_", self.metric_name(name))
            if not name or name[0].isdigit():
                raise ValueError("Invalid Prometheus metric name")
            getattr(self._metrics, method)(
                name, "", float(value), labels={**tags, **self._labels}
            )

    def emit_counter(self, name: str, value: float, tags: Mapping[str, str]) -> None:
        self._emit("count", name, value, tags)

    def emit_store(self, name: str, value: float, tags: Mapping[str, str]) -> None:
        self._emit("value", name, value, tags)

    def emit_timer(self, name: str, value_us: float, tags: Mapping[str, str]) -> None:
        self._emit("distribution", name, value_us, tags)

    def emit_histogram(self, name: str, value: float, tags: Mapping[str, str]) -> None:
        self._emit("distribution", name, value, tags)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        for publisher in list(self._publishers.values()) + [self._publisher]:
            publisher.close()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(5)


def create_sink(conf: Any) -> PrometheusSink:
    server_url = str(OmegaConf.select(conf, "server_url") or "").strip()
    if not server_url:
        raise ValueError("Prometheus sink requires the task collector server_url")
    return PrometheusSink(
        server_url,
        listen_host=str(OmegaConf.select(conf, "listen_host", default="0.0.0.0")),
        port=int(OmegaConf.select(conf, "port", default=0)),
        advertise_host=OmegaConf.select(conf, "advertise_host"),
        prefix=str(OmegaConf.select(conf, "prefix") or ""),
        labels=collect_sink_tags(conf),
    )
