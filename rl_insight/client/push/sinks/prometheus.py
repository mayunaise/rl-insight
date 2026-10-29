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

"""Expose SDK and native exporter metrics through one sink-owned endpoint."""

from __future__ import annotations

import copy
import ipaddress
import logging
import re
import threading
import time
from collections.abc import Mapping
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any

from omegaconf import OmegaConf
from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.parser import text_string_to_metric_families

from ....server.network import format_host_port, local_addresses
from ....utils.prometheus_utils import MetricRegistry
from ..poller import TargetRegistry, _http_fetch_metrics
from ..tags import collect_sink_tags
from .base import PushSink

logger = logging.getLogger(__name__)


class _NativeCollector:
    """Read native targets in HTTP threads; SDK events never wait on exporter I/O."""

    def __init__(self, sdk, targets, labels, timeout):
        self._sdk, self._targets, self._labels = sdk, targets, labels
        self._timeout = timeout
        self._pool = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="insight-scrape"
        )
        self._slots = threading.BoundedSemaphore(4)
        self._lock = threading.Lock()
        self._last = {}
        self._frozen = None

    def _read(self, target, deadline):
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Scrape deadline exceeded")
            text = _http_fetch_metrics(target.address, timeout=min(2, remaining))
            if len(text.encode()) > 4 * 1024 * 1024:
                raise ValueError("Exporter response exceeds 4 MiB")
            families = list(text_string_to_metric_families(text))
            seen = set()
            for family in families:
                if family.name.startswith("rl_insight_proxy_"):
                    raise ValueError("Exporter uses reserved collector metric names")
                for sample in family.samples:
                    if len(seen) >= 100000 or time.monotonic() >= deadline:
                        raise TimeoutError("Exporter parsing budget exceeded")
                    labels = {
                        "job": target.job_name or "native",
                        **target.labels,
                        **sample.labels,
                        "instance": target.address,
                        "component": target.job_name
                        or target.labels.get("job", "native"),
                        **self._labels,
                    }
                    key = (sample.name, tuple(sorted(labels.items())))
                    if key in seen:
                        raise ValueError("Exporter has duplicate samples")
                    seen.add(key)
                    sample.labels.clear()
                    sample.labels.update(labels)
            return families
        finally:
            self._slots.release()

    @staticmethod
    def _merge(groups):
        definitions = {}
        for family in groups:
            definitions.setdefault(family.name, set()).add(
                (family.type, family.documentation)
            )
        merged = {}
        for family in groups:
            if len(definitions[family.name]) != 1:
                logger.warning("Conflicting native metric metadata: %s", family.name)
                continue
            if family.name not in merged:
                merged[family.name] = copy.copy(family)
                merged[family.name].samples = list(family.samples)
            else:
                merged[family.name].samples.extend(family.samples)
        return list(merged.values())

    def collect(self):
        if self._frozen is not None:
            return self._frozen
        sdk = list(self._sdk.collect())
        if not self._lock.acquire(blocking=False):
            return sdk
        pending = {}
        try:
            deadline = time.monotonic() + self._timeout
            targets = iter(self._targets.snapshot())
            exhausted = False
            collected = []
            up = GaugeMetricFamily(
                "rl_insight_proxy_endpoint_up",
                "Native exporter scrape success",
                labels=["instance"],
            )
            while (pending or not exhausted) and time.monotonic() < deadline:
                while len(pending) < 4 and not exhausted:
                    target = next(targets, None)
                    if target is None:
                        exhausted = True
                    elif self._slots.acquire(blocking=False):
                        pending[self._pool.submit(self._read, target, deadline)] = (
                            target
                        )
                    else:
                        up.add_metric([target.address], 0)
                if not pending:
                    continue
                done, _ = wait(
                    pending,
                    timeout=max(0, deadline - time.monotonic()),
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    break
                for future in done:
                    target = pending.pop(future)
                    try:
                        families = future.result()
                        collected.extend(families)
                        self._last[target.address] = families
                        up.add_metric([target.address], 1)
                    except Exception as exc:
                        logger.warning(
                            "Native exporter %s failed: %s", target.address, exc
                        )
                        up.add_metric([target.address], 0)
            for future, target in pending.items():
                up.add_metric([target.address], 0)
                if future.cancel():
                    self._slots.release()
            return self._merge(sdk + collected) + [up]
        finally:
            self._lock.release()

    def freeze(self):
        self.collect()
        with self._lock:
            self._frozen = self._merge(
                list(self._sdk.collect())
                + [family for families in self._last.values() for family in families]
            )
            return self._frozen

    def close(self):
        self._pool.shutdown(wait=False, cancel_futures=True)


class PrometheusSink(PushSink):
    """One private registry and HTTP listener in the Hub or direct SDK process."""

    def __init__(
        self,
        *,
        port=0,
        listen_host=None,
        advertise_host=None,
        prefix="",
        labels=None,
        scrape_timeout=5.0,
        drain_seconds=0.0,
    ):
        super().__init__(prefix)
        self._labels = dict(labels or {})
        self._lock = threading.RLock()
        self._closed = False
        self._drain_seconds = float(drain_seconds)
        if not 0 < float(scrape_timeout) <= 30 or not 0 <= self._drain_seconds <= 3600:
            raise ValueError("Invalid Prometheus collection or drain timeout")
        self.target_registry = TargetRegistry()
        sdk = CollectorRegistry()
        self._metrics = MetricRegistry(registry=sdk)
        self._collector = _NativeCollector(
            sdk, self.target_registry, self._labels, float(scrape_timeout)
        )
        exposed = CollectorRegistry()
        exposed.register(self._collector)
        host = advertise_host or (
            listen_host
            if listen_host not in {None, "", "0.0.0.0", "::"}
            else local_addresses()["host"] or "127.0.0.1"
        )
        if listen_host is None:
            listen_host = "::" if ipaddress.ip_address(host).version == 6 else "0.0.0.0"
        try:
            self._server, self._thread = start_http_server(
                port, addr=listen_host, registry=exposed
            )
            app = self._server.get_app()

            def prometheus_text(environ, start_response):
                # OpenMetrics encoding escapes native names such as vllm:.
                environ["HTTP_ACCEPT"] = "text/plain;version=0.0.4"
                return app(environ, start_response)

            self._server.set_app(prometheus_text)
        except BaseException:
            self._collector.close()
            raise
        self.metrics_endpoint = (
            "http://" + format_host_port(host, self._server.server_port) + "/metrics"
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

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        try:
            if self._collector.freeze():
                time.sleep(self._drain_seconds)
        finally:
            self._server.shutdown()
            self._server.server_close()
            self._thread.join(5)
            self._collector.close()


def create_sink(conf: Any) -> PrometheusSink:
    return PrometheusSink(
        port=int(OmegaConf.select(conf, "port", default=0)),
        listen_host=OmegaConf.select(conf, "listen_host"),
        advertise_host=OmegaConf.select(conf, "advertise_host"),
        prefix=str(OmegaConf.select(conf, "prefix") or ""),
        labels=collect_sink_tags(conf),
        scrape_timeout=float(OmegaConf.select(conf, "scrape_timeout", default=5)),
        drain_seconds=float(OmegaConf.select(conf, "drain_seconds", default=0)),
    )
