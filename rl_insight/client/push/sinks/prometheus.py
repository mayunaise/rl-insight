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

"""Expose SDK metrics and reuse direct target registration for external outputs."""

from __future__ import annotations

import re
import hashlib
import http.client
import logging
import math
import socket
import time
import ipaddress
import threading
from collections.abc import Mapping
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import asynccontextmanager
from types import SimpleNamespace
from urllib.parse import urlsplit
from typing import Any
from uuid import uuid4

import requests
from fastapi import Body, FastAPI, Header, Request
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool
from omegaconf import DictConfig, OmegaConf
from prometheus_client import (
    CollectorRegistry,
    Gauge,
    generate_latest,
    start_http_server,
)
from prometheus_client.parser import text_string_to_metric_families

from .... import __version__
from ....server.network import format_host_port, local_addresses
from ....utils.constants import PrometheusScrape
from ....utils.protocol import (
    RunError,
    SDK_VERSION_HEADER,
    metric_labels,
    require_version,
)
from ....utils.prometheus_utils import (
    MetricRegistry,
    _build_target_payload,
)

from ..poller import Target, TargetRegistry
from ..tags import collect_sink_tags
from .base import PushSink


logger = logging.getLogger(__name__)


class PrometheusSink(PushSink):
    """A private metrics registry per process; the task collector handles fan-in."""

    def __init__(
        self,
        server_url: str,
        *,
        listen_host=None,
        port=0,
        advertise_host=None,
        prefix="",
        labels=None,
    ):
        super().__init__(prefix)
        self._server_url = server_url.rstrip("/")
        self._labels = dict(labels or {})
        self._lock = threading.RLock()
        self._closed = False
        self._publisher_id = uuid4().hex
        self.target_registry = TargetRegistry(self._publish_target)
        registry = CollectorRegistry()
        self._metrics = MetricRegistry(registry=registry)
        host = advertise_host or (
            listen_host
            if listen_host not in {None, "", "0.0.0.0", "::"}
            else local_addresses()["host"] or "127.0.0.1"
        )
        if listen_host is None:
            listen_host = "::" if ipaddress.ip_address(host).version == 6 else "0.0.0.0"
        self._server, self._thread = start_http_server(
            port, addr=listen_host, registry=registry
        )
        self._publish_target(
            Target(
                format_host_port(host, self._server.server_port),
                {"job": "trainer_metrics"},
            )
        )

    def _publish_target(self, target: Target):
        with self._lock:
            if self._closed:
                return
            labels = {**target.labels, **self._labels}
            _publish_task_targets(
                self._server_url,
                self._publisher_id,
                [target.address],
                labels.get("job", "native"),
                [labels],
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
        _retire_task_targets(self._server_url, self._publisher_id)
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(5)


def create_sink(conf: Any) -> PrometheusSink:
    server_url = str(OmegaConf.select(conf, "server_url") or "").strip()
    if not server_url:
        raise ValueError("Prometheus sink requires the task collector server_url")
    return PrometheusSink(
        server_url,
        listen_host=OmegaConf.select(conf, "listen_host"),
        port=int(OmegaConf.select(conf, "port", default=0)),
        advertise_host=OmegaConf.select(conf, "advertise_host"),
        prefix=str(OmegaConf.select(conf, "prefix") or ""),
        labels=collect_sink_tags(conf),
    )


def _validate_publisher_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise RunError("Invalid publisher ID")
    return value


class TaskTargets:
    """Target/publisher TTL contract without managed Run identity or persistence."""

    def __init__(self, ttl=30):
        if not 1 <= ttl <= 3600:
            raise RunError("Target TTL must be between 1 and 3600 seconds")
        self.ttl = ttl
        self._lock = threading.RLock()
        self._targets = {}

    def register(self, payload):
        publisher = _validate_publisher_id(payload.get("publisher_id"))
        targets = payload.get("targets")
        if not isinstance(targets, list) or not targets or len(targets) > 64:
            raise RunError("Expected 1 to 64 targets")
        job = payload.get("job_name", "trainer_metrics")
        if not isinstance(job, str) or not job or len(job) > 256:
            raise RunError("Invalid target job")
        incoming = {}
        for target in targets:
            if not isinstance(target, dict) or not isinstance(
                target.get("target"), str
            ):
                raise RunError("Target must contain a host:port address")
            address = target["target"]
            try:
                parsed = urlsplit("//" + address)
                ipaddress.ip_address(parsed.hostname)
                if (
                    not parsed.port
                    or not 1 <= parsed.port <= 65535
                    or parsed.path
                    or parsed.query
                    or parsed.fragment
                    or parsed.username
                    or parsed.password
                    or "%" in parsed.hostname
                    or any(c.isspace() for c in address)
                ):
                    raise ValueError
            except (TypeError, ValueError) as exc:
                raise RunError(
                    "Target must be a literal IPv4/IPv6 address and port"
                ) from exc
            labels = metric_labels(target.get("labels", {}))
            incoming[address] = {
                "publisher_id": publisher,
                "metrics_url": "http://" + address + "/metrics",
                "component": job,
                "labels": {"job": job, **labels},
                "endpoint_id": hashlib.sha256(
                    (publisher + address).encode()
                ).hexdigest()[:32],
                "expires_at": time.monotonic() + self.ttl,
            }
        with self._lock:
            active = {item["metrics_url"]: item for item in self.endpoints()}
            for item in incoming.values():
                current = active.get(item["metrics_url"])
                if current and (
                    current["publisher_id"] != publisher or "snapshot" in current
                ):
                    raise RunError(
                        "Target already belongs to another or closing publisher", 409
                    )
            if len(set(self._targets) | set(incoming)) > 64:
                raise RunError("Task target capacity exceeded", 429)
            self._targets.update(incoming)
        return {"status": "ok", "ttl": self.ttl}

    def endpoints(self):
        with self._lock:
            now = time.monotonic()
            self._targets = {
                k: v for k, v in self._targets.items() if v["expires_at"] > now
            }
            return [dict(item) for item in self._targets.values()]

    def endpoint_valid(self, item):
        with self._lock:
            current = self._targets.get(urlsplit(item["metrics_url"]).netloc)
            return (
                current is not None
                and current["publisher_id"] == item["publisher_id"]
                and current["expires_at"] > time.monotonic()
            )

    def retire(self, item, snapshot):
        with self._lock:
            if self.endpoint_valid(item):
                current = self._targets[urlsplit(item["metrics_url"]).netloc]
                current.update(
                    snapshot=snapshot, expires_at=time.monotonic() + self.ttl
                )


class PrometheusCollector:
    def __init__(self, registry, timeout=9.0, *, labels=None):
        self.registry = registry
        self.timeout = timeout
        self._labels = metric_labels(labels or {})
        self._pool = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="insight-scrape"
        )
        self._capacity = threading.BoundedSemaphore(4)
        self._collect_lock = threading.Lock()

    def close(self):
        self._pool.shutdown(wait=True, cancel_futures=True)

    def _read(self, item, deadline):
        if "snapshot" in item:
            return item["snapshot"]
        address = urlsplit(item["metrics_url"])
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Scrape deadline exceeded")
        connection = http.client.HTTPConnection(
            address.hostname, address.port, timeout=min(2, remaining)
        )
        timer = None
        try:
            connection.connect()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Scrape deadline exceeded")
            sock = connection.sock
            sock.settimeout(min(2, remaining))

            def abort():
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

            timer = threading.Timer(remaining, abort)
            timer.daemon = True
            timer.start()
            connection.request(
                "GET",
                "/metrics",
                headers={
                    "Accept": "text/plain; version=0.0.4",
                    "Accept-Encoding": "identity",
                },
            )
            response = connection.getresponse()
            if response.status != 200:
                raise RunError("Exporter returned a failed response", 502)
            body = bytearray()
            while chunk := response.read(65536):
                body.extend(chunk)
                if len(body) > 4 * 1024 * 1024 or time.monotonic() >= deadline:
                    raise RunError("Exporter response exceeds collection budget", 502)
            return body.decode("utf-8")
        finally:
            if timer is not None:
                timer.cancel()
            connection.close()

    def retire(self, publisher_id):
        _validate_publisher_id(publisher_id)
        deadline = time.monotonic() + min(9, self.timeout)
        items = [
            item
            for item in self.registry.endpoints()
            if item["publisher_id"] == publisher_id
        ]
        pending = {
            self._pool.submit(self._read, item, deadline): item
            for item in items
            if "snapshot" not in item
        }
        done, _ = (
            wait(pending, timeout=max(0, deadline - time.monotonic()))
            if pending
            else ([], [])
        )
        failed = len(done) != len(pending)
        for future in done:
            try:
                self.registry.retire(pending[future], future.result())
            except (RunError, OSError, ValueError, http.client.HTTPException):
                failed = True
        if failed:
            raise RunError("Some final task targets could not be collected", 503)

    def _submit(self, item, deadline):
        if not self._capacity.acquire(blocking=False):
            return None

        def fetch():
            try:
                text = self._read(item, deadline)
                if not self.registry.endpoint_valid(item):
                    raise RunError("Endpoint lease changed during collection", 409)
                families = []
                seen = set()
                count = 0
                for family in text_string_to_metric_families(text):
                    families.append(family)
                    count += len(family.samples)
                    if count > 100000 or time.monotonic() >= deadline:
                        raise RunError("Endpoint parsing budget exceeded", 502)
                    if family.name.startswith("rl_insight_proxy_"):
                        raise RunError(
                            "Exporter uses reserved collection metric prefix", 409
                        )
                    for sample in family.samples:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Endpoint parsing deadline exceeded")
                        labels = {**item["labels"], **sample.labels, **self._labels}
                        for key, value in {
                            "component": item["component"],
                            "endpoint_id": item["endpoint_id"],
                        }.items():
                            if key in labels and str(labels[key]) != value:
                                raise RunError(
                                    "Exporter metric identity conflicts with registration",
                                    409,
                                )
                            labels[key] = value
                        labels.setdefault(
                            "instance", urlsplit(item["metrics_url"]).netloc
                        )
                        key = (sample.name, tuple(sorted(labels.items())))
                        if key in seen:
                            raise RunError("Exporter has duplicate samples", 409)
                        seen.add(key)
                        sample.labels.clear()
                        sample.labels.update(labels)
                return families, len(text.encode())
            finally:
                self._capacity.release()

        try:
            return self._pool.submit(fetch)
        except Exception:
            self._capacity.release()
            raise

    def collect(self, timeout=None):
        if not self._collect_lock.acquire(blocking=False):
            raise RunError("This task already has a collection request", 503)
        try:
            return self._collect(min(self.timeout, timeout or self.timeout))
        finally:
            self._collect_lock.release()

    def _collect(self, timeout):
        items = self.registry.endpoints()
        deadline = time.monotonic() + timeout
        sources = iter(items)
        pending = {}
        merged = {}
        results = []
        total_bytes = 0
        total_samples = 0
        successful = set()
        exhausted = False
        while pending or not exhausted:
            while not exhausted and len(pending) < 4:
                item = next(sources, None)
                if item is None:
                    exhausted = True
                    break
                future = self._submit(item, deadline)
                if future is None:
                    logger.warning(
                        "Task endpoint %s cannot acquire collection capacity",
                        item["endpoint_id"],
                    )
                else:
                    pending[future] = item
            if not pending:
                continue
            done, _ = wait(
                pending,
                timeout=max(0, deadline - time.monotonic()),
                return_when=FIRST_COMPLETED,
            )
            if not done:
                logger.warning(
                    "Task collection deadline exceeded with %d reads pending",
                    len(pending),
                )
                break
            for future in done:
                item = pending.pop(future)
                try:
                    families, size = future.result()
                    total_bytes += size
                    total_samples += sum(len(family.samples) for family in families)
                    if total_bytes > 16 * 1024 * 1024 or total_samples > 200000:
                        raise RunError("Aggregated sample budget exceeded", 503)
                    if time.monotonic() >= deadline or not self.registry.endpoint_valid(
                        item
                    ):
                        raise TimeoutError("Collection lease or deadline expired")
                    results.append((item, families))
                except (
                    RunError,
                    OSError,
                    ValueError,
                    http.client.HTTPException,
                ) as exc:
                    logger.warning(
                        "Task endpoint %s collection failed: %s",
                        item["endpoint_id"],
                        exc,
                    )
        definitions = {}
        for _item, families in results:
            for family in families:
                definitions.setdefault(family.name, set()).add(
                    (family.type, family.documentation)
                )
        conflicts = {
            name for name, variants in definitions.items() if len(variants) > 1
        }
        for item, families in results:
            if any(
                family.name in conflicts for family in families
            ) or not self.registry.endpoint_valid(item):
                logger.warning(
                    "Task endpoint %s has conflicting metadata or a changed lease",
                    item["endpoint_id"],
                )
                continue
            for family in families:
                if family.name in merged:
                    merged[family.name].samples.extend(family.samples)
                else:
                    merged[family.name] = family
            successful.add(item["endpoint_id"])
        metrics = CollectorRegistry()
        status = Gauge(
            "rl_insight_proxy_endpoint_up",
            "Endpoint collected successfully",
            ["endpoint_id", "component"],
            registry=metrics,
        )
        for item in items:
            status.labels(
                item["endpoint_id"],
                item["component"],
            ).set(item["endpoint_id"] in successful)
        for name, value in {
            "endpoints": len(items),
            "failures": len(items) - len(successful),
            "empty": not items,
        }.items():
            gauge = Gauge(
                "rl_insight_proxy_" + name,
                "Collection " + name,
                registry=metrics,
            )
            gauge.set(value)
        body = generate_latest(
            SimpleNamespace(collect=lambda: merged.values())
        ) + generate_latest(metrics)
        if len(body) > 16 * 1024 * 1024:
            raise RunError("Aggregated response budget exceeded", 503)
        return (503 if items and not successful else 200), body


_target_publications: dict[tuple[str, str, str, str], tuple[dict[str, Any], float]] = {}
_publication_lock = threading.Lock()
_publication_thread: threading.Thread | None = None


def _post_targets(base_url, payload):
    response = requests.post(
        base_url + "/api/v1/prometheus/targets",
        json=payload,
        timeout=10,
    )
    response.raise_for_status()
    result = response.json()
    ttl = result.get("ttl") if isinstance(result, dict) else None
    return (
        max(0.25, float(ttl) / 3)
        if isinstance(ttl, (int, float)) and 1 <= ttl <= 3600
        else None
    )


def _registration_retry(_base_url, _payload, error):
    code = getattr(error.response, "status_code", None)
    if code is not None and 400 <= code < 500 and code not in {408, 409, 429}:
        return None
    return 1.0


def _renew_targets():
    """One process-level renewal loop; no exporter-specific integration is needed."""
    global _publication_thread
    while True:
        with _publication_lock:
            if not _target_publications:
                _publication_thread = None
                return
            ready = [
                (key, value)
                for key, value in _target_publications.items()
                if value[1] <= time.monotonic()
            ]
        for key, (payload, _) in ready:
            delay = 1.0
            try:
                delay = _post_targets(key[0], payload)
            except requests.RequestException as exc:
                delay = _registration_retry(key[0], payload, exc)
                logger.warning("Target renewal failed: %s", exc)
            with _publication_lock:
                if (
                    key not in _target_publications
                    or _target_publications[key][0] is not payload
                ):
                    continue
                if delay is None:
                    _target_publications.pop(key, None)
                else:
                    _target_publications[key] = (payload, time.monotonic() + delay)
        time.sleep(0.1)


def _publish_task_targets(base_url, publisher_id, server_addresses, job_name, labels):
    global _publication_thread
    payload: dict[str, Any] = {
        "job_name": job_name or PrometheusScrape.TRAINER_METRICS_JOB,
        "targets": _build_target_payload(server_addresses, labels),
    }
    payload["publisher_id"] = publisher_id
    success, delay = True, None
    try:
        delay = _post_targets(base_url, payload)
    except requests.RequestException as exc:
        success = False
        delay = (
            _registration_retry(base_url, payload, exc)
            if "publisher_id" in payload
            else None
        )
        logger.warning("Target registration failed: %s", exc)
    if delay is not None:
        with _publication_lock:
            for target in payload["targets"]:
                key = (
                    base_url,
                    publisher_id,
                    payload["job_name"],
                    target["target"],
                )
                _target_publications[key] = (
                    {**payload, "targets": [target]},
                    time.monotonic() + delay,
                )
            if _publication_thread is None or not _publication_thread.is_alive():
                _publication_thread = threading.Thread(
                    target=_renew_targets, name="insight-targets", daemon=True
                )
                _publication_thread.start()
    return success


def _retire_task_targets(destination: str, publisher_id: str) -> None:
    """Stop renewing this sink and retain its final samples for one server TTL."""
    with _publication_lock:
        for key, (payload, _) in list(_target_publications.items()):
            if key[0] == destination and payload.get("publisher_id") == publisher_id:
                del _target_publications[key]
    try:
        with requests.Session() as session:
            session.trust_env = False
            response = session.delete(
                destination + "/api/v1/prometheus/targets/" + publisher_id,
                timeout=(2, 10),
            )
            response.raise_for_status()
    except requests.RequestException:
        logger.warning("Final task target collection failed; targets will expire")


def _scrape_timeout(request: Request) -> float:
    supplied = request.headers.get("X-Prometheus-Scrape-Timeout-Seconds")
    if supplied is None:
        return 9.0
    try:
        seconds = float(supplied)
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError
        return min(9.0, seconds - min(1.0, seconds * 0.2))
    except ValueError as exc:
        raise RunError("Invalid scrape timeout") from exc


def create_task_collector(conf: DictConfig) -> FastAPI:
    """One task, one pull endpoint; task identity belongs to the deployment."""
    registry = TaskTargets(
        float(OmegaConf.select(conf, "server.endpoint_ttl", default=30))
    )
    collection = PrometheusCollector(
        registry, labels=dict(OmegaConf.select(conf, "labels") or {})
    )

    @asynccontextmanager
    async def lifespan(_app):
        try:
            yield
        finally:
            await run_in_threadpool(collection.close)

    app = FastAPI(title="RL-Insight task collector", lifespan=lifespan)

    @app.exception_handler(RunError)
    async def run_error(_request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status)

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/api/v1/services")
    def services(client_version: str | None = Header(None, alias=SDK_VERSION_HEADER)):
        require_version(client_version, "Client")
        return {
            "status": "ok",
            "server_version": __version__,
            "task_metrics_path": "/metrics",
            "metrics_port": int(
                conf.server.get("metrics_port", conf.server.get("port", 18080))
            ),
            "labels": dict(OmegaConf.select(conf, "labels") or {}),
            "metrics_transport": "remote_write"
            if OmegaConf.select(conf, "prometheus.remote_write")
            else "pull",
        }

    @app.post("/api/v1/prometheus/targets")
    def register(payload: dict[str, Any] = Body(...)):
        return registry.register(payload)

    @app.delete("/api/v1/prometheus/targets/{publisher_id}")
    def retire(publisher_id: str):
        collection.retire(publisher_id)
        return {"status": "draining", "ttl": registry.ttl}

    @app.get("/metrics")
    def metrics(request: Request):
        code, body = collection.collect(timeout=_scrape_timeout(request))
        return Response(body, status_code=code, media_type="text/plain; version=0.0.4")

    return app
