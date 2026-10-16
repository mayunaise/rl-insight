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

"""Prometheus metric registry, ``/metrics`` HTTP server, and target registration helpers."""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import requests
import yaml
from omegaconf import DictConfig, OmegaConf
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from ..protocol import RunContext, endpoint_description, proof_headers
from ..server.network import format_host_port, local_addresses
from .constants import (
    MonitorBackend,
    MonitorEnv,
    MonitorPaths,
    PrometheusScrape,
    prometheus_targets_file_from_config,
)

logger = logging.getLogger(__file__)
logger.setLevel(logging.WARNING)

_pending_targets: list[
    tuple[list[str], str | None, list[Mapping[str, Any] | None] | None]
] = []
_pending_lock = threading.Lock()
_pending_probe: threading.Thread | None = None


def _wait_for_context() -> None:
    """Retry pending native targets for the lifetime of their worker process."""
    global _pending_probe
    from ..client.ray_monitor_client import current_job_context, current_job_config

    delay = 0.1
    while True:
        with _pending_lock:
            if not _pending_targets:
                _pending_probe = None
                return
        context = current_job_context()
        conf = current_job_config() if context is None else None
        if context or (conf is not None and conf.get("task_configuration", False)):
            flush_pending_targets(context)
        time.sleep(delay)
        delay = min(delay * 2, 2)


def _defer_targets(addresses, job, labels):
    global _pending_probe
    item = (
        list(addresses),
        job,
        [dict(x) if x else None for x in labels] if labels else None,
    )
    with _pending_lock:
        if item not in _pending_targets:
            if len(_pending_targets) >= 64:
                raise ValueError("Too many targets waiting for SDK initialization")
            _pending_targets.append(item)
        if _pending_probe is None or not _pending_probe.is_alive():
            _pending_probe = threading.Thread(
                target=_wait_for_context, name="insight-context", daemon=True
            )
            _pending_probe.start()


def flush_pending_targets(context: RunContext | None) -> None:
    with _pending_lock:
        pending = list(_pending_targets)
    for item in pending:
        if _register_targets(*item, context):
            with _pending_lock:
                if item in _pending_targets:
                    _pending_targets.remove(item)


__all__ = [
    "MetricRegistry",
    "PrometheusTarget",
    "PrometheusTargetStore",
    "metrics_asgi_app",
    "metrics_wsgi_app",
    "publish_metrics_endpoint",
    "start_metrics_http_server",
    "update_prometheus_config",
]


@dataclass(frozen=True)
class PrometheusTarget:
    """One scrape target plus optional labels for Prometheus static_configs."""

    target: str
    labels: Mapping[str, Any] = field(default_factory=dict)


class PrometheusTargetStore:
    """Maintain Prometheus scrape targets in a file_sd discovery file."""

    def __init__(
        self,
        config_file: str | Path,
        prometheus_port: int,
        targets_file: str | Path | None = None,
    ):
        self.config_file = Path(config_file).expanduser().resolve()
        self.targets_file = (
            Path(targets_file).expanduser().resolve()
            if targets_file is not None
            else self.config_file.with_name(PrometheusScrape.TARGETS_FILE_NAME)
        )
        self.prometheus_port = prometheus_port
        self._lock = threading.Lock()

    @classmethod
    def from_config(cls, conf: DictConfig) -> PrometheusTargetStore:
        runtime_dir = OmegaConf.select(conf, "server.runtime_dir")
        base = (
            Path(str(runtime_dir)).expanduser().resolve()
            if runtime_dir
            else (MonitorPaths.STATE_ROOT / "runtime").resolve()
        )
        prometheus_port = int(OmegaConf.select(conf, "prometheus.prometheus_port"))
        return cls(
            base / "prometheus.yml",
            prometheus_port,
            prometheus_targets_file_from_config(conf),
        )

    def register(
        self, job_name: str, targets: Sequence[PrometheusTarget]
    ) -> dict[str, Any]:
        incoming = {
            str(item.target): {str(k): str(v) for k, v in item.labels.items()}
            for item in targets
        }

        with self._lock, self._file_lock():
            target_map = self._read_targets()
            target_map.update(
                {(str(job_name), target): labels for target, labels in incoming.items()}
            )
            self._write_targets(target_map)

        return {
            "job_name": job_name,
            "target_count": sum(
                1 for stored_job, _target in target_map if stored_job == job_name
            ),
            "config_file": str(self.config_file),
            "targets_file": str(self.targets_file),
        }

    @contextmanager
    def _file_lock(self):
        """Serialize read-modify-write updates made by different processes."""
        self.targets_file.parent.mkdir(parents=True, exist_ok=True)
        lock_file = self.targets_file.with_name(f".{self.targets_file.name}.lock")
        with lock_file.open("a+", encoding="utf-8") as handle:
            if sys.platform == "win32":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if sys.platform == "win32":
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _write_targets(
        self, target_map: Mapping[tuple[str, str], Mapping[str, str]]
    ) -> None:
        groups = [
            {
                "targets": [target],
                "labels": {
                    **labels,
                    PrometheusScrape.DYNAMIC_JOB_LABEL: stored_job,
                },
            }
            for (stored_job, target), labels in sorted(target_map.items())
        ]
        payload = yaml.safe_dump(groups, sort_keys=False)
        tmp_path = self.targets_file.with_name(
            f".{self.targets_file.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        try:
            tmp_path.write_text(payload, encoding="utf-8")
            os.replace(tmp_path, self.targets_file)
        except BaseException:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            raise

    def _read_targets(self) -> dict[tuple[str, str], dict[str, str]]:
        if not self.targets_file.exists():
            return {}
        groups = yaml.safe_load(self.targets_file.read_text(encoding="utf-8")) or []
        if not isinstance(groups, list):
            raise ValueError("Prometheus file_sd targets must be a list")

        target_map: dict[tuple[str, str], dict[str, str]] = {}
        for group in groups:
            if not isinstance(group, Mapping):
                raise ValueError(
                    "Each Prometheus file_sd target group must be an object"
                )
            raw_labels = group.get("labels") or {}
            if not isinstance(raw_labels, Mapping):
                raise ValueError("Prometheus file_sd target labels must be an object")
            labels = {str(key): str(value) for key, value in raw_labels.items()}
            stored_job = labels.pop(PrometheusScrape.DYNAMIC_JOB_LABEL, "").strip()
            if not stored_job:
                raise ValueError(
                    "Prometheus file_sd target group is missing managed job label"
                )
            for target in group.get("targets") or []:
                target_map[(stored_job, str(target))] = dict(labels)
        return target_map

    def project_runs(self, endpoints, server_address, ended_runs=()):
        """Project active Runs and remove their managed direct targets under one lock."""
        from urllib.parse import urlsplit

        with self._lock, self._file_lock():
            direct = self._read_targets()
            addresses = {urlsplit(item["metrics_url"]).netloc for item in endpoints}
            direct = {
                key: labels
                for key, labels in direct.items()
                if key[1] not in addresses and labels.get("run_id") not in ended_runs
            }
            self._write_targets(direct)
            runs = sorted({item["run_id"] for item in endpoints})
            path = self.targets_file.with_name("run-targets.yml")
            temporary = path.with_suffix(".yml.tmp")
            payload = [
                {
                    "targets": [server_address],
                    "labels": {
                        "run_id": run_id,
                        "__metrics_path__": "/api/v1/runs/" + run_id + "/metrics",
                    },
                }
                for run_id in runs
            ]
            temporary.write_text(yaml.safe_dump(payload), encoding="utf-8")
            os.replace(temporary, path)

    def reload(self) -> bool:
        url = (
            "http://"
            + format_host_port(local_addresses()["loopback"], self.prometheus_port)
            + "/-/reload"
        )

        with requests.Session() as session:
            # bypass http_proxy for localhost Prometheus reload
            session.trust_env = False
            response = session.post(url, timeout=5)

        response.raise_for_status()
        return True


def _merge_labels(
    defaults: Mapping[str, Any] | None, overrides: Mapping[str, Any] | None
) -> dict[str, str]:
    """Merge Prometheus label dicts with string keys/values; ``overrides`` wins on duplicate keys.

    Args:
        defaults: Base labels (optional).
        overrides: Labels applied after defaults (optional).
    """
    out: dict[str, str] = {}
    if defaults:
        out.update({str(k): str(v) for k, v in defaults.items()})
    if overrides:
        out.update({str(k): str(v) for k, v in overrides.items()})
    return out


def metrics_wsgi_app(app, context: RunContext | None, incarnation: str):
    """Wrap a standard WSGI application with a stable exporter response identity."""
    headers = proof_headers(context, incarnation)

    def wrapped(environ, start_response):
        def respond(status, supplied, exc_info=None):
            clean = [
                (k, v)
                for k, v in supplied
                if k.lower() not in {key.lower() for key in headers}
            ]
            return start_response(status, clean + list(headers.items()), exc_info)

        return app(environ, respond)

    return wrapped


def metrics_asgi_app(app, context: RunContext | None, incarnation: str):
    """Wrap the public ASGI response boundary; no framework objects are inspected."""
    headers = [
        (key.lower().encode(), value.encode())
        for key, value in proof_headers(context, incarnation).items()
    ]

    async def wrapped(scope, receive, send):
        async def respond(message):
            if message["type"] == "http.response.start":
                message = dict(message)
                message["headers"] = [
                    (k, v)
                    for k, v in message.get("headers", [])
                    if k.lower() not in {key for key, _ in headers}
                ] + headers
            await send(message)

        await app(scope, receive, respond)

    return wrapped


class EndpointPublisher:
    """Own one endpoint lease and re-register it after server recovery."""

    def __init__(self, context, description, *, alive=None, retry_initial=False):
        self.context = context if isinstance(context, RunContext) else None
        self.description = endpoint_description(self.context, description)
        self._root = (
            context.server_url + "/api/v1/runs/" + context.run_id + "/endpoints"
            if self.context is not None
            else str(context).rstrip("/") + "/api/v1/endpoints"
        )
        self._alive = alive or (lambda: True)
        self._stop = threading.Event()
        self._session = requests.Session()
        self._session.trust_env = False
        self._request_lock = threading.RLock()
        self._lease = None
        self._interval = 1.0
        try:
            self._register()
        except requests.RequestException:
            if not retry_initial:
                self._session.close()
                raise
            logger.warning("Endpoint registration failed; will retry")
        except Exception:
            self._session.close()
            raise
        self._thread = threading.Thread(
            target=self._renew, name="insight-endpoint", daemon=True
        )
        self._thread.start()

    def _register(self):
        with self._request_lock:
            self._register_locked()

    def _register_locked(self):
        response = self._session.post(self._root, json=self.description, timeout=(2, 4))
        response.raise_for_status()
        lease = response.json()
        self._lease = lease["lease_id"]
        self._interval = max(0.25, lease["ttl"] / 3)
        self.description["labels"] = lease["labels"]

    def add_labels(self, labels):
        """Update descriptive labels through the same serialized registration path."""
        with self._request_lock:
            self.description["labels"].update(labels)
            try:
                self._register_locked()
            except requests.RequestException:
                self._lease = None
                logger.warning("Endpoint label publication failed; will retry")

    def _renew(self):
        while not self._stop.wait(self._interval) and self._alive():
            try:
                with self._request_lock:
                    if self._lease is None:
                        self._register_locked()
                        continue
                    response = self._session.post(
                        self._root + "/" + self._lease, timeout=(2, 4)
                    )
                    if response.status_code == 409:
                        self._register_locked()
                    else:
                        response.raise_for_status()
                        self.description["labels"] = response.json()["labels"]
            except requests.RequestException as exc:
                if getattr(exc.response, "status_code", None) == 409:
                    logger.warning("Endpoint publication stopped: %s", exc)
                    break
                logger.warning("Endpoint renewal failed; will retry: %s", exc)

    def close(self):
        self._stop.set()
        self._thread.join(7)
        if self._thread.is_alive():
            logger.warning("Endpoint renewal did not stop within its budget")
            return
        try:
            if self._lease:
                response = self._session.delete(
                    self._root + "/" + self._lease, timeout=(2, 3)
                )
                response.raise_for_status()
        except requests.RequestException:
            logger.warning("Endpoint revocation failed; lease will expire")
        finally:
            self._session.close()


def publish_metrics_endpoint(
    context: RunContext,
    metrics_url: str,
    *,
    component: str,
    instance: str,
    incarnation: str,
    labels=None,
    alive=None,
):
    """Publish a bound, proof-wrapped endpoint; retain the handle until shutdown."""
    return EndpointPublisher(
        context,
        {
            "metrics_url": metrics_url,
            "component": component,
            "instance": instance,
            "incarnation": incarnation,
            "labels": labels or {},
        },
        alive=alive,
    )


def start_metrics_http_server(
    port: int, addr: str = "", *, context: RunContext | None = None
):
    """Bind once and return the actual HTTP server/thread; port 0 allocates a port."""
    addr = addr.strip("[]") if addr else addr
    server, thread = start_http_server(port, addr=addr)
    if context:
        incarnation = uuid4().hex
        server.set_app(metrics_wsgi_app(server.get_app(), context, incarnation))
        try:
            server.publisher = publish_metrics_endpoint(
                context,
                "http://" + format_host_port(addr, server.server_port) + "/metrics",
                component="hub",
                instance="hub",
                incarnation=incarnation,
                labels={"job": PrometheusScrape.TRAINER_METRICS_JOB},
                alive=thread.is_alive,
            )
        except Exception:
            server.shutdown()
            server.server_close()
            raise
    return server, thread


class MetricRegistry:
    """Lazy registry of ``prometheus_client`` Counter/Gauge/Histogram objects keyed by metric name + label set."""

    def __init__(
        self, namespace: str = "", subsystem: str = "", *, registry=None
    ) -> None:
        """
        Args:
            namespace: Passed as Prometheus metric namespace (prefix segment).
            subsystem: Optional second prefix segment from ``prometheus_client`` API.
        """
        self._namespace = namespace
        self._subsystem = subsystem
        self._registry_options = {} if registry is None else {"registry": registry}
        self._counters: dict[tuple[str, tuple[str, ...]], object] = {}
        self._gauges: dict[tuple[str, tuple[str, ...]], object] = {}
        self._histograms: dict[tuple[str, tuple[str, ...]], object] = {}

    def _get_or_create_counter(
        self, name: str, documentation: str, label_names: tuple[str, ...]
    ):
        """Return a cached or new ``Counter`` for ``(name, label_names)``."""
        key = (name, label_names)
        if key not in self._counters:
            self._counters[key] = Counter(
                name,
                documentation,
                labelnames=label_names,
                namespace=self._namespace,
                subsystem=self._subsystem,
                **self._registry_options,
            )
        return self._counters[key]

    def _get_or_create_gauge(
        self, name: str, documentation: str, label_names: tuple[str, ...]
    ):
        """Return a cached or new ``Gauge`` for ``(name, label_names)``."""
        key = (name, label_names)
        if key not in self._gauges:
            self._gauges[key] = Gauge(
                name,
                documentation,
                labelnames=label_names,
                namespace=self._namespace,
                subsystem=self._subsystem,
                **self._registry_options,
            )
        return self._gauges[key]

    def _get_or_create_histogram(
        self,
        name: str,
        documentation: str,
        label_names: tuple[str, ...],
        buckets: tuple[float, ...] | None,
    ):
        """Return a cached or new ``Histogram`` for ``(name, label_names)`` with optional bucket boundaries."""
        key = (name, label_names)
        if key not in self._histograms:
            kw: dict[str, Any] = {}
            if buckets is not None:
                kw["buckets"] = buckets
            self._histograms[key] = Histogram(
                name,
                documentation,
                labelnames=label_names,
                namespace=self._namespace,
                subsystem=self._subsystem,
                **kw,
                **self._registry_options,
            )
        return self._histograms[key]

    def count(
        self,
        name: str,
        documentation: str,
        amount: float,
        defaults: Mapping[str, Any] | None = None,
        labels: Mapping[str, Any] | None = None,
    ) -> None:
        """Increment a counter, merging ``defaults`` and ``labels`` into the label set."""
        merged = _merge_labels(defaults, labels)
        names = tuple(sorted(merged.keys()))
        counter = self._get_or_create_counter(name, documentation, names)
        if merged:
            counter.labels(**merged).inc(amount)
        else:
            counter.inc(amount)

    def value(
        self,
        name: str,
        documentation: str,
        value: float,
        defaults: Mapping[str, Any] | None = None,
        labels: Mapping[str, Any] | None = None,
    ) -> None:
        """Set a gauge to ``value`` with merged labels."""
        merged = _merge_labels(defaults, labels)
        names = tuple(sorted(merged.keys()))
        gauge = self._get_or_create_gauge(name, documentation, names)
        if merged:
            gauge.labels(**merged).set(value)
        else:
            gauge.set(value)

    def distribution(
        self,
        name: str,
        documentation: str,
        value: float,
        defaults: Mapping[str, Any] | None = None,
        labels: Mapping[str, Any] | None = None,
        buckets: tuple[float, ...] | None = None,
    ) -> None:
        """Observe ``value`` on a histogram with merged labels and optional ``buckets``."""
        merged = _merge_labels(defaults, labels)
        names = tuple(sorted(merged.keys()))
        histogram = self._get_or_create_histogram(name, documentation, names, buckets)
        if merged:
            histogram.labels(**merged).observe(value)
        else:
            histogram.observe(value)


_push_bootstrap_lock = threading.Lock()
_push_bootstrap_attempted = False


def _bootstrap_push_registry():
    """Start the push backend in a process without an explicit ``init()``.

    Some integrations (e.g. Ray rollout actors in verl) call
    :func:`update_prometheus_config` from a worker process that never ran
    ``rl_insight.init()``. For the direct-emit push backend the client needs no
    external server, so it is safe to construct it lazily here: building the
    push client also creates its :class:`PrometheusPoller` and publishes the
    process-wide scrape target registry. Returns that registry, or ``None``
    when the configured backend is not ``push`` / bootstrap is unavailable.
    """
    global _push_bootstrap_attempted
    with _push_bootstrap_lock:
        from ..client.push.poller import get_active_registry

        existing = get_active_registry()
        if existing is not None:
            return existing
        if _push_bootstrap_attempted:
            return None
        _push_bootstrap_attempted = True
        try:
            from .. import api as _api
            from .monitor_config_loader import load_monitor_config

            conf = load_monitor_config(None)
            backend = str(OmegaConf.select(conf, "server.backend") or "").strip()
            if backend != MonitorBackend.PUSH:
                return None
            _api.init()
        except Exception:
            logger.exception("[rl-insight] push backend in-process bootstrap failed")
            return None
        return get_active_registry()


def update_prometheus_config(
    server_addresses: list[str],
    job_name: str | None = None,
    labels: list[Mapping[str, Any] | None] | None = None,
    *,
    context: RunContext | None = None,
    identity_in_metrics: bool = False,
) -> None:
    """Register trainer metrics endpoints with the RL-Insight server.

    The RL-Insight server writes these targets into its file_sd discovery file;
    Prometheus observes the update automatically. ``server_addresses`` should
    contain scrape targets in ``host:port`` or ``[ipv6]:port`` form, not full URLs.

    Args:
        server_addresses: Prometheus scrape targets exposed by trainer-side
            metric HTTP servers.
        job_name: Optional Prometheus scrape job name. Defaults to the managed
            trainer metrics job.
        labels: Optional per-target labels. When provided, its length must match
            ``server_addresses``.
    """
    if not server_addresses:
        logger.warning("[rl-insight] No server addresses available to register")
        return
    if labels is not None and len(labels) != len(server_addresses):
        raise ValueError(
            "labels length must match server_addresses length: "
            f"{len(labels)} != {len(server_addresses)}"
        )

    # Push targets stay local and do not require a server or RunContext.
    from ..client.push.poller import get_active_registry

    registry = get_active_registry()
    if registry is None:
        registry = _bootstrap_push_registry()
    if registry is not None:
        registry.set_targets(server_addresses, labels, job_name=job_name)
        logger.info(
            "[rl-insight] Registered %d push scrape targets in-process",
            len(server_addresses),
        )
        return

    if context is None:
        from ..client.ray_monitor_client import register_job_metrics

        if register_job_metrics(server_addresses, job_name, labels):
            return
    initialized = True
    if context is None:
        from ..api import _client_initialized, get_context

        context = get_context()
        initialized = _client_initialized()
    base_url = (
        context.server_url
        if context
        else str(os.environ.get(MonitorEnv.SERVER_URL, "")).strip().rstrip("/")
    )
    if not base_url:
        import ray

        if ray.is_initialized():
            _defer_targets(server_addresses, job_name, labels)
            return
        logger.error(
            "[rl-insight] RL-Insight server URL is required; "
            "set %s to register Prometheus targets",
            MonitorEnv.SERVER_URL,
        )
        return

    if context is None:
        if not initialized:
            from ..server.http_api import get_server_services

            if get_server_services().get("run_context_version") == 1:
                # Wait for logger initialization; never create a Run from an address.
                from ..client.ray_monitor_client import current_job_context

                context = current_job_context()
                if context is None:
                    _defer_targets(server_addresses, job_name, labels)
                    return

    _register_targets(
        server_addresses,
        job_name,
        labels,
        context,
        identity_in_metrics=identity_in_metrics,
    )


def _register_targets(
    server_addresses: list[str],
    job_name: str | None,
    labels: list[Mapping[str, Any] | None] | None,
    context: RunContext | None,
    *,
    identity_in_metrics: bool = False,
) -> bool:
    """Report success so deferred targets remain queued after a failed request."""
    if context is None:
        from ..client.ray_monitor_client import register_job_metrics

        try:
            if register_job_metrics(server_addresses, job_name, labels):
                return True
        except Exception:
            logger.exception("[rl-insight] Deferred job target registration failed")
            return False
    base_url = (
        context.server_url
        if context
        else str(os.environ.get(MonitorEnv.SERVER_URL, "")).strip().rstrip("/")
    )
    if not base_url:
        return False
    payload: dict[str, Any] = {
        "job_name": job_name or PrometheusScrape.TRAINER_METRICS_JOB,
        "targets": _build_target_payload(server_addresses, labels),
    }
    if context:
        payload["run_context"] = context.to_dict()
        payload["identity_in_metrics"] = identity_in_metrics
    url = f"{base_url}/api/v1/prometheus/targets"
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        print(
            f"[rl-insight] Registered {len(server_addresses)} Prometheus targets "
            f"with RL-Insight server (job_name={payload['job_name']})"
        )
        return True
    except requests.RequestException as exc:
        logger.error(
            "[rl-insight] Failed to register Prometheus targets at %s: %s", url, exc
        )

    return False


def _build_target_payload(
    server_addresses: list[str],
    labels: list[Mapping[str, Any] | None] | None = None,
) -> list[dict[str, Any]]:
    if labels is None:
        labels = [None] * len(server_addresses)

    targets: list[dict[str, Any]] = []
    for address, target_labels in zip(server_addresses, labels):
        item: dict[str, Any] = {"target": str(address)}
        if target_labels:
            item["labels"] = {
                str(key): str(value) for key, value in target_labels.items()
            }
        targets.append(item)
    return targets
