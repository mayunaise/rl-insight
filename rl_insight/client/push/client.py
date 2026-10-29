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

"""Push backend client: map monitor events to sink calls and fan out by stream."""

from __future__ import annotations

import atexit
import json
import logging
import os
import subprocess
import sys
import time
from uuid import uuid4
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import psutil
from omegaconf import DictConfig, ListConfig, OmegaConf

from ...utils.constants import MonitorEventKind
from ...utils.protocol import metric_labels
from ..base import MonitorClient
from .driver import build_sink, parse_sink_driver
from .poller import PrometheusPoller, set_active_registry
from .sinks.base import PushSink
from .tags import collect_sink_tags

if TYPE_CHECKING:
    from ...server.runtime import StartedStack
    from ...server.services import ServerServiceManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

DEFAULT_STREAMS = ("metric", "trace")
DEFAULT_METRIC_PREFIX = "trainer."
DEFAULT_TRACE_PREFIX = "trace."
DEFAULT_ROLLOUT_PREFIX = "rollout."
DEFAULT_ROLLOUT_INTERVAL = 15.0

STREAM_METRIC = "metric"
STREAM_TRACE = "trace"
STREAM_ROLLOUT = "rollout"


@dataclass(frozen=True)
class _MetricRule:
    name: str
    method: str | None
    scale: float
    tags: dict[str, str]


@dataclass
class _BoundSink:
    sink: PushSink
    streams: frozenset[str]
    rollout_prefix: str
    tags: dict[str, str]
    metric_rules: dict[str, _MetricRule] | None = None
    labels: dict[str, str] = field(default_factory=dict)
    alive: bool = True


_OP_METHOD = {
    "counter": "emit_counter",
    "store": "emit_store",
    "timer": "emit_timer",
}


def _scalar_string_tags(tags: Any) -> dict[str, str]:
    """Keep only scalar tag values and stringify keys/values (drop sequences)."""
    out: dict[str, str] = {}
    for key, value in (tags or {}).items():
        if isinstance(value, (str, bool, int, float)) or value is None:
            out[str(key)] = str(value)
    return out


class PushMonitorClient(MonitorClient):
    """Forward events in-process to configured sinks, routing by data stream."""

    def __init__(
        self,
        metric_prefix: str,
        trace_prefix: str,
        bound: list[_BoundSink],
        poller: PrometheusPoller | None,
        *,
        conf: DictConfig,
    ) -> None:
        self.conf = conf
        self._services: ServerServiceManager | None = None
        self._stack: StartedStack | None = None
        self._watcher: subprocess.Popen[bytes] | None = None
        self._metric_prefix = metric_prefix
        self._trace_prefix = trace_prefix
        self._bound = bound
        self._poller = poller
        self._closed = False
        atexit.register(self.close)

    # -- MonitorClient -----------------------------------------------------
    def apply_event(self, event: dict[str, Any]) -> None:
        kind = str(event.get("kind"))
        if kind == MonitorEventKind.TRACE:
            self._route_trace(event)
        else:
            self._route_metric(kind, event)

    @property
    def metrics_endpoint(self) -> str:
        return next(
            (
                b.sink.metrics_endpoint
                for b in self._bound
                if hasattr(b.sink, "metrics_endpoint")
            ),
            "",
        )

    def register_metrics(self, addresses, job, labels) -> bool:
        from .poller import get_active_registry

        registry = get_active_registry()
        if registry is not None:
            registry.set_targets(addresses, labels, job_name=job)
        # This output owns the targets even when native metric collection is disabled.
        return True

    def _start_services(self) -> None:
        """Start a native Agent only after the output has bound its metrics port."""
        from ...server.dependencies import prometheus_agent_enabled

        endpoint = self.metrics_endpoint
        if (
            OmegaConf.select(self.conf, "server.metrics_port") is not None
            and not endpoint
        ):
            raise ValueError("Configured Prometheus metrics endpoint did not start")
        if endpoint:
            OmegaConf.update(
                self.conf, "server.metrics_endpoint", endpoint, force_add=True
            )
        if prometheus_agent_enabled(self.conf):
            from ...server.services import ServerServiceManager

            self.conf.prometheus.metrics_endpoint = endpoint
            self.conf.server.metrics_port = urlsplit(endpoint).port
            self._services = ServerServiceManager(self.conf)
            self._stack = self._services.start(
                detach=True, attach_logs=False, auto_port=True
            )
            if self._stack is None:
                raise RuntimeError("Prometheus Agent runtime is already in use")
            owner = psutil.Process()
            metadata = {
                "owner": [owner.pid, owner.create_time()],
                "services": [
                    [
                        service.process.pid,
                        psutil.Process(service.process.pid).create_time(),
                    ]
                    for service in self._stack.services
                ],
                "state_file": str(self._stack.state_file),
            }
            with (Path(self.conf.server.log_dir) / "watchdog.log").open("ab") as log:
                self._watcher = subprocess.Popen(
                    [
                        sys.executable,
                        "-c",
                        "from rl_insight.client.push.client import _watch_push_services;import sys;_watch_push_services(sys.argv[1])",
                        json.dumps(metadata),
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=os.name == "posix",
                )
        if endpoint:
            logger.warning("[rl-insight] SDK metrics ready: %s", endpoint)

    # -- routing -----------------------------------------------------------
    @staticmethod
    def _method_for_kind(kind: str) -> str:
        if kind == MonitorEventKind.COUNTER:
            return "emit_counter"
        if kind == MonitorEventKind.GAUGE:
            return "emit_store"
        if kind == MonitorEventKind.HISTOGRAM:
            return "emit_histogram"
        return "emit_timer"

    def _route_metric(self, kind: str, event: dict[str, Any]) -> None:
        raw_name = str(event.get("name", ""))
        value = float(event.get("value", 0.0))
        event_tags = _scalar_string_tags(event.get("labels"))
        default_method = self._method_for_kind(kind)
        for bound in list(self._bound):
            if not bound.alive or STREAM_METRIC not in bound.streams:
                continue
            rule = (
                None if bound.metric_rules is None else bound.metric_rules.get(raw_name)
            )
            if bound.metric_rules is not None and rule is None:
                continue  # whitelist sink: unmapped trainer metrics are dropped
            if rule is None:
                suffix = f"{self._metric_prefix}{raw_name}"
                method = default_method
                out_value = value
                tags = {**bound.tags, **event_tags}
            else:
                suffix = rule.name
                method = rule.method or default_method
                out_value = value * rule.scale
                tags = {**bound.tags, **event_tags, **rule.tags}
            self._safe(bound, method, suffix, out_value, {**tags, **bound.labels})

    def _route_trace(self, event: dict[str, Any]) -> None:
        start_ns = int(event.get("start_time_ns", 0))
        end_ns = int(event.get("end_time_ns", 0))
        suffix = f"{self._trace_prefix}{event.get('name', '')}"
        # Keep the raw attributes (including sequences) so a tracing sink can
        # reconstruct the span; metrics-only sinks collapse to a duration timer
        # via PushSink.emit_span's default.
        event_attrs = dict(event.get("attributes") or {})
        for bound in list(self._bound):
            if not bound.alive or STREAM_TRACE not in bound.streams:
                continue
            attributes = {**bound.tags, **event_attrs, **bound.labels}
            self._safe(bound, "emit_span", suffix, start_ns, end_ns, attributes)

    def emit_rollout(
        self, name: str, op: str, value: float, tags: dict[str, str]
    ) -> None:
        """Sink callback used by the rollout poller for translated engine metrics."""
        method = {
            "counter": "emit_counter",
            "store": "emit_store",
            "timer": "emit_timer",
        }.get(op, "emit_store")
        for bound in list(self._bound):
            if not bound.alive or STREAM_ROLLOUT not in bound.streams:
                continue
            suffix = f"{bound.rollout_prefix}{name}"
            merged = {**bound.tags, **dict(tags or {}), **bound.labels}
            self._safe(bound, method, suffix, float(value), merged)

    def _safe(self, bound: _BoundSink, method: str, *args: Any) -> None:
        try:
            getattr(bound.sink, method)(*args)
        except Exception as exc:  # noqa: BLE001 - a sink must never break training
            if bound.alive:
                logger.warning(
                    "[rl-insight] push sink %r failed once (%s); disabling it.",
                    bound.sink.__class__.__name__,
                    exc,
                )
                bound.alive = False
                self._bound = [b for b in self._bound if b.alive]
                try:
                    bound.sink.close()
                except Exception:
                    logger.debug(
                        "[rl-insight] failed sink cleanup failed", exc_info=True
                    )

    def close(self) -> None:
        """Stop polling and flush sinks; idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._poller is not None:
            self._poller.stop()
        set_active_registry(None)
        for bound in self._bound:
            try:
                bound.sink.close()
            except Exception:
                logger.debug("[rl-insight] sink close failed", exc_info=True)
        try:
            if self._stack is not None and self._services is not None:
                self._services.stop()
        except Exception:
            logger.warning("[rl-insight] Agent cleanup failed", exc_info=True)
        finally:
            if self._watcher is not None:
                try:
                    self._watcher.terminate()
                    self._watcher.wait(timeout=5)
                except (OSError, subprocess.TimeoutExpired):
                    logger.debug(
                        "[rl-insight] Agent watcher cleanup failed", exc_info=True
                    )
            atexit.unregister(self.close)


def _streams(sink_conf: Any) -> frozenset[str]:
    raw = OmegaConf.select(sink_conf, "streams")
    if raw is None:
        return frozenset(DEFAULT_STREAMS)
    if not isinstance(raw, (list, tuple, ListConfig)):
        raise ValueError("Sink streams must be a list")
    streams = frozenset(str(item) for item in raw)
    if (
        not streams
        or len(streams) != len(raw)
        or not streams <= {STREAM_METRIC, STREAM_TRACE, STREAM_ROLLOUT}
    ):
        raise ValueError("Invalid sink streams")
    return streams


def _select_or(conf: Any, key: str, default: str) -> str:
    value = OmegaConf.select(conf, key)
    return default if value is None else str(value)


_VALID_OPS = frozenset(_OP_METHOD)


def _metric_mapping(sink_conf: Any) -> dict[str, _MetricRule] | None:
    """Parse a sink's ``metric_mapping`` list into ``{source: rule}``.

    When present, only trainer scalar events whose raw name matches a
    ``source`` are emitted to that sink (whitelist), using the mapped output
    ``name`` and optional ``op`` (store|counter|timer), ``scale`` and static
    ``tags``. Returns ``None`` when the sink keeps default prefix passthrough.

    Raises:
        ValueError: A rule is missing ``source``/``name`` or uses an unknown
            ``op``/non-numeric ``scale``. Callers decide whether to degrade.
    """
    raw = OmegaConf.select(sink_conf, "metric_mapping")
    if raw is None:
        return None
    rules: dict[str, _MetricRule] = {}
    for index, item in enumerate(raw):
        source = OmegaConf.select(item, "source")
        name = OmegaConf.select(item, "name")
        if source is None or str(source) == "" or name is None or str(name) == "":
            raise ValueError(
                f"metric_mapping[{index}] requires non-empty 'source' and 'name'"
            )
        op = OmegaConf.select(item, "op")
        if op is not None and str(op) not in _VALID_OPS:
            raise ValueError(
                f"metric_mapping[{index}] has invalid op {str(op)!r}; "
                f"expected one of {sorted(_VALID_OPS)}"
            )
        method = None if op is None else _OP_METHOD[str(op)]
        try:
            scale = float(OmegaConf.select(item, "scale", default=1.0))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"metric_mapping[{index}] 'scale' must be numeric"
            ) from exc
        tags = _scalar_string_tags(OmegaConf.select(item, "tags"))
        rules[str(source)] = _MetricRule(
            name=str(name), method=method, scale=scale, tags=tags
        )
    return rules


def _bind_sink(sink: PushSink, sink_conf: Any) -> _BoundSink | None:
    """Bind a loaded sink to its parsed config; degrade on bad config.

    Configuration errors (invalid streams or ``metric_mapping``) never crash
    training: the offending sink is skipped with a warning, mirroring the
    driver-load failure path in :func:`driver.build_sink`.
    """
    name = str(OmegaConf.select(sink_conf, "name") or sink.__class__.__name__)
    try:
        return _BoundSink(
            sink=sink,
            streams=_streams(sink_conf),
            rollout_prefix=_select_or(
                sink_conf, "rollout_name_prefix", DEFAULT_ROLLOUT_PREFIX
            ),
            tags=collect_sink_tags(sink_conf),
            labels={
                str(k): str(v)
                for k, v in dict(OmegaConf.select(sink_conf, "labels") or {}).items()
            },
            metric_rules=_metric_mapping(sink_conf),
        )
    except Exception as exc:  # noqa: BLE001 - bad config disables one sink only
        logger.warning(
            "[rl-insight] push sink %r configured incorrectly (%s); skipping it.",
            name,
            exc,
        )
        return None


def _parse_interval(push_conf: Any) -> float:
    """Read ``rollout.interval_seconds``; fall back to the default when invalid."""
    raw = OmegaConf.select(push_conf, "rollout.interval_seconds")
    if raw is None:
        return DEFAULT_ROLLOUT_INTERVAL
    try:
        interval = float(raw)
    except (TypeError, ValueError):
        logger.warning(
            "[rl-insight] push rollout.interval_seconds=%r is not numeric; "
            "using default %ss.",
            raw,
            DEFAULT_ROLLOUT_INTERVAL,
        )
        return DEFAULT_ROLLOUT_INTERVAL
    if interval <= 0:
        logger.warning(
            "[rl-insight] push rollout.interval_seconds=%s must be positive; "
            "using default %ss.",
            interval,
            DEFAULT_ROLLOUT_INTERVAL,
        )
        return DEFAULT_ROLLOUT_INTERVAL
    return interval


def create_push_monitor_client(conf: DictConfig) -> PushMonitorClient | None:
    """Build the push client from ``push`` config; return ``None`` when disabled.

    Returns ``None`` (monitoring off) when there is no ``push`` section, no
    usable sink, or every driver fails to load.
    """
    if conf.get("task_config") is not None:
        from ...utils.constants import MonitorPaths
        from ...utils.monitor_config_loader import expand_task_config

        conf = expand_task_config(
            {"rl_insight": conf.task_config},
            runtime_dir=MonitorPaths.STATE_ROOT / "tasks" / uuid4().hex,
        )
    push_conf = OmegaConf.select(conf, "push")
    sink_confs = OmegaConf.select(push_conf, "sinks") if push_conf is not None else None
    if not sink_confs:
        logger.warning("[rl-insight] push backend selected but no sinks configured.")
        return None

    if push_conf.get("validate_sinks", False):
        sink_confs = OmegaConf.create(
            _task_sinks(
                OmegaConf.to_container(sink_confs, resolve=True),
                metric_labels(conf.get("labels", {})),
            )
        )
    bound: list[_BoundSink] = []
    for sink_conf in sink_confs:
        sink = build_sink(sink_conf)
        if sink is None:
            continue
        binding = _bind_sink(sink, sink_conf)
        if binding is not None:
            bound.append(binding)
        else:
            try:
                sink.close()
            except Exception:
                logger.warning(
                    "[rl-insight] failed to close rejected sink", exc_info=True
                )

    if not bound:
        logger.warning("[rl-insight] push backend has no usable sinks; disabled.")
        return None

    metric_prefix = _select_or(push_conf, "metric_prefix", DEFAULT_METRIC_PREFIX)
    trace_prefix = _select_or(push_conf, "trace_prefix", DEFAULT_TRACE_PREFIX)

    rules = OmegaConf.select(push_conf, "rollout.metrics")
    interval = _parse_interval(push_conf)
    poller: PrometheusPoller | None = None
    client: PushMonitorClient
    target_registries = [
        b.sink.target_registry for b in bound if hasattr(b.sink, "target_registry")
    ]
    if target_registries and (rules or len(target_registries) > 1):
        for binding in bound:
            binding.sink.close()
        logger.warning(
            "[rl-insight] raw Prometheus export requires one collector and no "
            "rollout translation rules; monitoring disabled."
        )
        return None
    if target_registries:
        set_active_registry(target_registries[0])
    elif rules and any(STREAM_ROLLOUT in b.streams for b in bound):
        poller = PrometheusPoller(
            rules=rules,
            interval_seconds=interval,
            emit=lambda name, op, value, tags: client.emit_rollout(
                name, op, value, dict(tags)
            ),
        )
        set_active_registry(poller.targets)
    else:
        set_active_registry(None)

    client = PushMonitorClient(metric_prefix, trace_prefix, bound, poller, conf=conf)
    try:
        client._start_services()
        if poller is not None:
            poller.start()
        return client
    except BaseException:
        client.close()
        raise


def _task_sinks(raw: Any, labels: dict[str, str]) -> list[dict[str, Any]]:
    """Normalize only shared routing and identity; leave driver options opaque."""
    if not isinstance(raw, list) or not raw:
        raise ValueError("sinks requires a non-empty list")
    names, configurations = set(), set()
    for sink in raw:
        if not isinstance(sink, dict):
            raise ValueError("Each sink must be an object")
        module, factory = parse_sink_driver(sink.get("driver"))
        sink["driver"] = f"{module}:{factory}"
        sink["streams"] = sorted(_streams(OmegaConf.create(sink)))
        supplied = metric_labels(sink.get("labels", {}))
        if any(
            key in supplied and supplied[key] != value for key, value in labels.items()
        ):
            raise ValueError("Sink labels conflict with task identity")
        sink["labels"] = {**supplied, **labels}
        name = sink.get("name", sink["driver"])
        fingerprint = json.dumps(
            {key: value for key, value in sink.items() if key != "name"}, sort_keys=True
        )
        if (
            not isinstance(name, str)
            or not name.strip()
            or name in names
            or fingerprint in configurations
        ):
            raise ValueError("Duplicate sink configuration or name")
        names.add(name)
        configurations.add(fingerprint)
    return raw


def _watch_push_services(raw):
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
