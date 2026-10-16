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

"""Load and merge RL-Insight monitor configuration."""

from __future__ import annotations

import logging
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from omegaconf import DictConfig, OmegaConf

from .constants import MonitorBackend, MonitorDefaults, MonitorEnv, MonitorPaths
from ..protocol import metric_labels

logger = logging.getLogger(__name__)

_TRAINING_MONITOR_DEFAULTS = OmegaConf.create(
    {
        "server": {
            "namespace": MonitorDefaults.NAMESPACE,
            "backend": MonitorBackend.RAY,
            "url": "",
        },
        "prometheus": {
            "metrics_report_port": MonitorDefaults.METRICS_REPORT_PORT,
        },
    }
)
__all__ = [
    "load_monitor_config",
    "load_server_config_file",
]


def _copy_defaults() -> DictConfig:
    """Return a fresh copy of the built-in training defaults."""
    return OmegaConf.create(
        OmegaConf.to_container(_TRAINING_MONITOR_DEFAULTS, resolve=True)
    )


def _merge_external_config(base: DictConfig) -> DictConfig:
    """Merge the YAML file pointed to by ``RL_INSIGHT_CONFIG`` (if any) onto ``base``.

    A missing/unreadable file only logs a warning; monitoring must never crash
    training because of a bad config path.
    """
    raw_path = os.environ.get(MonitorEnv.CONFIG_PATH, "").strip()
    if not raw_path:
        return base
    path = Path(raw_path).expanduser()
    if not path.is_file():
        logger.warning(
            "[rl-insight] %s points to %s, but no such file exists; ignoring.",
            MonitorEnv.CONFIG_PATH,
            path,
        )
        return base
    try:
        external = OmegaConf.load(str(path))
    except Exception as exc:  # noqa: BLE001 - config errors must not break training
        logger.warning(
            "[rl-insight] Failed to load %s (%s); ignoring external config.",
            path,
            exc,
        )
        return base
    return OmegaConf.merge(base, external)


def load_monitor_config(
    config: Mapping[str, Any] | DictConfig | None = None,
) -> DictConfig:
    """Load monitor config with layered overrides.

    Precedence (lowest to highest): built-in defaults, the external YAML named
    by ``RL_INSIGHT_CONFIG``, the explicit ``config`` argument (``init(config)``),
    and finally the ``RL_INSIGHT_SERVER_URL`` / ``RL_INSIGHT_SERVER_BACKEND``
    environment variables for ``server.url`` / ``server.backend``.

    Args:
        config: Partial mapping or ``DictConfig`` merged on top of defaults and
            external file config; may be ``None``.

    Returns:
        Fully merged config with environment variable overrides applied.
    """
    if config is not None:
        source = config if OmegaConf.is_config(config) else OmegaConf.create(config)
        node = source.get("rl_insight", source)
        if isinstance(node, DictConfig) and (
            "rl_insight" in source
            or any(key in node for key in ("enabled", "metrics", "traces", "sinks"))
        ):
            if os.environ.get(MonitorEnv.CONFIG_PATH):
                raise ValueError(
                    "Training configuration conflicts with RL_INSIGHT_CONFIG"
                )
            expanded = expand_task_config(
                {"rl_insight": node}, runtime_dir=MonitorPaths.STATE_ROOT / "tasks"
            )
            if expanded is None:
                return OmegaConf.merge(_copy_defaults(), {"enabled": False})
            inherited_url = (
                os.environ.get(MonitorEnv.SERVER_URL, "").strip().rstrip("/")
            )
            if inherited_url and inherited_url != str(expanded.server.url).rstrip("/"):
                raise ValueError(
                    "Server URL environment conflicts with training configuration"
                )
            if os.environ.get(MonitorEnv.SERVER_BACKEND):
                raise ValueError("Training configuration selects its own outputs")
            return OmegaConf.merge(
                _copy_defaults(),
                {
                    "task_options": OmegaConf.to_container(node, resolve=True),
                    "task_signature": hashlib.sha256(
                        json.dumps(
                            OmegaConf.to_container(node, resolve=True), sort_keys=True
                        ).encode()
                    ).hexdigest(),
                    "labels": expanded.labels,
                },
            )
    merged = _merge_external_config(_copy_defaults())

    if config is not None:
        user = (
            OmegaConf.create(OmegaConf.to_container(config, resolve=True))
            if OmegaConf.is_config(config)
            else OmegaConf.create(dict(config))
        )
        if OmegaConf.select(merged, "task_configuration", default=False):
            missing = object()
            for key in ("server", "push", "labels"):
                if key in user and not isinstance(user[key], DictConfig):
                    raise ValueError(
                        "Explicit monitor configuration conflicts with task outputs"
                    )
            for key in ("server.backend", "server.url", "push.sinks"):
                supplied = OmegaConf.select(user, key, default=missing)
                if supplied is not missing and supplied != OmegaConf.select(
                    merged, key
                ):
                    raise ValueError(
                        "Explicit monitor configuration conflicts with task outputs"
                    )
            for key, value in dict(OmegaConf.select(merged, "labels") or {}).items():
                supplied = OmegaConf.select(user, "labels", default={})
                if key in supplied and supplied[key] != value:
                    raise ValueError(
                        "Explicit monitor labels conflict with task identity"
                    )
        merged = OmegaConf.merge(merged, user)

    if url := os.environ.get(MonitorEnv.SERVER_URL):
        if (
            OmegaConf.select(merged, "task_configuration", default=False)
            and str(url).strip() != merged.server.url
        ):
            raise ValueError("Server URL environment conflicts with task configuration")
        merged.server.url = str(url).strip()
    if backend := os.environ.get(MonitorEnv.SERVER_BACKEND, "").strip():
        if (
            OmegaConf.select(merged, "task_configuration", default=False)
            and backend != merged.server.backend
        ):
            raise ValueError("Backend environment conflicts with task configuration")
        merged.server.backend = backend
    return merged


def load_server_config_file(config_path: str | Path | None = None) -> DictConfig:
    """Load server YAML used by ``rl-insight server start/stop``.

    Args:
        config_path: YAML file path; default is the bundled ``config/config.yaml``.

    Returns:
        Loaded server config.
    """
    yaml_path = (
        MonitorPaths.CONFIG_FILE.resolve()
        if config_path is None
        else Path(config_path).expanduser().resolve()
    )
    if config_path is None:
        user_conf = OmegaConf.load(str(yaml_path))
    else:
        default_conf = OmegaConf.load(str(MonitorPaths.CONFIG_FILE.resolve()))
        user_conf = OmegaConf.merge(default_conf, OmegaConf.load(str(yaml_path)))
    conf = OmegaConf.merge(
        OmegaConf.create({"service_root": str(yaml_path.parent)}),
        user_conf,
    )
    conf = OmegaConf.create(OmegaConf.to_container(conf, resolve=True))
    del conf.service_root
    return conf


def _http_url(value: Any, field: str) -> str:
    try:
        if not isinstance(value, str) or not value.strip():
            raise ValueError
        parsed = urlsplit(value.strip())
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.fragment
            or any(c.isspace() for c in value)
            or (parsed.port is not None and not 1 <= parsed.port <= 65535)
        ):
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} requires a valid HTTP(S) URL") from exc
    return value.strip().rstrip("/")


_PROMETHEUS_SINK = "rl_insight.client.push.sinks.prometheus:create_sink"
_OTLP_SINK = "rl_insight.client.push.sinks.otlp:create_sink"


def _task_sinks(raw: Any, labels: dict[str, str]) -> list[dict[str, Any]]:
    """Validate routing/identity; leave driver-specific options to its factory."""
    if not isinstance(raw, list) or not raw:
        raise ValueError("sinks requires a non-empty list")
    names, configurations = set(), set()
    destinations: dict[tuple[str, str], set[str]] = {}
    for sink in raw:
        if not isinstance(sink, dict):
            raise ValueError("Each sink must be an object")
        driver = sink.get("driver")
        if (
            not isinstance(driver, str)
            or ":" not in driver
            or any(not part.strip() for part in driver.split(":", 1))
        ):
            raise ValueError("Sink driver requires package.module:factory")
        name = sink.get("name", driver)
        streams = sink.get("streams", ["metric", "trace"])
        if (
            not isinstance(streams, list)
            or not streams
            or any(item not in ("metric", "trace", "rollout") for item in streams)
            or len(set(streams)) != len(streams)
        ):
            raise ValueError("Sink streams must contain metric, trace or rollout")
        fingerprint = json.dumps(
            {k: v for k, v in sink.items() if k != "name"}, sort_keys=True
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
        if driver in (_PROMETHEUS_SINK, _OTLP_SINK):
            destination = ""
            if driver == _OTLP_SINK:
                destination = _http_url(sink.get("endpoint"), "sinks.endpoint")
                sink["endpoint"] = destination
            key = (driver, destination)
            previous = destinations.setdefault(key, set())
            if previous and (
                driver == _PROMETHEUS_SINK or previous.intersection(streams)
            ):
                raise ValueError("Duplicate sink output destination")
            previous.update(streams)
        supplied = metric_labels(sink.get("labels", {}))
        if any(key in supplied for key in ("run_id", "exported_run_id")) or any(
            key in supplied and supplied[key] != value for key, value in labels.items()
        ):
            raise ValueError("Sink labels conflict with task identity")
        sink["labels"] = {**supplied, **labels}
    return raw


def expand_task_config(
    config: Mapping[str, Any] | DictConfig,
    *,
    runtime_dir: str | Path,
    server_url: str | None = None,
) -> DictConfig | None:
    """Expand only a task's ``rl_insight`` subtree into existing service/SDK config.

    The caller supplies a composed task configuration and deployment context.
    This function does not launch services or evaluate unrelated framework keys.
    """
    source = config if OmegaConf.is_config(config) else OmegaConf.create(config)
    node = OmegaConf.select(source, "rl_insight")
    if node is None:
        return None
    if not isinstance(node, DictConfig):
        raise ValueError("rl_insight must be an object")
    enabled = node.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ValueError("rl_insight.enabled must be boolean")
    if not enabled:
        return None
    try:
        options = OmegaConf.to_container(node, resolve=True, throw_on_missing=True)
    except Exception as exc:
        raise ValueError(
            "Cannot resolve rl_insight configuration; check required environment variables"
        ) from exc
    if not isinstance(options, dict):
        raise ValueError("rl_insight must be an object")
    if set(options) - {"enabled", "metrics", "traces", "sinks", "labels", "server"}:
        raise ValueError("Unknown rl_insight configuration field")
    for key in ("metrics", "traces", "labels", "server"):
        if key in options and not isinstance(options[key], dict):
            raise ValueError(f"rl_insight.{key} must be an object")
    labels = metric_labels(options.get("labels", {}))
    if any(key in labels for key in ("run_id", "exported_run_id")):
        raise ValueError("Run identity is managed separately from task labels")
    advanced = "sinks" in options
    if advanced and any(key in options for key in ("metrics", "traces")):
        raise ValueError("Choose sinks or simplified metrics/traces configuration")
    sinks = _task_sinks(options["sinks"], labels) if advanced else []
    metrics = options.get("metrics", {})
    traces = options.get("traces", {})
    settings = options.get("server", {})
    if set(settings) - {"port", "namespace", "endpoint_ttl", "url"}:
        raise ValueError(
            "rl_insight.server accepts port, namespace, endpoint_ttl and url only"
        )
    if set(metrics) - {
        "pull",
        "port",
        "remote_write",
        "scrape_interval",
        "scrape_timeout",
        "binary_path",
        "data_dir",
    }:
        raise ValueError("Unknown rl_insight.metrics field")
    if set(traces) - {"endpoint", "headers", "service_name"}:
        raise ValueError("Unknown rl_insight.traces field")
    pull = metrics.get("pull", False)
    if not isinstance(pull, bool):
        raise ValueError("rl_insight.metrics.pull must be boolean")
    destinations = metrics.get("remote_write", [])
    if "remote_write" in metrics and (
        not isinstance(destinations, list) or not destinations
    ):
        raise ValueError("remote_write requires a non-empty destination list")
    for destination in destinations:
        if not isinstance(destination, dict):
            raise ValueError("remote_write destinations must be objects")
        _http_url(destination.get("url"), "metrics.remote_write.url")
    if pull and destinations:
        raise ValueError("Choose metrics.pull or metrics.remote_write")
    has_metrics = (
        pull
        or bool(destinations)
        or any(s["driver"] == _PROMETHEUS_SINK for s in sinks)
    )
    if "metrics" in options and not has_metrics:
        raise ValueError("metrics requires pull=true or remote_write destinations")
    if "traces" in options:
        traces["endpoint"] = _http_url(traces.get("endpoint"), "traces.endpoint")
        headers = traces.get("headers", {})
        if not isinstance(headers, dict) or any(
            not isinstance(k, str)
            or not isinstance(v, (str, int, float, bool))
            or any(c in k + str(v) for c in "\r\n")
            for k, v in headers.items()
        ):
            raise ValueError("traces.headers must contain scalar HTTP header values")
    root = Path(runtime_dir).expanduser().resolve()
    conf = load_server_config_file()
    conf = OmegaConf.merge(
        conf,
        {
            "server": {
                "namespace": MonitorDefaults.NAMESPACE,
                "runtime_dir": str(root / "services"),
                "state_file": str(root / "state.json"),
                "data_dir": str(root / "data"),
                "log_dir": str(root / "logs"),
                "endpoint_ttl": 45,
                **settings,
            }
        },
    )
    if type(conf.server.port) is not int or not 0 <= conf.server.port <= 65535:
        raise ValueError("server.port must be an integer from 0 to 65535")
    if (
        not isinstance(conf.server.endpoint_ttl, (int, float))
        or not 1 <= conf.server.endpoint_ttl <= 3600
    ):
        raise ValueError("server.endpoint_ttl must be between 1 and 3600 seconds")
    if "port" in metrics:
        if type(metrics["port"]) is not int or not 0 <= metrics["port"] <= 65535:
            raise ValueError("metrics.port must be an integer from 0 to 65535")
        conf.server.metrics_port = metrics["port"]
    selected_url = server_url or settings.get("url")
    conf.server.url = _http_url(selected_url, "server_url") if selected_url else ""
    external = advanced or has_metrics or bool(traces)
    conf.server.backend = "push" if external else "local"
    conf.server.enable = has_metrics or not external
    conf.labels = labels
    if not external:
        return conf
    for name in ("prometheus", "tempo", "grafana"):
        conf[name].enable = False
    conf.prometheus.prometheus_port = 0
    for field in ("scrape_interval", "scrape_timeout", "binary_path", "data_dir"):
        if field in metrics:
            conf.prometheus[field] = metrics[field]
    for sink in sinks:
        if sink["driver"] == _PROMETHEUS_SINK:
            if sink.get("server_url", conf.server.url) != conf.server.url:
                raise ValueError("Prometheus sink server_url conflicts with server.url")
            sink["server_url"] = "${server.url}"
            sink.setdefault("advertise_host", "${oc.env:POD_IP,null}")
    if has_metrics and not advanced:
        sinks.append(
            {
                "name": "prometheus",
                "driver": _PROMETHEUS_SINK,
                "streams": ["metric"],
                "server_url": "${server.url}",
                "advertise_host": "${oc.env:POD_IP,null}",
                "labels": labels,
            }
        )
    if destinations:
        conf.prometheus.remote_write = destinations
        conf.prometheus.external_labels = labels
    if traces:
        sinks.append(
            {
                "name": "tempo",
                "driver": _OTLP_SINK,
                "streams": ["trace"],
                **traces,
                "labels": labels,
            }
        )
    conf.push = {"metric_prefix": "rl_insight_monitor_", "sinks": sinks}
    return conf
