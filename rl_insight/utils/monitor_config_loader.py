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
from dataclasses import dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from omegaconf import DictConfig, OmegaConf
from omegaconf.errors import OmegaConfBaseException

from ..server.catalog import SERVICE_NAMES
from ..server.network import normalize_http_url

from .constants import MonitorBackend, MonitorDefaults, MonitorEnv, MonitorPaths
from .protocol import metric_labels

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


@dataclass
class _TaskServer:
    port: Optional[int] = None
    namespace: str = MonitorDefaults.NAMESPACE
    url: str = ""
    endpoint_ttl: float = 45.0


@dataclass
class _TaskMetrics:
    pull: bool = False
    port: Optional[int] = None
    remote_write: list[dict[str, Any]] = field(default_factory=list)
    scrape_interval: str = "10s"
    scrape_timeout: str = "10s"
    binary_path: str = ""
    data_dir: Optional[str] = None


@dataclass
class _TaskTraces:
    endpoint: str = ""
    headers: dict[str, Any] = field(default_factory=dict)
    service_name: str = ""


@dataclass
class _TaskConfig:
    enabled: bool = True
    server: _TaskServer = field(default_factory=_TaskServer)
    labels: dict[str, Any] = field(default_factory=dict)
    metrics: _TaskMetrics = field(default_factory=_TaskMetrics)
    traces: _TaskTraces = field(default_factory=_TaskTraces)
    sinks: list[dict[str, Any]] = field(default_factory=list)


_TASK_SCHEMA = OmegaConf.structured(_TaskConfig)
# server/labels also belong to legacy runtime configs; only other public fields
# distinguish a bare task config. Keep this compatibility rule in one place.
_TASK_SELECTORS = _TASK_SCHEMA.keys() - {"server", "labels"}


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
    node = _task_node(config)
    if node is not None:
        return _load_task_config(node)
    merged = _merge_external_config(_copy_defaults())

    if config is not None:
        user = (
            OmegaConf.create(OmegaConf.to_container(config, resolve=True))
            if OmegaConf.is_config(config)
            else OmegaConf.create(dict(config))
        )
        merged = OmegaConf.merge(merged, user)

    if url := os.environ.get(MonitorEnv.SERVER_URL):
        merged.server.url = str(url).strip()
    if backend := os.environ.get(MonitorEnv.SERVER_BACKEND, "").strip():
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


def _task_node(config) -> DictConfig | None:
    """Select the public subtree without resolving unrelated framework fields."""
    if config is None:
        return None
    source = config if OmegaConf.is_config(config) else OmegaConf.create(config)
    if "rl_insight" in source:
        node = source.rl_insight
        if not isinstance(node, DictConfig):
            raise ValueError("rl_insight must be an object")
        return node
    return source if _TASK_SELECTORS.intersection(source.keys()) else None


def _provided_options(conf, supplied):
    """Normalize declared fields, preserving omitted options and opaque driver maps."""
    if is_dataclass(OmegaConf.get_type(conf)):
        return {
            key: _provided_options(conf[key], value) for key, value in supplied.items()
        }
    return (
        OmegaConf.to_container(conf, resolve=True)
        if OmegaConf.is_config(conf)
        else conf
    )


def _task_options(node: DictConfig) -> dict[str, Any] | None:
    if node.get("enabled", True) is False:
        return None
    try:
        supplied = OmegaConf.to_container(node, resolve=True, throw_on_missing=True)
        validated = OmegaConf.merge(_TASK_SCHEMA, supplied)
        if not validated.enabled:
            return None
        return _provided_options(validated, supplied)
    except OmegaConfBaseException as exc:
        raise ValueError(f"Invalid rl_insight configuration: {exc}") from exc


def _load_task_config(node: DictConfig) -> DictConfig:
    if os.environ.get(MonitorEnv.CONFIG_PATH):
        raise ValueError("Training configuration conflicts with RL_INSIGHT_CONFIG")
    options = _task_options(node)
    if options is None:
        return OmegaConf.merge(_copy_defaults(), {"enabled": False})
    expanded = _expand_task_options(
        options, runtime_dir=MonitorPaths.STATE_ROOT / "tasks"
    )
    inherited_url = os.environ.get(MonitorEnv.SERVER_URL, "").strip().rstrip("/")
    if inherited_url and inherited_url != str(expanded.server.url).rstrip("/"):
        raise ValueError("Server URL environment conflicts with training configuration")
    if os.environ.get(MonitorEnv.SERVER_BACKEND):
        raise ValueError("Training configuration selects its own outputs")
    return OmegaConf.merge(
        _copy_defaults(),
        {
            "task_options": options,
            "task_signature": hashlib.sha256(
                json.dumps(options, sort_keys=True).encode()
            ).hexdigest(),
            "labels": expanded.labels,
            "retain_services": any(expanded[name].enable for name in SERVICE_NAMES),
        },
    )


def expand_task_config(
    config: Mapping[str, Any] | DictConfig,
    *,
    runtime_dir: str | Path,
) -> DictConfig | None:
    """Project public monitoring options onto the existing service template."""
    source = config if OmegaConf.is_config(config) else OmegaConf.create(config)
    node = OmegaConf.select(source, "rl_insight")
    if node is None:
        return None
    if not isinstance(node, DictConfig):
        raise ValueError("rl_insight must be an object")
    options = _task_options(node)
    return (
        _expand_task_options(options, runtime_dir=runtime_dir)
        if options is not None
        else None
    )


def _expand_task_options(
    options: dict[str, Any], *, runtime_dir: str | Path
) -> DictConfig:
    defaults = OmegaConf.to_container(_TASK_SCHEMA.server)
    settings = {
        **{key: value for key, value in defaults.items() if value is not None},
        **options.get("server", {}),
    }
    root = Path(runtime_dir).expanduser().resolve()
    conf = OmegaConf.merge(
        load_server_config_file(),
        {
            "server": {
                **settings,
                "runtime_dir": str(root / "services"),
                "state_file": str(root / "state.json"),
                "data_dir": str(root / "data"),
                "log_dir": str(root / "logs"),
            },
        },
    )
    if type(conf.server.port) is not int or not 0 <= conf.server.port <= 65535:
        raise ValueError("server.port must be an integer from 0 to 65535")
    if not 1 <= conf.server.endpoint_ttl <= 3600:
        raise ValueError("server.endpoint_ttl must be between 1 and 3600 seconds")
    conf.server.url = (
        normalize_http_url(settings["url"], "server.url") if settings["url"] else ""
    )
    conf.labels = metric_labels(options.get("labels", {}))
    if options.keys() & (_TASK_SELECTORS - {"enabled"}):
        from ..client.push.client import configure_outputs

        return configure_outputs(conf, options)
    return conf
