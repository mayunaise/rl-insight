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
                    "retain_services": any(
                        expanded[name].enable
                        for name in ("prometheus", "tempo", "grafana")
                    ),
                },
            )
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


def expand_task_config(
    config: Mapping[str, Any] | DictConfig,
    *,
    runtime_dir: str | Path,
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
    settings = options.get("server", {})
    if set(settings) - {"port", "namespace", "endpoint_ttl", "url"}:
        raise ValueError(
            "rl_insight.server accepts port, namespace, endpoint_ttl and url only"
        )
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
    selected_url = settings.get("url")
    conf.server.url = _http_url(selected_url, "server_url") if selected_url else ""
    conf.labels = labels
    if any(key in options for key in ("metrics", "traces", "sinks")):
        from ..client.push.client import configure_outputs

        return configure_outputs(conf, options)
    return conf
