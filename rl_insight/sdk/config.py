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

"""SDK deployment configuration resolution (RFC v4.1 §4.3, S0 design §4.2).

Resolution is per field: explicit ``config`` > environment > config file >
default. An explicit ``None`` clears lower-priority values. The result is frozen
once, at session open, and travels to workers inside the serialized context.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from ..utils.constants import MonitorEnv, MonitorMode
from .identity import WILDCARD_HOSTS, Context, IdentityError, validate_run_id

DISABLED = "disabled"
TRACE_DEFAULT_PATH = "/v1/traces"
MANAGED_PUBLISHER = "managed"

READY = "ready"
DEGRADED = "degraded"
NOT_READY = "not_ready"
UNSUPPORTED = "unsupported"

CHANNELS = (
    "registration",
    "metrics_scrape",
    "trace_accept",
    "trace_online",
    "archive_metrics",
    "archive_traces",
    "query",
    "export",
)

# Field key -> environment variable. Field keys are the dotted names from the
# S0 design table so config files can use them verbatim.
ENV_KEYS = {
    "server.url": MonitorEnv.SERVER_URL,
    "platform.task_id": MonitorEnv.TASK_ID,
    "trace.endpoint": MonitorEnv.OTLP_HTTP_ENDPOINT,
    "trace.host": MonitorEnv.OTLP_HOST,
    "trace.port": MonitorEnv.OTLP_PORT,
    "trace.path": MonitorEnv.OTLP_PATH,
    "publisher.adapter": MonitorEnv.PUBLISHER_ADAPTER,
    "publisher.config_ref": MonitorEnv.PUBLISHER_CONFIG_REF,
    "run.resume_id": None,
    "run.resume": None,
}


class ConfigError(ValueError):
    """Raised for an explicitly invalid deployment configuration."""


@dataclass(frozen=True)
class ChannelReadiness:
    """Per-channel readiness with a stable reason code."""

    status: str
    reason_code: str = ""

    def to_dict(self) -> dict[str, str]:
        return {"status": self.status, "reason_code": self.reason_code}


@dataclass(frozen=True)
class SDKConfig:
    """Frozen deployment configuration for one session."""

    mode: str
    server_url: str | None = None
    task_id: str | None = None
    trace_endpoint: str | None = None
    publisher_adapter: str | None = None
    publisher_config_ref: str | None = None
    resume_id: str | None = None
    resume: bool = False
    trace_reason_code: str = ""
    origins: Mapping[str, str] = field(default_factory=dict)
    diagnostics: tuple[str, ...] = ()

    @property
    def managed(self) -> bool:
        return self.mode == MonitorMode.MANAGED

    @property
    def enabled(self) -> bool:
        return self.mode in (MonitorMode.MANAGED, MonitorMode.PLATFORM)

    def readiness(self) -> dict[str, ChannelReadiness]:
        """Channel status for this configuration; never claims active == ready."""
        registration = (
            READY
            if self.managed
            else (READY if self.publisher_adapter else UNSUPPORTED)
        )
        registration_reason = "" if registration == READY else "NO_PUBLISHER"
        trace = READY if self.trace_endpoint else NOT_READY
        return {
            "registration": ChannelReadiness(registration, registration_reason),
            "metrics_scrape": ChannelReadiness(
                READY if registration == READY else UNSUPPORTED,
                "" if registration == READY else "NO_PUBLISHER",
            ),
            "trace_accept": ChannelReadiness(
                trace,
                ""
                if trace == READY
                else (self.trace_reason_code or "TRACE_ENDPOINT_MISSING"),
            ),
            "trace_online": ChannelReadiness(
                READY if self.managed and trace == READY else UNSUPPORTED,
                "" if self.managed else "EXTERNAL_PLATFORM",
            ),
            "archive_metrics": ChannelReadiness(
                READY if self.managed else UNSUPPORTED,
                "" if self.managed else "EXTERNAL_PLATFORM",
            ),
            "archive_traces": ChannelReadiness(
                READY if self.managed else UNSUPPORTED,
                "" if self.managed else "EXTERNAL_PLATFORM",
            ),
            "query": ChannelReadiness(
                READY if self.managed else UNSUPPORTED,
                "" if self.managed else "EXTERNAL_PLATFORM",
            ),
            "export": ChannelReadiness(
                READY if self.managed else UNSUPPORTED,
                "" if self.managed else "EXTERNAL_PLATFORM",
            ),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "server_url": self.server_url,
            "task_id": self.task_id,
            "trace_endpoint": self.trace_endpoint,
            "publisher_adapter": self.publisher_adapter,
            "publisher_config_ref": self.publisher_config_ref,
            "resume_id": self.resume_id,
            "resume": self.resume,
            "diagnostics": list(self.diagnostics),
            "readiness": {
                name: item.to_dict() for name, item in self.readiness().items()
            },
        }


def _http_url(value: Any, field_name: str, *, keep_path: bool) -> str:
    raw = str(value).strip()
    parts = urlsplit(raw)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ConfigError(f"{field_name} must be an absolute http(s) URL")
    if "@" in parts.netloc:
        raise ConfigError(f"{field_name} must not carry userinfo")
    if parts.fragment:
        raise ConfigError(f"{field_name} must not carry a fragment")
    return raw if keep_path else raw.rstrip("/")


def _assemble_trace_endpoint(host: str, port: Any, path: Any) -> str:
    if not host:
        raise ConfigError("trace.host must not be empty")
    if str(host) in WILDCARD_HOSTS:
        raise ConfigError("trace.host must not be a wildcard bind address")
    try:
        port_number = int(port)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"trace.port must be an integer, got {port!r}") from exc
    if not 1 <= port_number <= 65535:
        raise ConfigError("trace.port must be between 1 and 65535")
    path_value = str(path or TRACE_DEFAULT_PATH)
    if not path_value.startswith("/"):
        raise ConfigError("trace.path must start with '/'")
    return f"http://{host}:{port_number}{path_value}"


def _load_config_file(path: str) -> Mapping[str, Any]:
    try:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except OSError as exc:
        raise ConfigError(
            f"cannot read {MonitorEnv.CONFIG_FILE} {path!r}: {exc}"
        ) from exc
    if not isinstance(data, Mapping):
        raise ConfigError(f"{MonitorEnv.CONFIG_FILE} {path!r} must contain a mapping")
    return data


def config_from_context(
    context: Context,
    *,
    overrides: Mapping[str, Any] | None = None,
    env: Mapping[str, str] | None = None,
) -> SDKConfig:
    """Freeze a worker's configuration from the context it was handed.

    A worker never re-resolves ``RL_INSIGHT_*``: it does not know whether the
    driver ran managed or platform, and picking a different mode, address or
    identity would break the one-run rule (S3 §4.1). Environment values that
    disagree with the context only produce a diagnostic; the context wins.
    """
    identity = context.identity
    server_url = context.services.get("server")
    trace_endpoint = context.services.get("otlp_http")
    publisher = dict(context.publisher or {})
    adapter = str(publisher.get("adapter") or "") or None
    config_ref = publisher.get("config_ref")

    diagnostics: list[str] = []
    for key in sorted(dict(overrides or {})):
        diagnostics.append(
            f"attach ignores local override {key!r}: identity, mode and services "
            "come from the context"
        )
    for env_key in (
        MonitorEnv.SERVER_URL,
        MonitorEnv.TASK_ID,
        MonitorEnv.OTLP_HTTP_ENDPOINT,
        MonitorEnv.PUBLISHER_ADAPTER,
    ):
        raw = (os.environ if env is None else env).get(env_key)
        if raw not in (None, ""):
            diagnostics.append(
                f"worker environment {env_key} is ignored: the context is authoritative"
            )

    return SDKConfig(
        mode=identity.source,
        server_url=server_url,
        task_id=identity.id if identity.source == MonitorMode.PLATFORM else None,
        trace_endpoint=trace_endpoint,
        publisher_adapter=adapter,
        publisher_config_ref=str(config_ref) if config_ref else None,
        origins={"server.url": "context", "trace.endpoint": "context"},
        diagnostics=tuple(diagnostics),
    )


def resolve_config(
    explicit: Mapping[str, Any] | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> SDKConfig:
    """Resolve and validate the frozen session configuration."""
    environment = os.environ if env is None else env
    explicit = dict(explicit or {})

    file_cfg: Mapping[str, Any] = {}
    configured_file = explicit.get(
        "config_file", environment.get(MonitorEnv.CONFIG_FILE, "")
    )
    if configured_file:
        file_cfg = _load_config_file(str(configured_file))

    origins: dict[str, str] = {}

    def lookup(key: str) -> Any:
        if key in explicit:
            origins[key] = "explicit"
            return explicit[key]
        env_key = ENV_KEYS.get(key)
        raw = environment.get(env_key) if env_key else None
        if raw not in (None, ""):
            origins[key] = "env"
            return raw
        if file_cfg.get(key) is not None:
            origins[key] = "file"
            return file_cfg[key]
        origins[key] = "default"
        return None

    diagnostics: list[str] = []
    server_url = lookup("server.url")
    task_id = lookup("platform.task_id")
    trace_full = lookup("trace.endpoint")
    trace_host = lookup("trace.host")
    trace_port = lookup("trace.port")
    trace_path = lookup("trace.path")
    publisher_adapter = lookup("publisher.adapter")
    publisher_ref = lookup("publisher.config_ref")
    resume_id = lookup("run.resume_id")
    resume_flag = lookup("run.resume")

    if server_url is not None:
        server_url = _http_url(server_url, "server.url", keep_path=False)
        server_url = server_url or None
        if task_id is not None:
            diagnostics.append(
                "selected_mode=managed: platform.task_id is ignored for managed runs"
            )
            task_id = None

    if task_id is not None:
        try:
            task_id = validate_run_id(str(task_id))
        except IdentityError as exc:
            raise ConfigError(f"platform.task_id is invalid: {exc}") from exc

    if resume_id is not None:
        try:
            resume_id = validate_run_id(str(resume_id))
        except IdentityError as exc:
            raise ConfigError(f"run.resume_id is invalid: {exc}") from exc
    resume = bool(resume_flag)
    if resume_id and not resume:
        raise ConfigError("run.resume_id requires run.resume=true")
    if resume and not resume_id:
        raise ConfigError("run.resume=true requires run.resume_id")
    if resume_id and server_url is None:
        raise ConfigError("run.resume_id is only supported in managed mode")

    trace_endpoint: str | None = None
    trace_reason: str | None = None
    full_url = (
        _http_url(trace_full, "trace.endpoint", keep_path=True)
        if trace_full is not None
        else None
    )
    split_pair = trace_host is not None or trace_port is not None
    if split_pair and (trace_host is None or trace_port is None):
        raise ConfigError("trace.host and trace.port must be provided together")
    split_url = (
        _assemble_trace_endpoint(str(trace_host), trace_port, trace_path)
        if split_pair
        else None
    )
    if full_url and split_url:
        if full_url.rstrip("/") == split_url.rstrip("/"):
            trace_endpoint = full_url
        else:
            trace_reason = "TRACE_ENDPOINT_CONFLICT"
            diagnostics.append(
                "trace.endpoint conflicts with trace.host/port/path: "
                "the trace channel is disabled"
            )
    else:
        trace_endpoint = full_url or split_url
    if trace_reason:
        trace_endpoint = None

    if server_url:
        mode = MonitorMode.MANAGED
        publisher_adapter = publisher_adapter or MANAGED_PUBLISHER
    elif task_id:
        mode = MonitorMode.PLATFORM
    else:
        mode = DISABLED
        diagnostics.append(
            "observability disabled: neither a server URL nor a platform task id "
            "was provided; training continues without observation"
        )

    if mode == DISABLED:
        if trace_endpoint:
            diagnostics.append(
                "trace endpoint configured without an identity: "
                "no run context, trace channel disabled"
            )
        trace_endpoint = None
        publisher_adapter = None

    config = SDKConfig(
        mode=mode,
        server_url=server_url,
        task_id=task_id,
        trace_endpoint=trace_endpoint,
        publisher_adapter=publisher_adapter,
        publisher_config_ref=str(publisher_ref) if publisher_ref else None,
        resume_id=resume_id,
        resume=resume,
        trace_reason_code=trace_reason or "",
        origins=origins,
        diagnostics=tuple(diagnostics),
    )
    return config
