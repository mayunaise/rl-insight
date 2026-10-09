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

"""Ray client for the shared monitor hub."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, cast

import ray
from omegaconf import DictConfig

from ..collector.ray_monitor_hub import MonitorHubActor
from ..sdk.identity import Context
from ..utils.constants import MonitorEnv, MonitorRayActor
from .base import MonitorClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

__all__ = [
    "HubIdentityConflict",
    "MonitorRayClient",
    "create_ray_monitor_client",
    "get_or_create_monitor_hub",
    "release_monitor_hub",
]


class HubIdentityConflict(RuntimeError):
    """An existing hub actor belongs to a different run and is never reused."""


# Session-mode handles borrowed by this process: actor name -> (handle, refcount).
# A worker's close releases one reference; only the last borrower stops the hub.
_HUB_HANDLES: dict[str, tuple[Any, int]] = {}


def _coerce_context(
    context: Context | Mapping[str, Any] | None,
) -> Context | None:
    if context is None or isinstance(context, Context):
        return context
    return Context.from_dict(context)


def _current_job_actor_name() -> str:
    """Return a hub actor name scoped to the current Ray job."""
    job_id = ray.get_runtime_context().get_job_id()
    job_id_str = job_id if isinstance(job_id, str) else job_id.hex()
    return f"{MonitorRayActor.NAME}_{job_id_str}"


def _hub_actor_name(context: Context) -> str:
    """Hub name for one run: full run_key plus Ray job id, never the display name."""
    return f"{_current_job_actor_name()}_{context.identity.run_key}"


def _retain(actor_name: str, handle: Any) -> None:
    _, count = _HUB_HANDLES.get(actor_name, (handle, 0))
    _HUB_HANDLES[actor_name] = (handle, count + 1)


def release_monitor_hub(actor_name: str) -> None:
    """Drop one reference to a hub; the last borrower stops its session resources."""
    entry = _HUB_HANDLES.get(actor_name)
    if entry is None:
        return
    handle, count = entry
    if count > 1:
        _HUB_HANDLES[actor_name] = (handle, count - 1)
        return
    _HUB_HANDLES.pop(actor_name, None)
    handle.shutdown.remote()


def _check_hub_identity(handle: Any, context: Context) -> None:
    """Refuse to reuse a hub that serves another run."""
    existing = ray.get(handle.get_identity.remote())
    wanted = context.identity.to_dict()
    if existing != wanted:
        raise HubIdentityConflict(
            f"monitor hub serves {existing!r}, refusing to reuse it for {wanted!r}"
        )


def get_or_create_monitor_hub(
    conf: DictConfig,
    context: Context | Mapping[str, Any] | None = None,
) -> Any:
    """Get or create a job-scoped MonitorHubActor with health check.

    Without a context the hub is job-scoped (legacy single-session behavior).
    With a context the hub is scoped to that run and Ray job, its identity is
    compared before reuse, and this process holds one reference.

    Args:
        conf: Merged trainer monitor config passed to the actor constructor.
        context: Frozen run context, when the caller is an explicit session.

    Returns:
        Ray actor handle for ``MonitorHubActor``.

    Raises:
        RuntimeError: If Ray is not initialized.
        HubIdentityConflict: If the existing hub serves a different run.
    """
    resolved = _coerce_context(context)
    actor_name = (
        _hub_actor_name(resolved) if resolved is not None else _current_job_actor_name()
    )
    namespace = MonitorRayActor.NAMESPACE

    try:
        handle = ray.get_actor(actor_name, namespace=namespace)
        logger.info(
            "[rl-insight] Connected to existing monitor hub actor %r.", actor_name
        )
        if resolved is not None:
            _check_hub_identity(handle, resolved)
            _retain(actor_name, handle)
        return handle
    except ValueError:
        logger.info(
            "[rl-insight] No existing monitor hub actor %r found; creating one.",
            actor_name,
        )
    actor_options: dict[str, Any] = {
        "name": actor_name,
        "namespace": namespace,
    }
    server_url = str(conf.server.url).strip()
    # The legacy hub finds its server through the environment; a session hub gets
    # the frozen services in its context and must not re-read the environment.
    if server_url and resolved is None:
        actor_options["runtime_env"] = {
            "env_vars": {MonitorEnv.SERVER_URL: server_url},
        }

    try:
        actor_cls = cast(Any, MonitorHubActor)
        handle = (
            actor_cls.options(**actor_options).remote(conf)
            if resolved is None
            else actor_cls.options(**actor_options).remote(conf, resolved.to_dict())
        )
    except ValueError:
        logger.info(
            "[rl-insight] Monitor hub actor %r was created concurrently; "
            "connecting to it.",
            actor_name,
        )
        handle = ray.get_actor(actor_name, namespace=namespace)
        if resolved is not None:
            _check_hub_identity(handle, resolved)
    if resolved is not None:
        _retain(actor_name, handle)
    return handle


def create_ray_monitor_client(
    conf: DictConfig,
    context: Context | Mapping[str, Any] | None = None,
) -> MonitorRayClient | None:
    """Build a client that talks to ``MonitorHubActor`` over Ray.

    Args:
        conf: Merged monitor configuration.
        context: Frozen run context, when the caller is an explicit session.

    Returns:
        Client instance, or ``None`` if Ray is not initialized (monitoring disabled).
    """
    if not ray.is_initialized():
        logger.warning("[rl-insight] Ray is not initialized; monitoring is disabled.")
        return None

    resolved = _coerce_context(context)
    handle = get_or_create_monitor_hub(conf, resolved)
    actor_name = _hub_actor_name(resolved) if resolved is not None else ""
    return MonitorRayClient(handle, actor_name=actor_name)


class MonitorRayClient(MonitorClient):
    """Ray facade: ``apply_event`` submits work to the hub without blocking on completion."""

    def __init__(self, actor_handle: Any, actor_name: str = "") -> None:
        """
        Args:
            actor_handle: Return value of ``get_or_create_monitor_hub``.
            actor_name: Session-mode hub name used to release this client's
                reference on close; empty for a legacy client.
        """
        self._actor = actor_handle
        self._actor_name = actor_name
        self._closed = False

    def apply_event(self, event: dict[str, Any]) -> None:
        """Submit ``MonitorHubActor.apply_event`` on the actor (fire-and-forget; no ``ray.get``).

        Args:
            event: Serialized monitor event (see ``metric_*`` / ``trace_*`` helpers for shapes).

        Note:
            Errors on the hub side are not surfaced here. Ordering follows Ray actor scheduling.
        """
        self._actor.apply_event.remote(event)

    def close(self) -> None:
        """Release this client's hub reference; idempotent and legacy-safe."""
        if self._closed or not self._actor_name:
            return
        self._closed = True
        release_monitor_hub(self._actor_name)
