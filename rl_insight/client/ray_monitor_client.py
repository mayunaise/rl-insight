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
import threading
from collections import deque
from typing import Any, cast

import ray
from omegaconf import DictConfig

from .. import __version__
from ..collector.ray_monitor_hub import MonitorHubActor
from ..server.http_api import initialize_server_connection
from ..utils.constants import MonitorEnv, MonitorRayActor, MonitorServer
from ..utils.protocol import (
    MIN_SUPPORTED_VERSION,
    InvalidVersionError,
    RunContext,
    RunError,
    require_version,
)
from .base import MonitorClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

__all__ = ["MonitorRayClient", "create_ray_monitor_client", "get_or_create_monitor_hub"]


def _current_job_actor_name() -> str:
    """Return a hub actor name scoped to the current Ray job."""
    job_id = ray.get_runtime_context().get_job_id()
    return f"{MonitorRayActor.NAME}_{job_id}"


def initialize_current_job_connection() -> RunContext | None:
    """Initialize native collection against an existing Hub, without creating one."""
    if not ray.is_initialized():
        return None
    try:
        hub = ray.get_actor(
            _current_job_actor_name(), namespace=MonitorRayActor.NAMESPACE
        )
    except ValueError:
        return None
    try:
        return _initialize_hub_connection(hub, timeout=2)
    except (ray.exceptions.GetTimeoutError, ray.exceptions.ActorUnavailableError):
        return None


def _initialize_hub_connection(hub: Any, *, timeout: float) -> RunContext:
    if not hasattr(hub, "connect"):
        require_version(None, "Ray Hub (initialization API unavailable)")
    try:
        context = ray.get(hub.connect.remote(__version__), timeout=timeout)
    except ray.exceptions.ActorUnavailableError:
        # Preserve the caller's retry path for temporary unavailability.
        raise
    except (ray.exceptions.RayActorError, ray.exceptions.RayTaskError) as exc:
        if isinstance(exc, ray.exceptions.RayTaskError) and isinstance(
            exc.cause, InvalidVersionError
        ):
            raise exc.cause from exc
        if "No module named 'rl_insight.utils.protocol'" in str(exc):
            require_version(None, "Ray worker SDK (version protocol unavailable)")
        message = (
            "Failed to communicate with the RL-Insight Ray Hub. "
            f"The SDK on the driver, Hub and all workers must be >= {MIN_SUPPORTED_VERSION}. "
            "Upgrade older SDKs and restart the training job. "
            "See the chained Ray error for the underlying cause."
        )
        logger.error(message)
        raise RunError(message, 502) from exc
    if not isinstance(context, RunContext):
        raise RunError("Ray Hub returned an invalid Context", 502)
    require_version(getattr(context, "version", None), "Ray Hub")
    return context


def get_or_create_monitor_hub(conf: DictConfig, *, create: bool = True) -> Any:
    """Reuse the current job's Hub; Ray handles concurrent creation if allowed."""
    actor_name = _current_job_actor_name()
    namespace = MonitorRayActor.NAMESPACE
    try:
        return ray.get_actor(actor_name, namespace=namespace)
    except ValueError:
        if not create:
            return None
    actor_options: dict[str, Any] = {
        "name": actor_name,
        "namespace": namespace,
        "get_if_exists": True,
    }
    server_url = str(conf.server.url).strip()
    if server_url:
        actor_options["runtime_env"] = {
            "env_vars": {MonitorEnv.SERVER_URL: server_url},
        }

    return cast(Any, MonitorHubActor).options(**actor_options).remote(conf)


def create_ray_monitor_client(conf: DictConfig) -> MonitorRayClient | None:
    """Build a client that talks to ``MonitorHubActor`` over Ray.

    Args:
        conf: Merged monitor configuration.

    Returns:
        Client instance, or ``None`` if Ray is not initialized (monitoring disabled).
    """
    if not ray.is_initialized():
        logger.warning("[rl-insight] Ray is not initialized; monitoring is disabled.")
        return None

    initialize_server_connection(str(conf.server.url))
    handle = get_or_create_monitor_hub(conf, create=bool(conf.get("run_metadata")))
    client = MonitorRayClient(handle)
    if not client.initialize():
        client._start_waiter()
    return client


class MonitorRayClient(MonitorClient):
    """Ray facade: ``apply_event`` submits work to the hub without blocking on completion."""

    def __init__(self, actor_handle: Any) -> None:
        """
        Args:
            actor_handle: Return value of ``get_or_create_monitor_hub``.
        """
        self._actor = actor_handle
        self._actor_name = _current_job_actor_name() if actor_handle is None else ""
        self._pending: deque[dict[str, Any]] = deque()
        self._lock = threading.Lock()
        self._init_lock = threading.Lock()
        self._closed = threading.Event()
        self._waiter: threading.Thread | None = None
        self._compatibility_error: RunError | None = None
        self._initialized = False
        # Includes service discovery and actor scheduling during initialization.
        self._timeout = (
            MonitorServer.SERVICE_DISCOVERY_RETRIES
            * (
                MonitorServer.SERVICE_DISCOVERY_TIMEOUT_SECONDS
                + MonitorServer.SERVICE_DISCOVERY_RETRY_DELAY_SECONDS
            )
            + 30
        )

    def initialize(self) -> bool:
        """Establish and validate the Hub connection before collecting data."""
        with self._init_lock:
            if self._compatibility_error is not None:
                raise self._compatibility_error
            if self._initialized:
                return True
            if self._actor is None:
                try:
                    self._actor = ray.get_actor(
                        self._actor_name, namespace=MonitorRayActor.NAMESPACE
                    )
                except ValueError:
                    return False
            try:
                _initialize_hub_connection(self._actor, timeout=self._timeout)
            except RunError as exc:
                self._compatibility_error = exc
                raise
            self._initialized = True
            return True

    def get_context(self) -> RunContext | None:
        if self._compatibility_error is not None:
            raise self._compatibility_error
        if not self._initialized:
            return None
        return ray.get(self._actor.get_context.remote(), timeout=self._timeout)

    def apply_event(self, event: dict[str, Any]) -> None:
        """Submit ``MonitorHubActor.apply_event`` on the actor (fire-and-forget; no ``ray.get``).

        Args:
            event: Serialized monitor event (see ``metric_*`` / ``trace_*`` helpers for shapes).

        Note:
            Errors on the hub side are not surfaced here. Ordering follows Ray actor scheduling.
        """
        with self._lock:
            if self._compatibility_error is not None:
                raise self._compatibility_error
            if self._closed.is_set():
                raise RunError("Monitor client is closed")
            if self._initialized and not self._pending:
                self._actor.apply_event.remote(event)
                return
            if len(self._pending) >= 1024:
                raise RunError(
                    "Too many events waiting for configured Run initialization"
                )
            self._pending.append(event)
            self._start_waiter()

    def _start_waiter(self) -> None:
        if self._closed.is_set():
            return
        if self._waiter is None or not self._waiter.is_alive():
            self._waiter = threading.Thread(
                target=self._wait_for_hub, name="insight-events", daemon=True
            )
            self._waiter.start()

    def _flush_pending(self) -> None:
        with self._lock:
            while self._pending:
                self._actor.apply_event.remote(self._pending[0])
                self._pending.popleft()

    def _wait_for_hub(self) -> None:
        delay = 0.1
        while not self._closed.is_set():
            try:
                if self.initialize():
                    self._flush_pending()
                    return
            except RunError as exc:
                with self._lock:
                    self._compatibility_error = exc
                    self._pending.clear()
                logger.error("[rl-insight] Cannot initialize pending events: %s", exc)
                return
            except ray.exceptions.RayError:
                logger.exception("[rl-insight] Cannot attach pending events to the Run")
            if self._closed.wait(delay):
                return
            delay = min(delay * 2, 2)

    def close(self) -> None:
        self._closed.set()
        if self._pending:
            if self.initialize():
                self._flush_pending()
            else:
                with self._lock:
                    logger.warning(
                        "[rl-insight] Discarding %d events: no configured Run was initialized",
                        len(self._pending),
                    )
                    self._pending.clear()
