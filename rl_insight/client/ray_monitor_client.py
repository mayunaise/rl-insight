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
from typing import Any, cast
from uuid import uuid4

import ray
from omegaconf import DictConfig, OmegaConf

from ..collector.ray_monitor_hub import MonitorHubActor
from ..protocol import RunContext
from ..utils.constants import MonitorEnv, MonitorRayActor
from .base import MonitorClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

_owned_hubs: dict[str, str] = {}

__all__ = ["MonitorRayClient", "create_ray_monitor_client", "get_or_create_monitor_hub"]


def _current_job_actor_name() -> str:
    """Return a hub actor name scoped to the current Ray job."""
    job_id = ray.get_runtime_context().get_job_id()
    job_id_str = job_id if isinstance(job_id, str) else job_id.hex()
    return f"{MonitorRayActor.NAME}_{job_id_str}"


def current_job_context() -> RunContext | None:
    """Resolve only an existing Hub; native registration must not create a Run."""
    if not ray.is_initialized():
        return None
    try:
        hub = ray.get_actor(
            _current_job_actor_name(), namespace=MonitorRayActor.NAMESPACE
        )
        value = ray.get(hub.get_context.remote(), timeout=2)
        return RunContext.from_dict(value) if value else None
    except (ValueError, ray.exceptions.RayError):
        return None


def current_job_config() -> DictConfig | None:
    """Read an initialized job configuration without creating a Hub or services."""
    if not ray.is_initialized():
        return None
    try:
        hub = ray.get_actor(
            _current_job_actor_name(), namespace=MonitorRayActor.NAMESPACE
        )
        return OmegaConf.create(ray.get(hub.get_monitor_config.remote(), timeout=90))
    except (ValueError, ray.exceptions.RayError):
        return None


def register_job_metrics(addresses, job, labels) -> bool:
    conf = current_job_config()
    if conf is None or not conf.get("task_configuration", False):
        return False
    hub = ray.get_actor(_current_job_actor_name(), namespace=MonitorRayActor.NAMESPACE)
    ray.get(hub.register_native_metrics.remote(addresses, job, labels), timeout=15)
    return True


def get_or_create_monitor_hub(conf: DictConfig) -> Any:
    """Get or create a job-scoped MonitorHubActor with health check.

    Each Ray job gets its own hub actor named
    ``MonitorRayActor.NAME_{job_id}``. The actor is not detached, so it
    cleans up automatically when the job exits.

    Args:
        conf: Merged trainer monitor config passed to the actor constructor.

    Returns:
        Ray actor handle for ``MonitorHubActor``.

    Raises:
        RuntimeError: If Ray is not initialized.
    """

    actor_name = _current_job_actor_name()
    namespace = MonitorRayActor.NAMESPACE

    try:
        handle = ray.get_actor(actor_name, namespace=namespace)
        logger.info(
            "[rl-insight] Connected to existing monitor hub actor %r.", actor_name
        )
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
    if conf.get("task_options") is not None:
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        head = next(
            node
            for node in ray.nodes()
            if node["Alive"] and node["Resources"].get("node:__internal_head__")
        )
        actor_options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
            head["NodeID"], soft=False
        )
    env_vars = {}
    options = conf.get("task_options")
    if options is not None and not any(
        key in options for key in ("metrics", "traces", "sinks")
    ):
        # The full history backend outlives this job's Hub and its Ray worker.
        env_vars["RAY_kill_child_processes_on_worker_exit"] = "false"
    server_url = str(conf.server.url).strip()
    if server_url:
        env_vars[MonitorEnv.SERVER_URL] = server_url
    if env_vars:
        actor_options["runtime_env"] = {"env_vars": env_vars}

    try:
        actor_cls = cast(Any, MonitorHubActor)
        token = uuid4().hex
        handle = actor_cls.options(**actor_options).remote(conf, owner_token=token)
        _owned_hubs[actor_name] = token
        return handle
    except ValueError:
        logger.info(
            "[rl-insight] Monitor hub actor %r was created concurrently; "
            "connecting to it.",
            actor_name,
        )
        return ray.get_actor(actor_name, namespace=namespace)


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

    handle = get_or_create_monitor_hub(conf)
    resolved = OmegaConf.create(ray.get(handle.get_monitor_config.remote(), timeout=90))
    if conf.get("task_options") is not None and conf.get(
        "task_signature"
    ) != resolved.get("task_signature"):
        raise ValueError(
            "Worker monitoring configuration conflicts with the existing job"
        )
    value = ray.get(handle.get_context.remote(), timeout=15)
    client = MonitorRayClient(
        handle,
        RunContext.from_dict(value) if value else None,
        _owned_hubs.get(_current_job_actor_name()),
    )
    client.conf = resolved
    return client


class MonitorRayClient(MonitorClient):
    """Ray facade: ``apply_event`` submits work to the hub without blocking on completion."""

    conf: DictConfig

    def __init__(
        self,
        actor_handle: Any,
        context: RunContext | None = None,
        owner_token: str | None = None,
    ) -> None:
        """
        Args:
            actor_handle: Return value of ``get_or_create_monitor_hub``.
        """
        self._actor = actor_handle
        self._context = context
        self._owner_token = owner_token

    def get_context(self) -> RunContext | None:
        return self._context

    def flush(self) -> None:
        """Drain this client's prior submissions before the Ray job can exit."""
        operation = (
            self._actor.prepare_finish.remote(self._owner_token)
            if self._owner_token
            else self._actor.flush.remote()
        )
        ray.get(operation, timeout=35)

    def close(self) -> None:
        if self._owner_token:
            try:
                ray.get(
                    self._actor.end_run.remote(self._owner_token),
                    timeout=30 + float(self.conf.get("endpoint_ttl", 0)),
                )
            finally:
                _owned_hubs.pop(_current_job_actor_name(), None)
                self._owner_token = None

    def apply_event(self, event: dict[str, Any]) -> None:
        """Submit ``MonitorHubActor.apply_event`` on the actor (fire-and-forget; no ``ray.get``).

        Args:
            event: Serialized monitor event (see ``metric_*`` / ``trace_*`` helpers for shapes).

        Note:
            Errors on the hub side are not surfaced here. Ordering follows Ray actor scheduling.
        """
        self._actor.apply_event.remote(event)
