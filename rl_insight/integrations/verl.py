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

"""verl integration glue (S3 §3, §5, §6, §7).

The training repository keeps only lifecycle, serialized context passing and
hook calls; every decision about mode, address, identity and cleanup stays in
the SDK session. The driver opens one session and puts its serialized context
into the training config; each process that emits or binds an endpoint calls
:func:`attach` with that same context and never creates a second identity.

The context travels as a plain string in the config the framework already
passes to its Ray actors (``trainer.rl_insight_context`` for the trainer,
``rollout.observability_context`` for the rollout server actors), so no
``RL_INSIGHT_*`` deployment variable is re-read in a worker.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
import weakref
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from ..sdk.identity import (
    EPOCH_HEADER,
    INCARNATION_HEADER,
    MANAGED,
    RUN_ID_HEADER,
    Context,
)
from ..sdk.metrics import (
    IdentityHeaderMiddleware,
    MetricsEndpoint,
    identity_headers,
)
from ..sdk.publisher import PublicationResult
from ..sdk.session import Session, attach as sdk_attach, open_session

logger = logging.getLogger(__name__)

# Serialized context keys. The trainer key is a new top-level entry of the
# ``trainer`` config node; the rollout keys are declared fields of the rollout
# config that every rollout server actor already receives.
TRAINER_CONTEXT_KEY = "rl_insight_context"
ROLLOUT_CONTEXT_FIELD = "observability_context"
ROLLOUT_COMPONENT_FIELD = "observability_component"

METRICS_PATH = "/metrics"
STATE_INTERVAL = "state_interval"
TRAINER_COMPONENT = "trainer"
# The three headers the controlled scrape route requires on the scraped
# response before it stores the body as this run's data (S2 §4.2).
METRIC_HEADER_LIST = ", ".join((RUN_ID_HEADER, EPOCH_HEADER, INCARNATION_HEADER))

# Process-local registry of the sessions this process attached. It is keyed by
# run key, so two runs in one process stay separate; it is never "the current
# run" and it is never used to create an identity.
_PROCESS_SESSIONS: dict[str, Session] = {}
_PROCESS_LOCK = threading.Lock()


@dataclass
class DriverSession:
    """The driver's own session plus the context every worker must receive."""

    session: Session
    context_json: str | None = None

    @property
    def enabled(self) -> bool:
        return self.context_json is not None

    @property
    def identity(self) -> Any:
        return self.session.identity

    def close(self, deadline_seconds: float = 5.0) -> dict[str, Any] | None:
        return close(self.session, deadline_seconds=deadline_seconds)


# ---------------------------------------------------------------------------
# context transport
# ---------------------------------------------------------------------------
def _read(node: Any, dotted: str, default: Any = None) -> Any:
    """Read a dotted key from a DictConfig, a mapping or a dataclass."""
    value = node
    for part in dotted.split("."):
        if value is None:
            return default
        if isinstance(value, Mapping):
            value = value.get(part, default)
        else:
            value = getattr(value, part, default)
    return value


def read_trainer_context(config: Any) -> str | None:
    """The serialized context the driver stored for the trainer processes."""
    value = _read(config, f"trainer.{TRAINER_CONTEXT_KEY}")
    return str(value) if value else None


def write_trainer_context(config: Any, context_json: str | None) -> None:
    """Store the serialized context in the config the TaskRunner receives."""
    if not context_json:
        return
    from omegaconf import OmegaConf

    OmegaConf.update(
        config, f"trainer.{TRAINER_CONTEXT_KEY}", context_json, force_add=True
    )


def read_rollout_context(rollout_config: Any) -> str | None:
    """The serialized context declared on a rollout config."""
    value = _read(rollout_config, ROLLOUT_CONTEXT_FIELD)
    return str(value) if value else None


def read_rollout_component(rollout_config: Any) -> str:
    """The component name declared next to the rollout context."""
    value = _read(rollout_config, ROLLOUT_COMPONENT_FIELD)
    return str(value) if value else "rollout"


def write_rollout_context(
    rollout_config: Any, context_json: str | None, *, component: str
) -> None:
    """Declare the context on the rollout config its server actors receive.

    The rollout config is serialized to every rollout server actor, so this is
    the parameter path the framework already has; the fields are declared on the
    rollout config schema.
    """
    if not context_json:
        return
    from omegaconf import OmegaConf

    OmegaConf.update(
        rollout_config, ROLLOUT_CONTEXT_FIELD, context_json, force_add=True
    )
    OmegaConf.update(rollout_config, ROLLOUT_COMPONENT_FIELD, component, force_add=True)


# ---------------------------------------------------------------------------
# sessions
# ---------------------------------------------------------------------------
def register_process_session(session: Session) -> None:
    """Remember one attached session in this process, keyed by run."""
    context = session.context
    if context is None:
        return
    with _PROCESS_LOCK:
        _PROCESS_SESSIONS[context.identity.run_key] = session


def unregister_process_session(session: Session) -> None:
    context = session.context
    if context is None:
        return
    with _PROCESS_LOCK:
        if _PROCESS_SESSIONS.get(context.identity.run_key) is session:
            _PROCESS_SESSIONS.pop(context.identity.run_key, None)


def process_session(context_json: str | None) -> Session | None:
    """The session this process attached for that context, if it attached one."""
    if not context_json:
        return None
    key = Context.from_json(context_json).identity.run_key
    with _PROCESS_LOCK:
        return _PROCESS_SESSIONS.get(key)


def only_process_session() -> Session | None:
    """The session this process attached, when there is exactly one.

    Hooks that receive no config handle (the profiler decorator) use this. A
    process taking part in two runs emits nothing rather than guessing one.
    """
    with _PROCESS_LOCK:
        if len(_PROCESS_SESSIONS) != 1:
            return None
        return next(iter(_PROCESS_SESSIONS.values()))


def open_driver_session(
    config: Any,
    *,
    project_name: str | None = None,
    experiment_name: str | None = None,
) -> DriverSession:
    """Create the run exactly once, in the driver process (S3 §3 step 1).

    The deployment configuration comes from the ``trainer.rl_insight`` node and
    the environment, exactly as the legacy facade did; nothing else in the
    trainer reads it again.
    """
    project = (
        project_name
        if project_name is not None
        else _read(config, "trainer.project_name", "")
    )
    experiment = (
        experiment_name
        if experiment_name is not None
        else _read(config, "trainer.experiment_name", "")
    )
    sdk_config = _read(config, "trainer.rl_insight")
    metadata = {
        "project": project or "",
        "experiment_name": experiment or "",
    }
    session = open_session(
        metadata,
        config=_plain(sdk_config) if sdk_config else None,
    )
    register_process_session(session)
    context = session.context
    return DriverSession(
        session=session,
        context_json=context.to_json() if context is not None else None,
    )


def attach(context_json: str | None, *, component: str = "") -> Session | None:
    """Join the driver's run in this process; never creates an identity."""
    if not context_json:
        return None
    session = sdk_attach(context_json)
    register_process_session(session)
    if component:
        logger.debug("[rl-insight] attached %s to run %s", component, session.identity)
    return session


def attach_from_config(config: Any, *, component: str = "") -> Session | None:
    """Attach the process that received the trainer config."""
    return attach(read_trainer_context(config), component=component)


def attach_rollout_session(rollout_config: Any) -> Session | None:
    """Attach a rollout server actor that received the declared context."""
    return attach(
        read_rollout_context(rollout_config),
        component=read_rollout_component(rollout_config),
    )


def close(
    session: Session | None, *, deadline_seconds: float = 5.0
) -> dict[str, Any] | None:
    """Bounded, idempotent cleanup of one process's own session (S3 §4.3)."""
    if session is None:
        return None
    report = session.close(deadline_seconds=deadline_seconds)
    unregister_process_session(session)
    outcome = report.to_dict()
    if not report.closed or report.errors or not report.traces_flushed:
        logger.warning("[rl-insight] session close report: %s", outcome)
    return outcome


def _plain(node: Any) -> Mapping[str, Any]:
    if not isinstance(node, Mapping):
        raise TypeError(
            "trainer.rl_insight must be a mapping of SDK configuration keys"
        )
    return {str(key): value for key, value in node.items()}


# ---------------------------------------------------------------------------
# emit hooks
# ---------------------------------------------------------------------------
def log_scalars(
    session: Session | None, data: Mapping[str, Any], step: Any = None
) -> int:
    """Export scalar training metrics; returns the number actually written.

    Metric names keep the legacy transformation (``/`` becomes ``_``) so
    existing dashboards do not change.
    """
    if session is None:
        return 0
    written = 0
    for key, value in data.items():
        try:
            scalar = float(value)
        except (TypeError, ValueError):
            continue
        session.metric_gauge(str(key).replace("/", "_"), scalar)
        written += 1
    return written


@dataclass
class _Lane:
    occupant: str = ""
    count: int = 0
    start_ns: int = 0
    attributes: dict[str, Any] = field(default_factory=dict)


_LANES: weakref.WeakKeyDictionary[Session, dict[str, _Lane]] = (
    weakref.WeakKeyDictionary()
)
_LANES_LOCK = threading.Lock()


@contextmanager
def trace_state(
    session: Session | None,
    name: str,
    *,
    state_lane_id: str | int | None = None,
    **labels: Any,
) -> Iterator[None]:
    """Report one training/rollout state interval on a logical lane.

    Lane semantics are the ones the legacy facade already published: one state
    at a time per lane, same-name overlaps merge into a single interval, and a
    different state entered while the lane is occupied is dropped. A process
    without a session emits nothing and creates no identity.
    """
    if session is None or session.context is None:
        yield
        return

    lane_key = str(state_lane_id) if state_lane_id is not None else str(id(session))
    attributes = {
        **labels,
        "monitor.trace_segment": STATE_INTERVAL,
        "state_name": name,
        "state_lane_id": lane_key,
    }
    counted = False
    with _LANES_LOCK:
        lanes = _LANES.setdefault(session, {})
        lane = lanes.setdefault(lane_key, _Lane())
        if not lane.occupant:
            lane.occupant = name
            lane.count = 1
            lane.start_ns = time.time_ns()
            lane.attributes = attributes
            counted = True
        elif lane.occupant == name:
            lane.count += 1
            counted = True
    try:
        yield
    finally:
        emitted: tuple[int, int, dict[str, Any], str] | None = None
        with _LANES_LOCK:
            lanes = _LANES.get(session) or {}
            lane = lanes.get(lane_key)
            if counted and lane is not None:
                lane.count -= 1
                if lane.count == 0:
                    emitted = (
                        lane.start_ns,
                        time.time_ns(),
                        lane.attributes,
                        lane.occupant,
                    )
                    lanes.pop(lane_key, None)
        if emitted is not None:
            # Emit outside the lock: a slow exporter must not block other lanes.
            session.trace_span(
                emitted[3],
                start_time_ns=emitted[0],
                end_time_ns=emitted[1],
                attributes=emitted[2],
            )


# ---------------------------------------------------------------------------
# endpoint publication
# ---------------------------------------------------------------------------
def publish_metrics_endpoint(
    session: Session | None,
    *,
    component: str,
    instance_key: str,
    advertised_host: str,
    actual_port: int,
    path: str = METRICS_PATH,
    incarnation: str | None = None,
    replica: Any = None,
    renew: bool = True,
) -> PublicationResult:
    """Publish one bound metrics endpoint and keep its lease alive (S3 §5.2).

    Only a real, bound address is ever published; a rejection is returned to the
    caller instead of being retried into a silently missing target.
    """
    if session is None:
        return PublicationResult(
            accepted=False,
            code="NO_SESSION",
            message="this process has no run session; nothing was published",
        )
    facts: dict[str, Any] = {
        "target_kind": "metrics",
        "component": component,
        "instance_key": instance_key,
        "advertised_host": advertised_host,
        "actual_port": int(actual_port),
        "scheme": "http",
        "path": path,
    }
    if incarnation:
        facts["incarnation"] = incarnation
    if replica is not None:
        facts["replica"] = replica
    result = session.publish_endpoint(facts, renewable=True)
    if result.accepted and renew:
        session.start_renewal()
    return result


def serve_trainer_metrics(
    session: Session | None,
    *,
    component: str = TRAINER_COMPONENT,
    instance_key: str = "",
    host: str = "127.0.0.1",
    advertise_host: str | None = None,
    port: int = 0,
) -> MetricsEndpoint | None:
    """Bind and publish the *logging* session's own registry (S3 §5.2).

    ``log_scalars`` writes into one session's registry; without a bound
    exporter nothing can scrape it, because the MonitorHub is a different
    process with a different registry. It must therefore be the process that
    logs the scalars — the trainer, not the driver that only created the run —
    that calls this. The trainer serves its own registry, stamps its run
    identity on the response and keeps the lease alive, exactly like every
    other endpoint owner. A rejected publication is returned in the endpoint's
    report instead of being hidden.

    The published host is the bound host: a node address no socket listens on
    would be a target the scrape route can only ever refuse.
    """
    if session is None or session.context is None:
        return None
    bind_host = host or advertise_host or "127.0.0.1"
    endpoint = session.start_metrics_endpoint(
        host=bind_host,
        port=port,
        advertise_host=advertise_host or bind_host,
        component=component,
        instance_key=instance_key or f"{component}/0",
    )
    if endpoint.publication.accepted:
        session.start_renewal()
    return endpoint


def transfer_queue_facts(
    endpoint: str, *, instance_key: str = "controller/0"
) -> dict[str, Any]:
    """The published facts of one TransferQueue metrics address.

    The incarnation is derived from the bound address, so it changes exactly
    when the exporter rebinds to another address.
    """
    host, port, path = split_address(endpoint)
    return {
        "target_kind": "metrics",
        "component": "transfer_queue",
        "instance_key": instance_key,
        "advertised_host": host,
        "actual_port": port,
        "scheme": "http",
        "path": path,
        "incarnation": hashlib.sha256(
            f"{host}:{port}{path}".encode("utf-8")
        ).hexdigest()[:32],
    }


def publish_transfer_queue_endpoint(
    session: Session | None,
    endpoint: str | None,
    *,
    instance_key: str = "controller/0",
    probe: Callable[[], str | None] | None = None,
    identity_headers_stamped: bool = False,
) -> PublicationResult:
    """Publish the TransferQueue metrics endpoint the training driver read.

    The Controller owns the exporter; this registers the address it reported
    after bind, and only when that exporter can carry the run's identity
    headers. The controlled scrape route verifies those headers on the very
    response it ingests, and the TransferQueue exporter
    (``transfer_queue.metrics.TQMetricsExporter.start``) currently starts a
    plain ``prometheus_client`` server with no hook for them, so publishing its
    address would only produce a target that is permanently refused. Instead
    this returns ``not_ready`` with the reason; stamping the headers is a
    TransferQueue-side change, and until it lands the driver must not claim the
    endpoint as a scrapeable target of this run.

    When the owning process does stamp them (``identity_headers_stamped``),
    ``probe`` is the liveness condition for the renewal: it returns the
    Controller's current address while the Controller is reachable and ``None``
    once it is not, so a dead Controller converges to no target by TTL rather
    than being kept in the scrape set by a healthy driver. A Controller that
    came back at another address replaces the same slot with the address it
    reports now.
    """
    if not endpoint:
        return PublicationResult(
            accepted=False,
            code="NO_ENDPOINT",
            message="transfer queue reported no metrics endpoint",
        )
    if session is None:
        return PublicationResult(
            accepted=False,
            code="NO_SESSION",
            message="this process has no run session; nothing was published",
        )
    facts = transfer_queue_facts(endpoint, instance_key=instance_key)
    if not identity_headers_stamped:
        return PublicationResult(
            accepted=False,
            code="IDENTITY_HEADERS_UNSUPPORTED",
            message=(
                "the TransferQueue metrics exporter does not stamp the run "
                "identity headers the controlled scrape route verifies, so its "
                "address is not published as a target of this run; "
                f"{METRIC_HEADER_LIST} must be answered by its /metrics response"
            ),
        )

    def keepalive() -> Mapping[str, Any] | None:
        current = probe() if probe is not None else None
        if not current:
            return None
        return transfer_queue_facts(str(current), instance_key=instance_key)

    result = session.publish_endpoint(
        facts, keepalive=keepalive if probe is not None else None, renewable=True
    )
    if result.accepted:
        session.start_renewal()
    return result


def split_address(
    address: str, default_path: str = METRICS_PATH
) -> tuple[str, int, str]:
    """Split ``host:port`` or ``http://host:port/path`` into its parts."""
    raw = str(address).strip()
    if "://" not in raw:
        raw = f"http://{raw}"
    parts = urlsplit(raw)
    if not parts.hostname or parts.port is None:
        raise ValueError(f"metrics address {address!r} is not a host:port address")
    return parts.hostname, int(parts.port), parts.path or default_path


# ---------------------------------------------------------------------------
# identity-aware metrics handler for framework-owned servers
# ---------------------------------------------------------------------------
def install_metrics_identity(
    app: Any,
    session: Session | None,
    *,
    incarnation: str,
    path: str = METRICS_PATH,
) -> bool:
    """Stamp this process's identity on its own ``/metrics`` response (S3 §5.3).

    The framework server keeps answering the body; only the identity headers of
    the owning instance are added, on the same response. Returns whether the
    handler was installed, so a caller without a session can report
    ``not_ready`` instead of assuming scrape ownership.
    """
    if session is None or session.context is None:
        return False
    context = session.context
    epoch = context.lifecycle_epoch if context.identity.source == MANAGED else None
    headers = identity_headers(context.identity, epoch=epoch, incarnation=incarnation)
    if getattr(app, "middleware_stack", None) is None:
        app.add_middleware(IdentityHeaderMiddleware, headers=headers, path=path)
    else:
        # vLLM's ``build_app`` ends with the SageMaker standards bootstrap, which
        # materializes the stack before returning; Starlette then refuses
        # ``add_middleware`` ("Cannot add middleware after an application has
        # started"). Wrap the built stack instead: the framework server keeps
        # answering /metrics, only the identity headers are ours.
        app.middleware_stack = IdentityHeaderMiddleware(
            app.build_middleware_stack(), headers=headers, path=path
        )
    return True


__all__ = [
    "DriverSession",
    "METRICS_PATH",
    "TRAINER_CONTEXT_KEY",
    "ROLLOUT_COMPONENT_FIELD",
    "ROLLOUT_CONTEXT_FIELD",
    "attach",
    "attach_from_config",
    "attach_rollout_session",
    "close",
    "install_metrics_identity",
    "log_scalars",
    "only_process_session",
    "open_driver_session",
    "process_session",
    "publish_metrics_endpoint",
    "publish_transfer_queue_endpoint",
    "read_rollout_component",
    "read_rollout_context",
    "read_trainer_context",
    "register_process_session",
    "serve_trainer_metrics",
    "split_address",
    "trace_state",
    "transfer_queue_facts",
    "unregister_process_session",
    "write_rollout_context",
    "write_trainer_context",
]
