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

"""Explicit SDK sessions (RFC v4.1 §4.2, S0 design §4.1).

The driver calls :func:`open_session` exactly once and hands
``session.context`` to every worker; workers call :func:`attach` and never
create a second identity. There is no process-global current run.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import urlsplit

from prometheus_client import CollectorRegistry

from ..utils.constants import MonitorDefaults
from ..utils.prometheus_utils import MetricRegistry
from .config import (
    UNSUPPORTED,
    ChannelReadiness,
    SDKConfig,
    config_from_context,
    resolve_config,
)
from .identity import (
    MANAGED,
    PLATFORM,
    Context,
    ContextError,
    RunIdentity,
    platform_identity,
    run_key,
)
from .metrics import IdentityMetricsServer, MetricsEndpoint
from .publisher import (
    RENEW_AFTER_SECONDS,
    Lease,
    ManagedPublisher,
    ProtocolError,
    PublicationResult,
    ServerClient,
    UnsupportedPublisher,
    create_publisher_adapter,
    slot_key,
    validate_publication_facts,
)
from .traces import SessionTracer

logger = logging.getLogger(__name__)

# How long a single liveness probe and the renewal-loop teardown may take. Both
# are outer bounds for calls into components this process does not own, whose
# own calls may have no timeout at all.
PROBE_TIMEOUT_SECONDS = 5.0
RENEWAL_STOP_TIMEOUT_SECONDS = 5.0

# A lease that fails with one of these codes is gone for good; renewing it again
# cannot succeed, so the process stops renewing it instead of retrying forever.
NON_RETRYABLE_LEASE_ERRORS = frozenset(
    {
        "LEASE_EXPIRED",
        "OWNERSHIP_CONFLICT",
        "STALE_EPOCH",
        "REVISION_CONFLICT",
        "RUN_ARCHIVED",
        "RUN_NOT_WRITABLE",
        "RUN_NOT_FOUND",
    }
)


@dataclass(frozen=True)
class CloseReport:
    """Result of a bounded session close."""

    closed: bool
    revoked_leases: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    traces_flushed: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "closed": self.closed,
            "revoked_leases": list(self.revoked_leases),
            "errors": list(self.errors),
            "traces_flushed": self.traces_flushed,
        }


class Session:
    """One process's view of a run: identity, publisher, leases, readiness."""

    def __init__(
        self,
        *,
        config: SDKConfig,
        context: Context | None = None,
        publisher: Any = None,
        metrics_namespace: str = MonitorDefaults.NAMESPACE,
        probe_timeout: float = PROBE_TIMEOUT_SECONDS,
    ) -> None:
        self.config = config
        self.context = context
        self.publisher = publisher or UnsupportedPublisher()
        self.probe_timeout = float(probe_timeout)
        self.closed = False
        self.close_count = 0
        self.leases: dict[str, Lease] = {}
        # Liveness callbacks of the slots this process proxies for a component it
        # does not own the exporter of (S3 §5.4).
        self._keepalive: dict[str, Callable[[], Mapping[str, Any] | None]] = {}
        # Slots this process is allowed to keep alive; a proxied slot without a
        # liveness callback is never renewed (S3 §5.4).
        self._renewable: dict[str, bool] = {}
        # Every session binds its own scrape endpoint on its own registry; there
        # is no process-global "current run" (S2 §3.2).
        self.metrics_registry = CollectorRegistry()
        self.metrics = MetricRegistry(
            namespace=metrics_namespace, registry=self.metrics_registry
        )
        self.metrics_endpoints: list[MetricsEndpoint] = []
        # Emits after close are counted no-ops, never silent successes.
        self.dropped_events = 0
        self._tracer: SessionTracer | None = None
        self._tracer_lock = threading.Lock()
        self._renewal_thread: threading.Thread | None = None
        self._renewal_stop = threading.Event()

    # -- identity ------------------------------------------------------
    @property
    def identity(self) -> RunIdentity | None:
        return self.context.identity if self.context else None

    # -- emit ----------------------------------------------------------
    def _emit_enabled(self) -> bool:
        """A closed or disabled session counts the emit instead of pretending."""
        if self.closed or self.identity is None:
            self.dropped_events += 1
            return False
        return True

    def metric_count(
        self, name: str, amount: float = 1.0, documentation: str = "", **labels: Any
    ) -> None:
        """Record a counter increment in this session's registry."""
        if not self._emit_enabled():
            return
        self.metrics.count(
            str(name),
            documentation or f"Counter {name}",
            float(amount),
            {},
            dict(labels),
        )

    def metric_gauge(
        self, name: str, value: float, documentation: str = "", **labels: Any
    ) -> None:
        """Record the latest value of a gauge in this session's registry."""
        if not self._emit_enabled():
            return
        self.metrics.value(
            str(name),
            documentation or f"Gauge {name}",
            float(value),
            {},
            dict(labels),
        )

    def metric_histogram(
        self, name: str, value: float, documentation: str = "", **labels: Any
    ) -> None:
        """Observe one sample in this session's registry."""
        if not self._emit_enabled():
            return
        self.metrics.distribution(
            str(name),
            documentation or f"Histogram {name}",
            float(value),
            {},
            dict(labels),
            buckets=None,
        )

    # -- traces --------------------------------------------------------
    @property
    def tracer(self) -> SessionTracer:
        """This session's tracer; created on first use from the frozen context."""
        with self._tracer_lock:
            if self._tracer is None:
                if self.context is None:
                    raise ContextError("a disabled session has no run to export under")
                self._tracer = SessionTracer(
                    self.context,
                    endpoint=self.config.trace_endpoint,
                )
            return self._tracer

    def trace_span(
        self,
        name: str,
        *,
        start_time_ns: int,
        end_time_ns: int,
        attributes: Mapping[str, Any] | None = None,
    ) -> None:
        """Emit one completed span through this session's exporter."""
        if not self._emit_enabled():
            return
        self.tracer.record_span(
            name,
            start_time_ns,
            end_time_ns,
            attributes=dict(attributes or {}),
        )

    # -- publishing ----------------------------------------------------
    def publish_endpoint(
        self,
        facts: Mapping[str, Any],
        *,
        keepalive: Callable[[], Mapping[str, Any] | None] | None = None,
        renewable: bool = False,
    ) -> PublicationResult:
        """Publish one bound endpoint and remember its lease.

        ``renewable`` says whether this process may keep the lease alive: it is
        true for an exporter this process owns, and for a proxied slot only
        together with ``keepalive``, which returns the facts of that
        component's current address while its liveness can be confirmed and
        ``None`` once it cannot (S3 §5.4). A slot published without either is
        left to TTL instead of being renewed by an unrelated healthy process.
        """
        if self.closed:
            return PublicationResult(
                accepted=False,
                code="SESSION_CLOSED",
                message="session is closed; publish is a counted no-op",
            )
        identity = self.identity
        if identity is None:
            return PublicationResult(
                accepted=False,
                code="INVALID_IDENTITY",
                message="no run identity in this session",
            )
        try:
            normalized = validate_publication_facts(facts)
        except ProtocolError as exc:
            return PublicationResult(accepted=False, code=exc.code, message=exc.message)
        result = self.publisher.upsert(identity, normalized)
        if result.accepted and result.lease_id:
            key = slot_key(
                identity,
                normalized["target_kind"],
                normalized["component"],
                normalized["instance_key"],
            )
            self.leases[key] = Lease(
                lease_id=result.lease_id,
                endpoint_id=result.endpoint_id or "",
                revision=int(result.revision or 0),
                lifecycle_epoch=int(result.lifecycle_epoch or 0),
                incarnation=normalized["incarnation"],
                slot=key,
            )
            self._renewable[key] = bool(renewable or keepalive is not None)
            if keepalive is not None:
                self._keepalive[key] = keepalive
        return result

    # -- metrics endpoint ----------------------------------------------
    def start_metrics_endpoint(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        advertise_host: str | None = None,
        component: str = "",
        instance_key: str = "0",
        target_kind: str = "metrics",
        incarnation: str | None = None,
    ) -> MetricsEndpoint:
        """Bind this session's registry, then publish the real bound address.

        The endpoint is only retained when the publication was accepted; the
        outcome is returned either way so the caller sees the rejection code.
        """
        identity = self.identity
        if identity is None:
            raise ContextError("a metrics endpoint requires a run identity")
        # Managed endpoints carry the lifecycle epoch the scrape route verifies;
        # platform collection has no managed epoch, so none is fabricated here.
        epoch = self.context.lifecycle_epoch if self.context else None
        if identity.source == MANAGED and epoch is None:
            raise ContextError("a managed metrics endpoint requires a lifecycle epoch")
        incarnation = incarnation or uuid.uuid4().hex
        server = IdentityMetricsServer(
            self.metrics_registry,
            identity=identity,
            epoch=epoch,
            incarnation=incarnation,
            host=host,
            port=port,
        )
        server.start()
        publication = self.publish_endpoint(
            {
                "target_kind": target_kind,
                "component": component,
                "instance_key": instance_key,
                "incarnation": incarnation,
                "advertised_host": advertise_host or host,
                "actual_port": server.port,
                "scheme": "http",
                "path": server.path,
            },
            # This process owns the exporter it just bound.
            renewable=True,
        )
        endpoint = MetricsEndpoint(server=server, publication=publication)
        if publication.accepted:
            self.metrics_endpoints.append(endpoint)
        else:
            # Nothing will scrape a rejected endpoint; do not leak the socket.
            server.stop()
        return endpoint

    # -- lease renewal -------------------------------------------------
    def start_renewal(self, interval: float = RENEW_AFTER_SECONDS) -> None:
        """Keep this process's leases alive; repeated calls are no-ops."""
        if self._renewal_thread is not None:
            return
        self._renewal_stop.clear()
        thread = threading.Thread(
            target=self._renewal_loop,
            args=(interval, self._renewal_stop),
            daemon=True,
        )
        self._renewal_thread = thread
        thread.start()

    def stop_renewal(self, timeout: float = RENEWAL_STOP_TIMEOUT_SECONDS) -> bool:
        """Stop the renewal loop; returns whether it actually stopped."""
        thread = self._renewal_thread
        if thread is None:
            return True
        self._renewal_stop.set()
        thread.join(max(0.0, float(timeout)))
        stopped = not thread.is_alive()
        if stopped:
            self._renewal_thread = None
        else:
            logger.warning(
                "[rl-insight] the lease renewal loop did not stop within %.1fs",
                float(timeout),
            )
        return stopped

    def _renewal_loop(self, interval: float, stop: threading.Event) -> None:
        while not stop.wait(interval):
            for key in list(self.leases):
                if not self._renewable.get(key):
                    # Nothing in this process can prove the component is alive,
                    # so this slot is left to TTL (S3 §5.4).
                    continue
                lease = self.leases.get(key)
                if lease is None or not self._still_alive(key, lease):
                    continue
                # The liveness callback may have replaced the slot with the
                # component's new address; renew the lease that is current now.
                lease = self.leases.get(key)
                if lease is None:
                    continue
                try:
                    outcome = self.publisher.renew(lease)
                except Exception:  # one failed renew must not kill the loop
                    logger.warning(
                        "[rl-insight] renewing lease %s failed",
                        lease.lease_id,
                        exc_info=True,
                    )
                    continue
                if outcome.accepted:
                    if outcome.renew_after:
                        interval = float(outcome.renew_after)
                elif outcome.code in NON_RETRYABLE_LEASE_ERRORS:
                    # The responsible process can no longer prove the component
                    # is alive: stop renewing it instead of holding the slot.
                    self._forget_slot(key)

    def _still_alive(self, key: str, lease: Lease) -> bool:
        """Confirm a proxied component's liveness; False means stop renewing.

        A callback that cannot answer within the probe budget, or that reports a
        different incarnation, means the endpoint we hold is no longer the
        component's: the slot is dropped (TTL reclaims it) or replaced with the
        revision we hold.
        """
        keepalive = self._keepalive.get(key)
        if keepalive is None:
            return True
        facts = self._probe(keepalive, key)
        if facts is None:
            self._forget_slot(key)
            return False
        if str(facts.get("incarnation") or "") != lease.incarnation:
            self.publish_endpoint(
                dict(facts, expected_revision=lease.revision),
                keepalive=keepalive,
                renewable=True,
            )
            return False
        return True

    def _probe(
        self, keepalive: Callable[[], Mapping[str, Any] | None], key: str
    ) -> Mapping[str, Any] | None:
        """Run one liveness probe under a bounded wait.

        A probe talks to a component this process does not own, and its own
        call may have no timeout at all (a Ray actor call, for instance), so it
        runs on a worker thread that is given ``probe_timeout`` to answer. A
        probe that cannot answer inside that budget counts as "liveness cannot
        be confirmed": the slot stops being renewed instead of blocking the
        renewal loop - and the whole training shutdown - forever.
        """
        outcome: list[Mapping[str, Any] | None] = []
        failure: list[BaseException] = []

        def call() -> None:
            try:
                outcome.append(keepalive())
            except BaseException as exc:  # reported, never raised into the loop
                failure.append(exc)

        probe = threading.Thread(
            target=call, name=f"rl-insight-probe-{key}", daemon=True
        )
        probe.start()
        deadline = time.monotonic() + float(self.probe_timeout)
        # Wait in slices so a shutdown is not held up by a probe that never
        # answers: the slot stops being renewed either way.
        while probe.is_alive() and not self._renewal_stop.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            probe.join(min(0.05, remaining))
        if probe.is_alive():
            logger.warning(
                "[rl-insight] liveness check for %s did not answer within %.1fs",
                key,
                float(self.probe_timeout),
            )
            return None
        if failure:
            logger.warning(
                "[rl-insight] liveness check for %s failed: %s", key, failure[0]
            )
            return None
        return outcome[0] if outcome else None

    def _forget_slot(self, key: str) -> None:
        self.leases.pop(key, None)
        self._keepalive.pop(key, None)
        self._renewable.pop(key, None)

    # -- readiness -----------------------------------------------------
    def readiness(self) -> dict[str, ChannelReadiness]:
        base = dict(self.config.readiness())
        if self.context is None:
            for name in base:
                base[name] = ChannelReadiness(UNSUPPORTED, "NO_RUN_IDENTITY")
        if self.closed:
            # A closed session publishes nothing and scrapes nothing.
            for name in base:
                base[name] = ChannelReadiness("not_ready", "SESSION_CLOSED")
        return base

    # -- shutdown ------------------------------------------------------
    def close(self, deadline_seconds: float = 5.0) -> CloseReport:
        """Stop publishing/renewal, flush traces, revoke leases; idempotent.

        ``deadline_seconds`` is the budget for the whole close, not per lease:
        the trace flush and every revocation share the remaining time (S3 §4.3).
        """
        if self.closed:
            return CloseReport(closed=True)
        deadline = time.monotonic() + float(deadline_seconds)
        errors: list[str] = []
        if not self.stop_renewal(max(0.0, deadline - time.monotonic())):
            errors.append("RENEWAL_NOT_STOPPED")
        self._keepalive.clear()
        for endpoint in self.metrics_endpoints:
            # Each blocking step shares the one deadline the caller gave close.
            if not endpoint.server.stop(max(0.0, deadline - time.monotonic())):
                errors.append("METRICS_ENDPOINT_NOT_STOPPED")
        self.metrics_endpoints.clear()

        traces_flushed = True
        tracer = self._tracer
        if tracer is not None and tracer.enabled:
            traces_flushed = tracer.flush(deadline - time.monotonic())
            if not traces_flushed:
                errors.append("TRACE_FLUSH_TIMEOUT")
            tracer.shutdown(deadline - time.monotonic())

        revoked: list[str] = []
        for key, lease in list(self.leases.items()):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                errors.append(
                    f"close budget exhausted before revoking {lease.lease_id}"
                )
                break
            # The revocation shares the close deadline; a lease the transport
            # cannot be told about within it is reported, not waited on.
            outcome = self.publisher.delete(lease, timeout=remaining)
            if outcome.accepted:
                revoked.append(lease.lease_id)
                self.leases.pop(key, None)
            else:
                errors.append(f"{lease.lease_id}: {outcome.code}")
        self.closed = True
        self.close_count += 1
        return CloseReport(
            closed=True,
            revoked_leases=tuple(revoked),
            errors=tuple(errors),
            traces_flushed=traces_flushed,
        )

    def __enter__(self) -> Session:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _managed_client(config: SDKConfig, client: Any = None) -> Any:
    if client is not None:
        return client
    if not config.server_url:
        raise ContextError("managed session requires a server URL")
    return ServerClient(config.server_url)


def _managed_trace_endpoint(server: Any, server_url: str | None) -> str | None:
    """Resolve the managed OTLP/HTTP address from the server's discovery payload.

    The server URL is the monitoring service address, so a path the server
    advertises is resolved against it; an absolute address is used as
    advertised (RFC v4.1 §4.3, S3 §6).
    """
    advertised = (server.services().get("services") or {}).get("otlp_http")
    if not advertised:
        return None
    text = str(advertised)
    if urlsplit(text).scheme:
        return text
    if not server_url:
        return None
    path = text if text.startswith("/") else f"/{text}"
    return f"{server_url.rstrip('/')}{path}"


def _context_from_run(config: SDKConfig, run: Mapping[str, Any]) -> Context:
    return Context(
        identity=RunIdentity.from_dict(run["identity"]),
        services=dict(run.get("services") or {}),
        capabilities=tuple(run.get("capabilities") or ()),
        publisher={
            "adapter": config.publisher_adapter or MANAGED,
            "config_ref": config.publisher_config_ref,
        },
        server_instance_id=run.get("server_instance_id"),
        lifecycle_epoch=run.get("lifecycle_epoch"),
    )


def open_session(
    metadata: Mapping[str, Any] | None = None,
    *,
    config: SDKConfig | Mapping[str, Any] | None = None,
    client: Any = None,
    env: Mapping[str, str] | None = None,
    metrics_namespace: str = MonitorDefaults.NAMESPACE,
) -> Session:
    """Open the driver session once: resolve config, create or resume the run."""
    metadata = dict(metadata or {})
    project = metadata.get("project", "")
    experiment_name = metadata.get("experiment_name", "")
    resolved = (
        config if isinstance(config, SDKConfig) else resolve_config(config, env=env)
    )

    if not resolved.enabled:
        return Session(config=resolved, context=None, publisher=UnsupportedPublisher())

    if resolved.mode == PLATFORM:
        identity = platform_identity(resolved.task_id, project, experiment_name)
        services = (
            {"otlp_http": resolved.trace_endpoint} if resolved.trace_endpoint else {}
        )
        context = Context(
            identity=identity,
            services=services,
            capabilities=("identity_v1", "session_context_v1"),
            publisher={
                "adapter": resolved.publisher_adapter,
                "config_ref": resolved.publisher_config_ref,
            },
        )
        return Session(
            config=resolved,
            context=context,
            publisher=create_publisher_adapter(
                resolved.publisher_adapter,
                context=context,
                config=resolved,
                client=client,
            ),
            metrics_namespace=metrics_namespace,
        )

    server = _managed_client(resolved, client)
    if resolved.resume_id:
        run = server.resume_run(
            run_key(MANAGED, resolved.resume_id),
            project=project,
            expected_revision=int(metadata.get("expected_revision") or 0),
        )
    else:
        run = server.create_run(
            project=project,
            experiment_name=experiment_name or None,
            idempotency_key=str(metadata.get("idempotency_key") or uuid.uuid4()),
        )
    services = {"server": resolved.server_url, **run.get("services", {})}
    # Managed resolves its own OTLP/HTTP address from the server it was pointed
    # at, so no extra endpoint setting is needed and every worker inherits the
    # address from the serialized context (S3 §6).
    try:
        discovered = _managed_trace_endpoint(server, resolved.server_url)
    except ProtocolError as exc:
        discovered = None
        resolved = replace(
            resolved,
            diagnostics=resolved.diagnostics
            + (f"managed trace discovery failed: {exc.code}",),
            trace_reason_code="TRACE_DISCOVERY_FAILED",
        )
    endpoint = discovered or services.get("otlp_http") or resolved.trace_endpoint
    if endpoint:
        resolved = replace(resolved, trace_endpoint=endpoint)
        services["otlp_http"] = endpoint
    context = _context_from_run(
        resolved,
        {**run, "services": services},
    )
    return Session(
        config=resolved,
        context=context,
        publisher=ManagedPublisher(server, context.identity),
        metrics_namespace=metrics_namespace,
    )


def attach(
    context: Context | Mapping[str, Any] | str,
    *,
    config: SDKConfig | Mapping[str, Any] | None = None,
    client: Any = None,
    env: Mapping[str, str] | None = None,
    metrics_namespace: str = MonitorDefaults.NAMESPACE,
) -> Session:
    """Join an existing run in this process; never creates a new identity.

    The worker derives mode, address, identity and epoch from the context alone:
    its own ``RL_INSIGHT_*`` environment and config file are not consulted, and
    a conflicting value only produces a diagnostic (S3 §4.1). An explicit
    :class:`SDKConfig` is ignored the same way: a worker may not pick the mode
    or identity of the run it joins.
    """
    if isinstance(context, str):
        context = Context.from_json(context)
    elif not isinstance(context, Context):
        context = Context.from_dict(context)

    resolved = config_from_context(
        context,
        overrides=None if isinstance(config, SDKConfig) else config,
        env=env,
    )
    if isinstance(config, SDKConfig):
        resolved = replace(
            resolved,
            diagnostics=resolved.diagnostics
            + (
                "attach ignores an explicit SDKConfig: the context carries the "
                "frozen mode, services and identity",
            ),
        )

    adapter = (context.publisher or {}).get("adapter")
    if adapter == MANAGED and context.services.get("server"):
        publisher: Any = ManagedPublisher(
            _managed_client(resolved, client), context.identity
        )
    else:
        publisher = create_publisher_adapter(
            adapter, context=context, config=resolved, client=client
        )
    return Session(
        config=resolved,
        context=context,
        publisher=publisher,
        metrics_namespace=metrics_namespace,
    )
