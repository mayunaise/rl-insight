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

"""Ray named actor that collects monitor events and exposes a metrics endpoint."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import ray
from omegaconf import DictConfig

from ..sdk.identity import Context
from ..sdk.session import Session, attach
from ..sdk.traces import SessionTracer
from ..server.http_api import get_server_services
from ..server.network import format_host_port, service_url_from_server_url
from ..utils import (
    MetricRegistry,
    MonitorEventKind,
    OpenTelemetryTraceCollector,
    start_metrics_http_server,
    update_prometheus_config,
)
from ..utils.constants import MonitorRayActor
from .base import MonitorCollector

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

__all__ = ["MonitorHubActor"]


@ray.remote
class MonitorHubActor(MonitorCollector):
    """Ray detached actor: receives monitor events from trainers, serves ``/metrics``, optional OTLP traces.

    Actor methods run one at a time (no ``max_concurrency``), so hub state updates are serialized.

    Two ways in, both job-scoped:

    * session mode (S3): the caller passes the frozen ``context`` of one run. The
      hub attaches itself, owns its own registry, metrics endpoint and OTLP
      provider, publishes its real bound address and renews its own lease.
    * legacy mode: no context; the hub discovers the server services and writes
      its address into the shared file-SD store, as before.
    """

    def __init__(
        self,
        conf: DictConfig,
        context: Context | Mapping[str, Any] | None = None,
    ) -> None:
        """
        Args:
            conf: Merged monitor config with ``server`` and ``prometheus`` sections.
            context: Frozen run context; when set this hub serves exactly one run.
        """
        self._conf = conf
        self._context = (
            None
            if context is None
            else (
                context if isinstance(context, Context) else Context.from_dict(context)
            )
        )
        self._session: Session | None = None
        # Legacy mode uses the shared collector; session mode uses the session's
        # own identity-bound tracer. Both expose ``enabled``/``record_span``.
        self._trace_collector: OpenTelemetryTraceCollector | SessionTracer
        self._events_applied = 0
        self._node_ip = ray.util.get_node_ip_address()
        self._metrics_port = int(self._conf.prometheus.metrics_report_port)
        self._event_handlers = {
            MonitorEventKind.COUNTER: self._handle_counter,
            MonitorEventKind.GAUGE: self._handle_gauge,
            MonitorEventKind.HISTOGRAM: self._handle_histogram,
            MonitorEventKind.TRACE: self._handle_trace,
        }

        namespace = str(self._conf.server.namespace)
        if self._context is None:
            services = get_server_services()
            trace_endpoint = service_url_from_server_url(
                str(self._conf.server.url),
                services.get("otlp_port"),
                "/v1/traces",
            )
            self._registry = MetricRegistry(namespace=namespace)
            self._trace_collector = OpenTelemetryTraceCollector(
                namespace=namespace,
                endpoint=trace_endpoint,
            )
            start_metrics_http_server(self._metrics_port, addr=self._node_ip)
            update_prometheus_config(
                [format_host_port(self._node_ip, self._metrics_port)]
            )
        else:
            self._session = attach(self._context, metrics_namespace=namespace)
            self._registry = self._session.metrics
            self._trace_collector = self._session.tracer
            endpoint = self._session.start_metrics_endpoint(
                component=MonitorRayActor.COMPONENT,
                instance_key=f"hub/{self._job_id()}",
                host=self._node_ip,
                port=self._metrics_port,
                advertise_host=self._node_ip,
            )
            if endpoint.publication.accepted:
                self._session.start_renewal()
                self._metrics_port = endpoint.server.port
            else:
                logger.error(
                    "[rl-insight] MonitorHubActor endpoint not published: %s %s",
                    endpoint.publication.code,
                    endpoint.publication.message,
                )
                self._metrics_port = 0
        logger.info(
            "[rl-insight] MonitorHubActor HTTP bind %s:%s",
            self._node_ip,
            self._metrics_port,
        )

    @staticmethod
    def _job_id() -> str:
        """Return the current Ray job id as a string."""
        job_id = ray.get_runtime_context().get_job_id()
        return job_id if isinstance(job_id, str) else job_id.hex()

    def apply_event(self, event: dict[str, Any]) -> None:
        """Dispatch one event by ``kind``: counter/gauge/histogram update Prometheus registry, trace exports OTLP.

        Args:
            event: Must include ``kind``; metric kinds need ``name``/``value``; trace needs ``start_time_ns``/``end_time_ns``.
        """
        self._events_applied += 1
        try:
            kind = event["kind"]
        except KeyError as e:
            raise ValueError(f"Event missing required field: {e!r}") from e

        handler = self._event_handlers.get(kind)
        if handler is None:
            raise ValueError(f"Unknown event kind: {kind!r}")
        handler(event)

    def get_status(self) -> dict[str, Any]:
        """Return a small status dict for debugging (endpoints, counters).

        Returns:
            Dict with ``actor_name``, ``namespace`` (Ray placement namespace, not metric prefix), scrape URL, flags.
        """
        return {
            "actor_name": MonitorRayActor.NAME,
            "namespace": MonitorRayActor.NAMESPACE,
            "node_ip": self._node_ip,
            "metrics_endpoint": (
                "http://"
                + format_host_port(self._node_ip, self._metrics_port)
                + "/metrics"
            ),
            "prometheus_metrics_enabled": True,
            "otel_traces_enabled": self._trace_collector.enabled,
            "events_applied": self._events_applied,
        }

    def get_identity(self) -> dict[str, Any] | None:
        """Return this hub's run identity, or ``None`` for a legacy hub.

        A caller that finds an existing hub compares this with its own context
        before reusing it: two runs in one Ray job must never share a hub (S3 §4.2).
        """
        return None if self._context is None else self._context.identity.to_dict()

    def shutdown(self) -> None:
        """Stop this hub's own observation resources; a legacy hub has none to stop.

        Called by the last borrower in the owning process, never by a worker's
        session close.
        """
        if self._session is not None:
            self._session.close()

    def _handle_counter(self, event: dict[str, Any]) -> None:
        """Increment a Prometheus counter from a ``counter`` event payload."""
        self._registry.count(
            event["name"],
            event.get("documentation") or "",
            float(event["value"]),
            {},
            dict(event.get("labels") or {}),
        )

    def _handle_gauge(self, event: dict[str, Any]) -> None:
        """Set a Prometheus gauge from a ``gauge`` event payload."""
        self._registry.value(
            event["name"],
            event.get("documentation") or "",
            float(event["value"]),
            {},
            dict(event.get("labels") or {}),
        )

    def _handle_histogram(self, event: dict[str, Any]) -> None:
        """Observe one sample on a Prometheus histogram from a ``histogram`` event payload."""
        self._registry.distribution(
            event["name"],
            event.get("documentation") or "",
            float(event["value"]),
            {},
            dict(event.get("labels") or {}),
            buckets=None,
        )

    def _handle_trace(self, event: dict[str, Any]) -> None:
        """Export one trace span via OTLP (no-op if collector disabled)."""
        if not self._trace_collector.enabled:
            return
        attrs = dict(event.get("attributes") or {})
        self._export_trace_span(
            event["name"],
            int(event["start_time_ns"]),
            int(event["end_time_ns"]),
            attrs,
        )

    def _export_trace_span(
        self,
        name: str,
        start_time_ns: int,
        end_time_ns: int,
        attributes: dict[str, Any],
    ) -> None:
        """Export one root span via OTLP (no-op if collector disabled)."""
        if not self._trace_collector.enabled:
            return
        self._trace_collector.record_span(
            name,
            start_time_ns,
            end_time_ns,
            attributes=attributes,
        )
