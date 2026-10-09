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

"""Per-session OTLP/HTTP trace export (S3 §6).

Each session owns its provider and its Resource: the identity attribute is
``rl_insight.run_id`` in managed mode and ``rl_insight.task_id`` in platform
mode, and managed traffic carries the same identity headers the OTLP ingress
route verifies. The provider is held explicitly and closed by the session; the
global ``set_tracer_provider`` is never touched, so two sessions in one process
cannot overwrite each other's identity.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping
from typing import Any

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from ..utils.constants import MonitorDefaults
from .identity import (
    EPOCH_HEADER,
    MANAGED,
    RUN_ID_HEADER,
    SERVER_INSTANCE_HEADER,
    Context,
    encode_run_id_header,
)

logger = logging.getLogger(__name__)

__all__ = ["SessionTracer", "identity_resource_attributes", "managed_trace_headers"]

# Bounded queue: a full queue drops spans loudly through the batch processor's
# own logger instead of growing without limit.
MAX_QUEUE_SIZE = 2048
# One OTLP/HTTP request budget; keeps a hung collector from holding a session
# open past its close budget.
EXPORT_TIMEOUT_SECONDS = 3.0


def identity_resource_attributes(context: Context, service_name: str) -> dict[str, Any]:
    """Resource attributes carrying the authoritative identity plus display data."""
    identity = context.identity
    return {
        SERVICE_NAME: service_name,
        identity.trace_attribute: identity.id,
        "rl_insight.project": identity.project,
        "rl_insight.experiment_name": identity.experiment_name or "",
    }


def managed_trace_headers(context: Context) -> dict[str, str]:
    """Identity headers for managed OTLP/HTTP traffic; empty for platform."""
    if context.identity.source != MANAGED:
        return {}
    headers = {RUN_ID_HEADER: encode_run_id_header(context.identity.id)}
    if context.server_instance_id:
        headers[SERVER_INSTANCE_HEADER] = context.server_instance_id
    if context.lifecycle_epoch is not None:
        headers[EPOCH_HEADER] = str(context.lifecycle_epoch)
    return headers


class SessionTracer:
    """One session's tracer: identity Resource, HTTP exporter, owned provider."""

    def __init__(
        self,
        context: Context,
        *,
        endpoint: str | None = None,
        service_name: str = MonitorDefaults.NAMESPACE,
        max_queue_size: int = MAX_QUEUE_SIZE,
    ) -> None:
        self.context = context
        self.endpoint = endpoint
        self.service_name = service_name
        self._provider: TracerProvider | None = None
        self._processor: BatchSpanProcessor | None = None
        if not endpoint:
            return
        resource = Resource.create(
            identity_resource_attributes(context, service_name),
        )
        exporter = OTLPSpanExporter(
            endpoint=endpoint,
            headers=managed_trace_headers(context) or None,
            timeout=EXPORT_TIMEOUT_SECONDS,
        )
        provider = TracerProvider(resource=resource)
        processor = BatchSpanProcessor(exporter, max_queue_size=max_queue_size)
        provider.add_span_processor(processor)
        self._provider = provider
        self._processor = processor

    @property
    def enabled(self) -> bool:
        """True when this session has an endpoint to export to."""
        return self._provider is not None

    def record_span(
        self,
        name: str,
        start_time_ns: int,
        end_time_ns: int,
        *,
        attributes: Mapping[str, Any] | None = None,
    ) -> None:
        """Emit one root span; a session without an exporter is a no-op."""
        if self._provider is None:
            return
        span = self._provider.get_tracer(__name__).start_span(
            name,
            start_time=int(start_time_ns),
            attributes=dict(attributes or {}),
        )
        span.end(end_time=int(end_time_ns))

    def flush(self, timeout_seconds: float) -> bool:
        """Flush pending spans within the budget; ``False`` means unconfirmed.

        ``BatchSpanProcessor.force_flush`` blocks for as long as its HTTP export
        takes, so it runs on a helper thread that is abandoned (daemon) when the
        budget expires — the session close never blocks past its deadline.
        """
        if self._provider is None or self._processor is None:
            return True
        processor = self._processor
        outcome: list[bool] = []
        thread = threading.Thread(
            target=lambda: outcome.append(processor.force_flush()), daemon=True
        )
        thread.start()
        thread.join(max(0.0, float(timeout_seconds)))
        return bool(outcome) and outcome[0]

    def shutdown(self, timeout_seconds: float = 0.0) -> None:
        """Stop the batch processor within the budget; repeated calls are no-ops.

        The processor, not the provider, is stopped directly: the provider's own
        ``shutdown`` flushes with a fixed 30s budget, which would blow the
        session's total close budget.
        """
        processor, self._processor = self._processor, None
        self._provider = None
        if processor is None:
            return
        thread = threading.Thread(target=processor.shutdown, daemon=True)
        thread.start()
        thread.join(max(0.0, float(timeout_seconds)))
