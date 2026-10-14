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

"""Sink interface for the direct-emit (push) backend.

A sink receives metric names **relative to its own** :attr:`prefix`; the push
client supplies the stream segment (e.g. ``trainer.`` / ``trace.`` /
``rollout.``) and the sink prepends its deployment-specific root prefix.
Timers always use microseconds.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping

TagMap = Mapping[str, str]
AttrMap = Mapping[str, Any]

_NS_PER_US = 1000


class PushSink(ABC):
    """Abstract destination for direct metric emission.

    Args:
        prefix: Deployment-specific root prefix prepended to every metric.
    """

    def __init__(self, prefix: str = "") -> None:
        self._prefix = str(prefix or "").strip(".")

    @property
    def prefix(self) -> str:
        """Root metric prefix (no trailing dot)."""
        return self._prefix

    def metric_name(self, suffix: str) -> str:
        """Return the fully qualified metric name for a prefix-relative ``suffix``."""
        suffix = str(suffix).lstrip(".")
        return f"{self._prefix}.{suffix}" if self._prefix else suffix

    @abstractmethod
    def emit_counter(self, name: str, value: float, tags: TagMap) -> None:
        """Add ``value`` to a monotonic counter identified by ``name``."""

    @abstractmethod
    def emit_store(self, name: str, value: float, tags: TagMap) -> None:
        """Record the latest gauge ``value`` for ``name``."""

    @abstractmethod
    def emit_timer(self, name: str, value_us: float, tags: TagMap) -> None:
        """Record a duration ``value_us`` (microseconds) for ``name``."""

    def emit_histogram(self, name: str, value: float, tags: TagMap) -> None:
        """Record one histogram sample ``value`` for ``name``.

        Sinks that support native distributions (e.g. OTLP/Prometheus) override
        this; the default treats the observation as a timer value, matching the
        duration histograms verl reports in microseconds.
        """
        self.emit_timer(name, value, tags)

    def emit_span(
        self,
        name: str,
        start_time_ns: int,
        end_time_ns: int,
        attributes: AttrMap,
    ) -> None:
        """Record a completed span (``trace_span``/``trace_op``/``trace_state``).

        ``name`` is the stream-routed metric name and ``attributes`` carries the
        raw span attributes (including non-scalar values). Sinks with a tracing
        backend (e.g. OTLP/Tempo) override this to keep the span structure; the
        default collapses the span to its duration timer so metrics-only
        backends (such as a time-series store) need no tracing dependency.
        """
        duration_us = max(0, int(end_time_ns) - int(start_time_ns)) / _NS_PER_US
        tags = {
            str(key): str(value)
            for key, value in (attributes or {}).items()
            if value is not None and isinstance(value, (str, bool, int, float))
        }
        self.emit_timer(name, duration_us, tags)

    def flush(self) -> None:
        """Best-effort flush of any buffered points. Default: no-op."""

    def close(self) -> None:
        """Release resources. Default: flush then no-op."""
        self.flush()
