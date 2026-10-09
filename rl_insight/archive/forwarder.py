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

"""Trace forwarding to Tempo from the durable archive (S2 §5).

There is no message queue: ``ArchiveStore.pending_forward`` is the durable
backlog and ``mark_forwarded`` is the cursor. Only the contiguous prefix that
Tempo accepted advances the cursor, so a failed batch is retried instead of
lost. A partially accepted batch is reported as degraded and the whole batch is
kept, because guessing which spans were rejected is worse than replaying them.
"""

from __future__ import annotations

import base64
import binascii
import logging
import threading
import time
from typing import Any, Callable

import requests
from google.protobuf.message import DecodeError  # type: ignore[import-untyped]
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)

from ..sdk.identity import RunIdentity
from ..server.run_registry import ACTIVE
from .records import TRACES, SpanRecord
from .writer import ArchiveStore

logger = logging.getLogger(__name__)

FORWARD_TIMEOUT_SECONDS = 10.0
FORWARD_BATCH_SIZE = 200
FORWARD_INTERVAL_SECONDS = 2.0

LOSSLESS_REPLAY_UNAVAILABLE = "LOSSLESS_REPLAY_UNAVAILABLE"
ARCHIVE_PAYLOAD_CORRUPT = "ARCHIVE_PAYLOAD_CORRUPT"
TEMPO_UNAVAILABLE = "TEMPO_UNAVAILABLE"
TEMPO_REJECTED = "TEMPO_REJECTED"
PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
# A round that raised instead of returning a per-run code.
FORWARD_ROUND_FAILED = "FORWARD_ROUND_FAILED"


class TraceForwarder:
    """One bounded worker that forwards archived trace envelopes to Tempo."""

    def __init__(
        self,
        registry: Any,
        store: ArchiveStore,
        *,
        tempo_endpoint: str,
        batch_size: int = FORWARD_BATCH_SIZE,
        timeout: float = FORWARD_TIMEOUT_SECONDS,
        interval: float = FORWARD_INTERVAL_SECONDS,
        session: Any = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self.registry = registry
        self.store = store
        self.tempo_endpoint = tempo_endpoint
        self.batch_size = int(batch_size)
        self.timeout = float(timeout)
        self.interval = float(interval)
        self.http = session if session is not None else requests.Session()
        self._now = now
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_error = ""

    # -- one round -----------------------------------------------------
    def run_once(self) -> dict[str, Any]:
        """Forward a bounded batch per active run; returns one snapshot."""
        forwarded = 0
        errors: dict[str, str] = {}
        seen = 0
        for run in self.registry.list_runs(state=ACTIVE):
            seen += 1
            identity = RunIdentity.from_dict(run["identity"])
            pending = self.store.pending_forward(identity, TRACES)
            sent_count = 0
            sent_seq = 0
            error = ""
            for seq, record in pending:
                if sent_count >= self.batch_size:
                    break
                if not isinstance(record, SpanRecord) or not record.replayable:
                    error = LOSSLESS_REPLAY_UNAVAILABLE
                    break
                error = self._send(record)
                if error:
                    break
                sent_count += 1
                sent_seq = seq
            if sent_count:
                self.store.mark_forwarded(identity, TRACES, sent_seq)
                forwarded += sent_count
            self.registry.note_forwarding(
                run["run_key"],
                backlog=len(pending) - sent_count,
                error_code=error,
            )
            if error:
                errors[run["run_key"]] = error
        return {"runs": seen, "forwarded": forwarded, "errors": errors}

    def _send(self, record: SpanRecord) -> str:
        """Send one envelope; the returned code is empty on acceptance."""
        envelope = ExportTraceServiceRequest()
        try:
            envelope.ParseFromString(base64.b64decode(record.payload_b64))
        except (DecodeError, binascii.Error, ValueError):
            return ARCHIVE_PAYLOAD_CORRUPT
        try:
            response = self.http.post(
                self.tempo_endpoint,
                data=envelope.SerializeToString(),
                headers={"Content-Type": "application/x-protobuf"},
                timeout=self.timeout,
            )
        except requests.RequestException:
            return TEMPO_UNAVAILABLE
        if response.status_code == 429 or response.status_code >= 500:
            return TEMPO_UNAVAILABLE
        if response.status_code != 200:
            return TEMPO_REJECTED
        result = ExportTraceServiceResponse()
        try:
            result.ParseFromString(response.content)
        except DecodeError:
            return TEMPO_REJECTED
        if (
            result.partial_success.rejected_spans
            or result.partial_success.error_message
        ):
            return PARTIAL_SUCCESS
        return ""

    # -- worker --------------------------------------------------------
    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="rl-insight-trace-forwarder", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval + 1.0)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                # One bad round (a store or Registry error) must not end the
                # worker silently: it is reported and retried on the next tick,
                # so the backlog stops growing without anyone noticing.
                self.last_error = FORWARD_ROUND_FAILED
                logger.exception(
                    "[rl-insight] trace forwarding round failed; retrying in %ss",
                    self.interval,
                )
            else:
                self.last_error = ""
            self._stop.wait(self.interval)

    @property
    def running(self) -> bool:
        """Whether the worker thread is still alive (a health signal)."""
        return self._thread is not None and self._thread.is_alive()


__all__ = ["TraceForwarder"]
