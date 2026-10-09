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

"""Per-session Prometheus metrics endpoint (S2 §4.2).

Each session owns its own :class:`CollectorRegistry`; this module exposes that
registry on a real loopback socket and stamps every response with the run
identity headers the controlled scrape route verifies on the same response it
ingests. The headers come from the immutable context passed to the constructor,
never from the request.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any

from prometheus_client import CollectorRegistry, generate_latest

from .identity import (
    EPOCH_HEADER,
    INCARNATION_HEADER,
    RUN_ID_HEADER,
    RunIdentity,
    encode_run_id_header,
)
from .publisher import PublicationResult

logger = logging.getLogger(__name__)

# Wire form fixed by S2 §4.2: the scrape route rejects any other content type.
CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# How long ``shutdown()`` may take to be noticed. It is the only part of
# :meth:`IdentityMetricsServer.stop` that waits, so it bounds a scrape loop
# against a caller's remaining shutdown budget.
_POLL_INTERVAL_SECONDS = 0.1


def identity_headers(
    identity: RunIdentity, *, epoch: int | None, incarnation: str
) -> dict[str, str]:
    """Identity response headers for one exporter instance (S2 §4.2).

    Managed endpoints carry the epoch the scrape route verifies; platform
    endpoints have no managed lifecycle, so no epoch header is fabricated.
    """
    headers = {
        RUN_ID_HEADER: encode_run_id_header(identity.id),
        INCARNATION_HEADER: incarnation,
    }
    if epoch is not None:
        headers[EPOCH_HEADER] = str(int(epoch))
    return headers


@dataclass(frozen=True)
class MetricsEndpoint:
    """One bound metrics endpoint and its publication outcome."""

    server: IdentityMetricsServer
    publication: PublicationResult


class IdentityMetricsServer:
    """Serve one session's registry with its run identity headers."""

    def __init__(
        self,
        registry: CollectorRegistry,
        *,
        identity: RunIdentity,
        epoch: int | None,
        incarnation: str,
        host: str = "127.0.0.1",
        port: int = 0,
        path: str = "/metrics",
    ) -> None:
        self.registry = registry
        self.identity = identity
        self.epoch = None if epoch is None else int(epoch)
        self.incarnation = incarnation
        self.host = host
        self.path = path
        # Managed endpoints carry the epoch the scrape route verifies; platform
        # endpoints have no managed lifecycle, so no epoch header is fabricated
        # (S3 §2.3).
        self.headers = identity_headers(
            identity, epoch=self.epoch, incarnation=incarnation
        )
        self._requested_port = int(port)
        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None

    def start(self) -> None:
        """Bind the port and serve on a background thread."""
        server = ThreadingHTTPServer(
            (self.host, self._requested_port), self._handler_class()
        )
        # An in-flight scrape must never hold up ``stop``: handler threads are
        # already daemons, so the server does not join them on close.
        server.block_on_close = False
        thread = Thread(
            target=server.serve_forever,
            args=(_POLL_INTERVAL_SECONDS,),
            daemon=True,
        )
        self._server = server
        self._thread = thread
        thread.start()

    def stop(self, timeout: float | None = None) -> bool:
        """Shut the server down within ``timeout``; returns whether it stopped.

        ``timeout`` is the caller's remaining shutdown budget, so a scrape
        handler that is still running cannot push the close past its deadline;
        ``None`` waits for the serve loop without a bound.
        """
        server, thread = self._server, self._thread
        if server is None or thread is None:
            return True
        self._server = None
        self._thread = None
        server.shutdown()
        server.server_close()
        thread.join(None if timeout is None else max(0.0, float(timeout)))
        stopped = not thread.is_alive()
        if not stopped:
            logger.warning(
                "[rl-insight] the metrics endpoint on %s:%s did not stop in time",
                self.host,
                self._requested_port,
            )
        return stopped

    @property
    def port(self) -> int:
        """The actual bound port; only valid after :meth:`start`."""
        if self._server is None:
            raise RuntimeError("metrics server is not started")
        return int(self._server.server_address[1])

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}{self.path}"

    def _handler_class(self) -> type[BaseHTTPRequestHandler]:
        owner = self

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path != owner.path:
                    self.send_error(404)
                    return
                body = generate_latest(owner.registry)
                self.send_response(200)
                for name, value in owner.headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Type", CONTENT_TYPE)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:
                return None

        return _Handler


class IdentityHeaderMiddleware:
    """Stamp one exporter's identity on its own metrics response (S3 §5.3).

    A framework-owned metrics server (vLLM, SGLang, TransferQueue) answers
    ``/metrics`` itself; wrapping that app keeps the body and the identity on the
    same response, which is what the controlled scrape route verifies. The
    headers come from the frozen context of the process that owns the exporter,
    never from the request, and they overwrite same-named business headers.

    Pure ASGI: no web framework is imported and no 0.0.0.0 or proxy address is
    involved.
    """

    def __init__(
        self,
        app: Any,
        *,
        headers: Mapping[str, str],
        path: str = "/metrics",
    ) -> None:
        self.app = app
        self.headers = {str(name): str(value) for name, value in headers.items()}
        self.path = path
        self._owned = {name.lower() for name in self.headers}

    async def __call__(
        self,
        scope: Mapping[str, Any],
        receive: Callable[[], Awaitable[Any]],
        send: Callable[[Any], Awaitable[None]],
    ) -> None:
        if scope.get("type") != "http" or _norm_path(scope.get("path")) != self.path:
            await self.app(scope, receive, send)
            return

        async def send_with_identity(message: Any) -> None:
            if message.get("type") == "http.response.start":
                kept = [
                    (name, value)
                    for name, value in message.get("headers") or ()
                    if name.decode("latin-1").lower() not in self._owned
                ]
                kept.extend(
                    (name.encode("latin-1"), value.encode("latin-1"))
                    for name, value in self.headers.items()
                )
                message = {**message, "headers": kept}
            await send(message)

        await self.app(scope, receive, send_with_identity)


def _norm_path(raw: Any) -> str:
    path = str(raw or "")
    if not path.startswith("/"):
        return path
    return path.rstrip("/") or "/"
