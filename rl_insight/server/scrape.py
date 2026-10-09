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

"""Controlled scrape route of the shared Prometheus (S2 §4.2).

Prometheus scrapes ``/internal/scrape/{endpoint_id}``; this route resolves the
address from the Registry (it accepts no URL), fetches it exactly once with a
size and time budget, verifies the identity headers on that same response,
re-checks that the lease and revision did not move while the fetch was in
flight, and only then hands the body to Prometheus.
"""

from __future__ import annotations

from typing import Any

import requests
from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response

from ..sdk.identity import (
    EPOCH_HEADER,
    INCARNATION_HEADER,
    RUN_ID_HEADER,
    encode_run_id_header,
)
from ..utils.constants import PrometheusScrape
from .run_registry import RegistryError

SCRAPE_TIMEOUT_SECONDS = 5.0
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
CHUNK_BYTES = 65536

TARGET_NOT_FOUND = "TARGET_NOT_FOUND"
IDENTITY_HEADER_MISMATCH = "IDENTITY_HEADER_MISMATCH"
SCRAPE_RESPONSE_TOO_LARGE = "SCRAPE_RESPONSE_TOO_LARGE"
SCRAPE_BACKEND_UNAVAILABLE = "SCRAPE_BACKEND_UNAVAILABLE"
LEASE_CHANGED = "LEASE_CHANGED"


def _error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "code": code,
            "message": message,
            "retryable": status_code >= 500,
            "request_id": "",
            "details": {},
        },
    )


def create_scrape_router(
    registry: Any,
    *,
    timeout: float = SCRAPE_TIMEOUT_SECONDS,
    max_bytes: int = MAX_RESPONSE_BYTES,
    session: Any = None,
) -> APIRouter:
    """Routes Prometheus' controlled scrape to one leased managed endpoint."""
    router = APIRouter()
    http = session if session is not None else requests.Session()

    @router.get(PrometheusScrape.SCRAPE_ROUTE + "/{endpoint_id}")
    def scrape(endpoint_id: str) -> Any:
        try:
            before = registry.scrape_target(endpoint_id)
        except RegistryError as exc:
            return _error(exc.status, exc.code, exc.message)

        fetched = _fetch(http, before["address"], timeout=timeout, max_bytes=max_bytes)
        if fetched.error_code:
            return _error(fetched.status, fetched.error_code, fetched.message)

        expected = {
            RUN_ID_HEADER: encode_run_id_header(before["run_id"]),
            EPOCH_HEADER: str(before["lifecycle_epoch"]),
            INCARNATION_HEADER: before["incarnation"],
        }
        for header, value in expected.items():
            if fetched.headers.get(header) != value:
                return _error(
                    409,
                    IDENTITY_HEADER_MISMATCH,
                    f"the scraped response does not carry the current identity "
                    f"({header})",
                )

        # The lease or revision may have moved while the response was in
        # flight; that body must not be stored as this run's data.
        try:
            after = registry.scrape_target(endpoint_id)
        except RegistryError as exc:
            return _error(exc.status, exc.code, exc.message)
        if (
            after["lease_id"] != before["lease_id"]
            or after["revision"] != before["revision"]
        ):
            return _error(
                409,
                LEASE_CHANGED,
                "the endpoint lease changed while the scrape was in flight",
            )

        registry.note_scrape(endpoint_id, after["incarnation"])
        return Response(
            content=fetched.body,
            media_type=fetched.content_type or "text/plain",
        )

    return router


class _Fetched:
    """One bounded scrape response."""

    def __init__(
        self,
        *,
        body: bytes = b"",
        headers: Any = None,
        content_type: str = "",
        error_code: str = "",
        message: str = "",
        status: int = 502,
    ) -> None:
        self.body = body
        self.headers = headers or {}
        self.content_type = content_type
        self.error_code = error_code
        self.message = message
        self.status = status


def _fetch(http: Any, url: str, *, timeout: float, max_bytes: int) -> _Fetched:
    """Fetch one response completely, bounded in bytes and in time."""
    try:
        response = http.get(url, timeout=timeout, stream=True)
    except requests.RequestException as exc:
        return _Fetched(
            error_code=SCRAPE_BACKEND_UNAVAILABLE,
            message=str(exc),
            status=502,
        )
    try:
        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_content(CHUNK_BYTES):
            total += len(chunk)
            if total > max_bytes:
                return _Fetched(
                    error_code=SCRAPE_RESPONSE_TOO_LARGE,
                    message=f"the endpoint returned more than {max_bytes} bytes",
                    status=502,
                )
            chunks.append(chunk)
        return _Fetched(
            body=b"".join(chunks),
            headers=response.headers,
            content_type=response.headers.get("content-type", ""),
        )
    finally:
        response.close()


__all__ = ["create_scrape_router"]
