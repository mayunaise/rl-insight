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

"""Run-scoped HTTP API for the managed prototype (S0 §5, S1 §3, S2 §3–§5).

The legacy experiment endpoints stay untouched; these routes are additive and
speak the identity protocol the new SDK uses. Errors always carry the
``code,message,retryable,request_id,details`` shape from S0 §5, and the OTLP
ingress answers with the protobuf bodies the OTLP/HTTP spec defines.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Body, Request, status
from fastapi.responses import JSONResponse, Response
from google.rpc import code_pb2  # type: ignore[import-untyped]
from google.rpc.status_pb2 import Status  # type: ignore[import-untyped]
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceResponse,
)

from ..archive.ingress import (
    PROTOBUF_CONTENT_TYPE,
    RETRYABLE_CODES,
    ingest_trace_request,
)
from ..archive.records import TRACES
from ..archive.writer import ArchiveStore
from ..sdk.config import NOT_READY, READY, UNSUPPORTED
from ..sdk.identity import RunIdentity
from .run_registry import RegistryError, RunRegistry

PROTOCOL_VERSION = 1
CAPABILITIES = (
    "identity_v1",
    "session_context_v1",
    "publisher_lease_v1",
    "run_query_v1",
)
MAX_OTLP_REQUEST_BYTES = 8 * 1024 * 1024
RETRY_AFTER_SECONDS = 5
REQUEST_TOO_LARGE = "REQUEST_TOO_LARGE"

# OTLP clients retry based on the HTTP status and read the gRPC code from the
# protobuf status body (S2 §5).
_GRPC_CODES = {
    "INVALID_IDENTITY": code_pb2.INVALID_ARGUMENT,
    "INVALID_CONFIG": code_pb2.INVALID_ARGUMENT,
    "UNSUPPORTED_ENCODING": code_pb2.INVALID_ARGUMENT,
    "MALFORMED_PROTOBUF": code_pb2.INVALID_ARGUMENT,
    "MIXED_IDENTITY": code_pb2.INVALID_ARGUMENT,
    REQUEST_TOO_LARGE: code_pb2.RESOURCE_EXHAUSTED,
    "SPAN_CONFLICT": code_pb2.ALREADY_EXISTS,
    "STALE_EPOCH": code_pb2.FAILED_PRECONDITION,
    "WRONG_INSTANCE": code_pb2.FAILED_PRECONDITION,
    "RUN_ARCHIVED": code_pb2.FAILED_PRECONDITION,
    "RUN_NOT_WRITABLE": code_pb2.FAILED_PRECONDITION,
    "RUN_NOT_FOUND": code_pb2.NOT_FOUND,
    "CAPACITY_EXCEEDED": code_pb2.RESOURCE_EXHAUSTED,
}


def data_dir_from_config(conf: Any) -> Path:
    """Resolve the server data dir; the Registry and archive both live under it."""
    from omegaconf import OmegaConf

    from ..utils.constants import MonitorPaths

    raw = OmegaConf.select(conf, "server.data_dir")
    return (
        Path(str(raw)).expanduser().resolve()
        if raw
        else (MonitorPaths.STATE_ROOT / "data").resolve()
    )


def registry_path_from_config(conf: Any) -> Path:
    """Resolve the Registry location under the server data dir (RFC v4.1 §3)."""
    return data_dir_from_config(conf) / "registry" / "experiments.sqlite"


def _channel(status_name: str, reason_code: str = "") -> dict[str, str]:
    return {"status": status_name, "reason_code": reason_code}


def server_readiness(
    *,
    prometheus_enabled: bool,
    tempo_enabled: bool,
    query_enabled: bool,
    archive_dir: Path,
) -> dict[str, dict[str, str]]:
    """Per-channel readiness from real component configuration (S2 §2).

    Nothing is marked ready merely because a run is active: a disabled backend
    is ``unsupported``, a missing implementation is ``not_ready`` with its
    reason, and ``archive_metrics`` stays honest about remote_write.
    """
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_ready = os.access(archive_dir, os.W_OK)
    archive_channel = (
        _channel(READY)
        if archive_ready
        else _channel(NOT_READY, "ARCHIVE_NOT_WRITABLE")
    )
    return {
        "registration": _channel(READY),
        "metrics_scrape": _channel(READY)
        if prometheus_enabled
        else _channel(UNSUPPORTED, "PROMETHEUS_DISABLED"),
        "trace_accept": archive_channel,
        "trace_online": _channel(READY)
        if tempo_enabled
        else _channel(UNSUPPORTED, "TEMPO_DISABLED"),
        "archive_metrics": _channel(NOT_READY, "REMOTE_WRITE_UNAVAILABLE"),
        "archive_traces": archive_channel,
        "query": _channel(READY)
        if query_enabled
        else _channel(UNSUPPORTED, "QUERY_BACKEND_NOT_CONFIGURED"),
        "export": _channel(NOT_READY, "EXPORT_REQUIRES_BARRIER"),
    }


def _error(
    status_code: int, code: str, message: str, *, retryable: bool = False
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "code": code,
            "message": message,
            "retryable": retryable,
            "request_id": "",
            "details": {},
        },
    )


def _guard(action: Callable[[], Any]) -> Any:
    try:
        return action()
    except RegistryError as exc:
        return _error(exc.status, exc.code, exc.message, retryable=exc.retryable)


def _otlp_error(status_code: int, code: str, message: str) -> Response:
    """OTLP/HTTP error: HTTP status plus a protobuf ``google.rpc.Status`` body."""
    payload = Status(
        code=_GRPC_CODES.get(code, code_pb2.UNKNOWN), message=f"{code}: {message}"
    )
    headers = {"X-RLInsight-Reason": code}
    if code in RETRYABLE_CODES or status_code in (413, 429, 503):
        headers["Retry-After"] = str(RETRY_AFTER_SECONDS)
    return Response(
        content=payload.SerializeToString(),
        status_code=status_code,
        media_type=PROTOBUF_CONTENT_TYPE,
        headers=headers,
    )


def _run_detail(
    registry: RunRegistry,
    store: ArchiveStore,
    run_key: str,
    *,
    readiness: Mapping[str, Mapping[str, str]],
    view_service: Any,
) -> dict[str, Any]:
    """Run payload plus the per-channel health snapshot and the view links."""
    payload = registry.get_run(run_key)
    health = registry.run_health(run_key)
    coverage = store.coverage(RunIdentity.from_dict(payload["identity"]), TRACES)
    channels = dict(health["channels"])
    channels["archive_metrics"] = _channel(NOT_READY, "REMOTE_WRITE_UNAVAILABLE")
    channels["archive_traces"] = (
        _channel(READY)
        if coverage["record_count"]
        else _channel(NOT_READY, "NO_ARCHIVED_TRACES")
    )
    channels["query"] = dict(readiness["query"])
    channels["export"] = _channel(NOT_READY, "EXPORT_REQUIRES_BARRIER")
    health["channels"] = channels
    payload["health"] = health
    payload["view"] = (
        view_service.ensure_run_view(run_key)
        if view_service is not None
        else {"status": "not_ready", "reason_code": "VIEW_SERVICE_UNAVAILABLE"}
    )
    return payload


def create_run_router(
    registry: RunRegistry,
    store: ArchiveStore,
    *,
    readiness: Mapping[str, Mapping[str, str]],
    view_service: Any = None,
    on_registry_change: Callable[[], None] | None = None,
    max_request_bytes: int = MAX_OTLP_REQUEST_BYTES,
) -> APIRouter:
    """Run/endpoint/lease routes plus the OTLP/HTTP trace ingress."""
    router = APIRouter()
    channels = {name: dict(value) for name, value in readiness.items()}

    @router.post("/runs", status_code=status.HTTP_201_CREATED)
    def create_run(request: Request, payload: dict[str, Any] = Body(...)) -> Any:
        return _guard(
            lambda: registry.create_run(
                project=str(payload.get("project") or ""),
                experiment_name=payload.get("experiment_name"),
                idempotency_key=request.headers.get("Idempotency-Key"),
            )
        )

    @router.get("/runs")
    def list_runs(
        project: str = "", state: str = "", limit: int = 0, cursor: str = ""
    ) -> Any:
        def action() -> dict[str, Any]:
            runs = registry.list_runs(
                project=project or None,
                state=state or None,
                limit=limit or None,
                cursor=cursor or None,
            )
            next_cursor = ""
            if limit and len(runs) == limit:
                last = runs[-1]
                next_cursor = f"{last['created_at']}:{last['identity']['id']}"
            return {"status": "ok", "runs": runs, "next_cursor": next_cursor}

        return _guard(action)

    @router.get("/runs/{run_key}")
    def get_run(run_key: str) -> Any:
        return _guard(
            lambda: _run_detail(
                registry,
                store,
                run_key,
                readiness=channels,
                view_service=view_service,
            )
        )

    @router.post("/runs/{run_key}/resume")
    def resume_run(run_key: str, payload: dict[str, Any] = Body(...)) -> Any:
        return _guard(
            lambda: registry.resume_run(
                run_key,
                project=str(payload.get("project") or ""),
                expected_revision=int(payload.get("expected_revision") or 0),
            )
        )

    @router.put("/runs/{run_key}/endpoints/{slot_key}")
    def upsert_endpoint(
        run_key: str, slot_key: str, payload: dict[str, Any] = Body(...)
    ) -> Any:
        try:
            result = registry.upsert_endpoint(run_key, slot_key, payload)
        except RegistryError as exc:
            return _error(exc.status, exc.code, exc.message, retryable=exc.retryable)
        # A registration change wakes the discovery coordinator instead of
        # waiting for its periodic tick (S2 §4.1).
        if on_registry_change is not None:
            on_registry_change()
        return result

    @router.post("/runs/{run_key}/leases/{lease_id}/renew")
    def renew_lease(
        run_key: str, lease_id: str, payload: dict[str, Any] = Body(...)
    ) -> Any:
        return _guard(lambda: registry.renew_lease(run_key, lease_id, payload))

    @router.delete("/runs/{run_key}/leases/{lease_id}")
    def delete_lease(
        run_key: str, lease_id: str, payload: dict[str, Any] = Body(...)
    ) -> Any:
        try:
            result = registry.delete_lease(run_key, lease_id, payload)
        except RegistryError as exc:
            return _error(exc.status, exc.code, exc.message, retryable=exc.retryable)
        if on_registry_change is not None:
            on_registry_change()
        return result

    @router.post("/otlp/v1/traces")
    async def otlp_traces(request: Request) -> Any:
        """Archive an OTLP/HTTP span batch durably, then acknowledge it."""
        body = await request.body()
        if len(body) > max_request_bytes:
            return _otlp_error(
                413,
                REQUEST_TOO_LARGE,
                f"the batch exceeds the {max_request_bytes} byte ingest budget",
            )
        outcome = ingest_trace_request(
            body=body,
            content_type=request.headers.get("content-type", ""),
            headers=dict(request.headers),
            registry=registry,
            store=store,
            server_instance_id=registry.server_instance_id,
        )
        if outcome.accepted:
            return Response(
                content=ExportTraceServiceResponse().SerializeToString(),
                media_type=PROTOBUF_CONTENT_TYPE,
            )
        return _otlp_error(outcome.status, outcome.code, outcome.message)

    return router


def services_payload(
    *,
    server_instance_id: str,
    legacy: Mapping[str, Any],
    readiness: Mapping[str, Mapping[str, str]],
    otlp_path: str = "/api/v1/otlp/v1/traces",
) -> dict[str, Any]:
    """Discovery payload: legacy port fields plus version/capability/readiness."""
    return {
        "status": "ok",
        "protocol_version": PROTOCOL_VERSION,
        "server_instance_id": server_instance_id,
        "capabilities": list(CAPABILITIES),
        "readiness": {name: dict(value) for name, value in readiness.items()},
        "services": {"otlp_http": otlp_path},
        **dict(legacy),
    }
