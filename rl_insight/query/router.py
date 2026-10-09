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

"""Run-scoped Prometheus/Tempo compatibility routes (S2 §6).

The caller mounts this router under the public API prefix, so the paths here are
``/runs/{run_key}/prometheus/api/v1/...`` and ``/runs/{run_key}/tempo/api/...``.
The path identity is the only identity: the run key is resolved through the
Registry, the backend address comes from server config, and anything that would
let a client pick its own run or backend is refused. Only the endpoints in the
S2 §6 table are open; everything else answers 404 instead of turning this into a
wildcard reverse proxy in front of the shared backends.
"""

from __future__ import annotations

import re
import time
import urllib.parse
from typing import Any

import requests
from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response

from ..sdk.identity import METRIC_LABEL, TRACE_ATTRIBUTE
from ..server.run_registry import RegistryError
from .errors import QueryError
from .promql import enforce_match_selector, enforce_selector, identity_selector
from .tempo import enforce_traceql, trace_ownership

UNSUPPORTED_ENDPOINT = "UNSUPPORTED_ENDPOINT"
UNSCOPED_TEMPO_ENDPOINT = "UNSCOPED_TEMPO_ENDPOINT"

# S2 §6.3: a Tempo search answer is forwarded only after every candidate was
# fetched and its full Resource ownership was checked, because one trace may
# carry spans of several runs. Both bounds are explicit outer limits: reaching
# one is reported, never presented as a shorter but complete result.
SEARCH_VERIFY_LIMIT = 20
SEARCH_VERIFY_BUDGET_SECONDS = 5.0

_ROUTE_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]

# Never forwarded to the backend: hop-by-hop headers, the client's own
# credentials, the backend routing headers the shared deployments use, and the
# internal identity headers.
_DENIED_HEADERS = frozenset(
    {
        "authorization",
        "connection",
        "content-length",
        "content-type",
        "host",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "x-scope-orgid",
    }
)
_DENIED_HEADER_PREFIXES = ("x-prometheus-", "x-tempo-", "x-rlinsight-")

# The path identity is authoritative; an identity override in the request is
# refused rather than ignored.
_IDENTITY_HEADERS = (
    "x-rlinsight-run-id",
    "x-rlinsight-epoch",
    "x-rlinsight-incarnation",
    "x-rlinsight-server-instance",
)
# The backend is fixed by server config; a client-chosen upstream is refused.
_UPSTREAM_PARAMS = ("upstream", "url")

_TRACE_ID = re.compile(r"^[0-9a-fA-F]{16,64}$")
_LABEL_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TAG_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")

Params = dict[str, list[str]]


async def _client_params(request: Request) -> Params:
    """Every client-supplied parameter, from the query string and a form body."""
    params: Params = {}
    for key, value in urllib.parse.parse_qsl(request.url.query, keep_blank_values=True):
        params.setdefault(key, []).append(value)
    body = await request.body()
    if not body:
        return params
    content_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if content_type != "application/x-www-form-urlencoded":
        raise QueryError(
            "UNSUPPORTED_CONTENT_TYPE",
            f"a {content_type or 'body-less'} request body is not accepted here",
            status=400,
        )
    try:
        fields = urllib.parse.parse_qsl(body.decode("utf-8"), keep_blank_values=True)
    except UnicodeDecodeError as exc:
        raise QueryError(
            "INVALID_REQUEST_BODY", f"the form body is not UTF-8: {exc}", status=400
        ) from exc
    for key, value in fields:
        params.setdefault(key, []).append(value)
    return params


def create_query_router(
    registry: Any,
    *,
    prometheus_url: str,
    tempo_url: str,
    timeout: float = 15.0,
    max_response_bytes: int = 8 * 1024 * 1024,
    session: Any = None,
) -> APIRouter:
    """Build the run-scoped Prometheus/Tempo entry points.

    ``registry`` only has to answer ``get_run(run_key)`` and raise
    ``RegistryError`` for an unknown key; the backend session defaults to a
    plain ``requests.Session`` and is injected in tests.
    """
    backend = session if session is not None else requests.Session()
    prometheus_base = prometheus_url.rstrip("/")
    tempo_base = tempo_url.rstrip("/")
    router = APIRouter()

    def resolve(run_key: str) -> tuple[str, str, str]:
        """The (run id, metric label, trace attribute) of one run key."""
        try:
            run = registry.get_run(run_key)
        except RegistryError as exc:
            raise QueryError(exc.code, exc.message, status=exc.status) from exc
        identity = run["identity"]
        source = identity["source"]
        return identity["id"], METRIC_LABEL[source], TRACE_ATTRIBUTE[source]

    def guard(request: Request, params: Params) -> None:
        """Refuse a request that tries to choose its own identity or backend."""
        for name in _IDENTITY_HEADERS:
            if name in request.headers:
                raise QueryError(
                    "IDENTITY_OVERRIDE_REJECTED",
                    f"the {name} header is not accepted; the run key in the path "
                    "is the only identity",
                    status=400,
                )
        for name in _UPSTREAM_PARAMS:
            if name in params:
                raise QueryError(
                    "CLIENT_UPSTREAM_REJECTED",
                    f"the {name} parameter is not accepted; the backend address "
                    "comes from server config",
                    status=400,
                )

    def forwarded_headers(request: Request) -> dict[str, str]:
        return {
            name: value
            for name, value in request.headers.items()
            if name.lower() not in _DENIED_HEADERS
            and not name.lower().startswith(_DENIED_HEADER_PREFIXES)
        }

    def fetch(
        url: str, params: Params, request: Request, budget: float | None = None
    ) -> Any:
        # ``budget`` is the caller's remaining time for a bounded loop; the
        # request may never outlive it, or the outer limit would only be
        # advisory.
        request_timeout = timeout if budget is None else min(timeout, max(0.0, budget))
        try:
            response = backend.request(
                "GET",
                url,
                params=params,
                headers=forwarded_headers(request),
                timeout=request_timeout,
            )
        except requests.Timeout as exc:
            raise QueryError(
                "BACKEND_TIMEOUT",
                f"the backend did not answer within {timeout}s",
                status=504,
            ) from exc
        except requests.RequestException as exc:
            raise QueryError(
                "BACKEND_UNAVAILABLE", f"the backend call failed: {exc}", status=502
            ) from exc
        if len(response.content) > max_response_bytes:
            raise QueryError(
                "RESPONSE_TOO_LARGE",
                f"the backend response exceeds {max_response_bytes} bytes",
                status=502,
            )
        return response

    def passthrough(response: Any) -> Response:
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers={
                "Content-Type": response.headers.get("Content-Type", "application/json")
            },
        )

    def prometheus_error(error: QueryError) -> JSONResponse:
        return JSONResponse(
            status_code=error.status,
            content={
                "status": "error",
                "errorType": error.code,
                "error": error.message,
                "reason_code": error.code,
            },
        )

    def tempo_error(error: QueryError) -> JSONResponse:
        return JSONResponse(status_code=error.status, content=error.to_dict())

    def verified_search(
        response: Any, request: Request, run_id: str, attribute: str
    ) -> Response:
        """Forward a search answer for the candidates that are this run's alone.

        The backend's own filtering is not proof: a trace can carry spans of
        several runs, so the titles, root services, durations and tags of the
        candidates would otherwise describe another run's data. Every candidate
        is fetched and checked; a candidate that is not this run's is left out,
        a candidate mixing runs is a conflict, and exhausting the candidate or
        time budget is reported instead of shortening the result silently.
        """
        try:
            payload = response.json()
        except ValueError as exc:
            raise QueryError(
                "BACKEND_RESPONSE_INVALID",
                f"the search response is not JSON: {exc}",
                status=502,
            ) from exc
        if not isinstance(payload, dict):
            raise QueryError(
                "BACKEND_RESPONSE_INVALID",
                "the search response is not a JSON object",
                status=502,
            )
        traces = payload.get("traces")
        if not isinstance(traces, list):
            raise QueryError(
                "BACKEND_RESPONSE_INVALID",
                "the search response carries no trace list",
                status=502,
            )
        if len(traces) > SEARCH_VERIFY_LIMIT:
            raise QueryError(
                "SEARCH_VERIFY_BUDGET_EXCEEDED",
                f"the search returned {len(traces)} candidates; at most "
                f"{SEARCH_VERIFY_LIMIT} can be verified",
                status=502,
            )
        deadline = time.monotonic() + SEARCH_VERIFY_BUDGET_SECONDS
        owned = []
        for candidate in traces:
            trace_id = candidate.get("traceID") if isinstance(candidate, dict) else None
            if not isinstance(trace_id, str) or not _TRACE_ID.match(trace_id):
                raise QueryError(
                    "BACKEND_RESPONSE_INVALID",
                    "a search candidate carries no trace ID",
                    status=502,
                )
            budget = deadline - time.monotonic()
            if budget <= 0:
                raise QueryError(
                    "SEARCH_VERIFY_TIMEOUT",
                    "verifying the search candidates took longer than "
                    f"{SEARCH_VERIFY_BUDGET_SECONDS}s",
                    status=504,
                )
            # The fetch is capped by the remaining budget, and the budget is
            # re-checked after the answer is parsed: a candidate that consumed
            # the last of it may not be reported as a complete result.
            answer = fetch(
                f"{tempo_base}/api/traces/{trace_id}", {}, request, budget=budget
            )
            if answer.status_code == 200:
                try:
                    trace = answer.json()
                except ValueError as exc:
                    raise QueryError(
                        "BACKEND_RESPONSE_INVALID",
                        f"the trace response is not JSON: {exc}",
                        status=502,
                    ) from exc
                ownership = trace_ownership(trace, attribute, run_id)
                if ownership["mixed"]:
                    raise QueryError(
                        "TRACE_ID_CONFLICT",
                        f"trace {trace_id!r} mixes spans from more than one run",
                        status=409,
                    )
                if ownership["owned"]:
                    owned.append(candidate)
            elif answer.status_code != 404:
                raise QueryError(
                    "BACKEND_ERROR",
                    f"the trace backend answered {answer.status_code}",
                    status=answer.status_code,
                )
            if time.monotonic() > deadline:
                raise QueryError(
                    "SEARCH_VERIFY_TIMEOUT",
                    "verifying the search candidates took longer than "
                    f"{SEARCH_VERIFY_BUDGET_SECONDS}s",
                    status=504,
                )
        return JSONResponse(
            status_code=response.status_code, content=dict(payload, traces=owned)
        )

    def prometheus_forward(
        run_key: str, request: Request, params: Params, upstream: str, scoped: str
    ) -> Response:
        run_id, label, _ = resolve(run_key)
        guard(request, params)
        if scoped == "query":
            expression = _last(params, "query")
            if not expression.strip():
                raise QueryError(
                    "MISSING_QUERY", "the query parameter is required", status=400
                )
            params["query"] = [enforce_selector(expression, run_id, label)]
        else:
            matches = params.get("match[]") or []
            params["match[]"] = [
                enforce_match_selector(match, run_id, label) for match in matches
            ] or [identity_selector(run_id, label)]
        return passthrough(fetch(f"{prometheus_base}{upstream}", params, request))

    async def prometheus_entry(
        run_key: str, request: Request, upstream: str, scoped: str
    ) -> Response:
        try:
            params = await _client_params(request)
            return await run_in_threadpool(
                prometheus_forward, run_key, request, params, upstream, scoped
            )
        except QueryError as error:
            return prometheus_error(error)

    def tempo_forward(
        run_key: str, request: Request, params: Params, upstream: str, kind: str
    ) -> Response:
        run_id, _, attribute = resolve(run_key)
        guard(request, params)
        if kind in ("search", "tag_values"):
            query = _last(params, "q")
            if kind == "search" and not query.strip():
                raise QueryError(
                    UNSCOPED_TEMPO_ENDPOINT,
                    "the tag-only Tempo search cannot be narrowed to one run, so "
                    "it is not passed through",
                    status=422,
                )
            params["q"] = [enforce_traceql(query, attribute, run_id)]
            response = fetch(f"{tempo_base}{upstream}", params, request)
            if kind == "search":
                return verified_search(response, request, run_id, attribute)
            return passthrough(response)

        response = fetch(f"{tempo_base}{upstream}", params, request)
        if kind != "trace":
            return passthrough(response)
        trace_id = upstream.rsplit("/", 1)[-1]
        if response.status_code == 404:
            raise QueryError(
                "TRACE_NOT_FOUND",
                f"trace {trace_id!r} is not known to the backend",
                status=404,
            )
        if response.status_code != 200:
            raise QueryError(
                "BACKEND_ERROR",
                f"the trace backend answered {response.status_code}",
                status=response.status_code,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise QueryError(
                "BACKEND_RESPONSE_INVALID",
                f"the trace response is not JSON: {exc}",
                status=502,
            ) from exc
        ownership = trace_ownership(payload, attribute, run_id)
        if ownership["mixed"]:
            raise QueryError(
                "TRACE_ID_CONFLICT",
                f"trace {trace_id!r} mixes spans from more than one run",
                status=409,
            )
        if not ownership["owned"]:
            raise QueryError(
                "TRACE_NOT_FOUND",
                f"trace {trace_id!r} has no span owned by this run",
                status=404,
            )
        return passthrough(response)

    async def tempo_entry(
        run_key: str, request: Request, upstream: str, kind: str
    ) -> Response:
        try:
            params = await _client_params(request)
            return await run_in_threadpool(
                tempo_forward, run_key, request, params, upstream, kind
            )
        except QueryError as error:
            return tempo_error(error)

    def unscoped(interface: str) -> JSONResponse:
        return tempo_error(
            QueryError(
                UNSCOPED_TEMPO_ENDPOINT,
                f"{interface} cannot be narrowed to one run by the frozen Tempo "
                "version, so it is not passed through",
                status=422,
            )
        )

    # -- Prometheus (S2 §6.1 table) ------------------------------------
    @router.get("/runs/{run_key}/prometheus/api/v1/query")
    @router.post("/runs/{run_key}/prometheus/api/v1/query")
    async def prometheus_query(run_key: str, request: Request) -> Response:
        return await prometheus_entry(run_key, request, "/api/v1/query", "query")

    @router.get("/runs/{run_key}/prometheus/api/v1/query_range")
    @router.post("/runs/{run_key}/prometheus/api/v1/query_range")
    async def prometheus_query_range(run_key: str, request: Request) -> Response:
        return await prometheus_entry(run_key, request, "/api/v1/query_range", "query")

    @router.get("/runs/{run_key}/prometheus/api/v1/series")
    @router.post("/runs/{run_key}/prometheus/api/v1/series")
    async def prometheus_series(run_key: str, request: Request) -> Response:
        return await prometheus_entry(run_key, request, "/api/v1/series", "match[]")

    @router.get("/runs/{run_key}/prometheus/api/v1/labels")
    @router.post("/runs/{run_key}/prometheus/api/v1/labels")
    async def prometheus_labels(run_key: str, request: Request) -> Response:
        return await prometheus_entry(run_key, request, "/api/v1/labels", "match[]")

    @router.get("/runs/{run_key}/prometheus/api/v1/label/{name}/values")
    @router.post("/runs/{run_key}/prometheus/api/v1/label/{name}/values")
    async def prometheus_label_values(
        run_key: str, name: str, request: Request
    ) -> Response:
        if not _LABEL_NAME.match(name):
            return prometheus_error(
                QueryError(
                    "INVALID_LABEL_NAME",
                    f"{name!r} is not a Prometheus label name",
                    status=400,
                )
            )
        return await prometheus_entry(
            run_key, request, f"/api/v1/label/{name}/values", "match[]"
        )

    # -- Tempo (S2 §6.1/§6.3 table) ------------------------------------
    @router.get("/runs/{run_key}/tempo/api/search")
    async def tempo_search(run_key: str, request: Request) -> Response:
        # The tag-based form of this endpoint has no TraceQL spanset to
        # constrain, so only its TraceQL form is open; a TraceQL answer is
        # forwarded only after its candidates were checked (S2 §6.3).
        return await tempo_entry(run_key, request, "/api/search", "search")

    @router.get("/runs/{run_key}/tempo/api/v2/search/tag/{tag}/values")
    async def tempo_tag_values(run_key: str, tag: str, request: Request) -> Response:
        if not _TAG_NAME.match(tag):
            return tempo_error(
                QueryError(
                    "INVALID_TAG_NAME", f"{tag!r} is not a Tempo tag name", status=400
                )
            )
        return await tempo_entry(
            run_key, request, f"/api/v2/search/tag/{tag}/values", "tag_values"
        )

    @router.get("/runs/{run_key}/tempo/api/traces/{trace_id}")
    async def tempo_trace(run_key: str, trace_id: str, request: Request) -> Response:
        if not _TRACE_ID.match(trace_id):
            return tempo_error(
                QueryError(
                    "INVALID_TRACE_ID",
                    f"{trace_id!r} is not a hexadecimal trace id",
                    status=400,
                )
            )
        return await tempo_entry(run_key, request, f"/api/traces/{trace_id}", "trace")

    @router.get("/runs/{run_key}/tempo/api/echo")
    async def tempo_echo(run_key: str, request: Request) -> Response:
        return await tempo_entry(run_key, request, "/api/echo", "plain")

    # Tag names are a global Tempo interface with no query to constrain.
    @router.get("/runs/{run_key}/tempo/api/search/tags")
    @router.get("/runs/{run_key}/tempo/api/v2/search/tags")
    async def tempo_unscoped_metadata() -> Response:
        return unscoped("the global Tempo tag interface")

    # -- everything else is explicitly closed ---------------------------
    @router.api_route(
        "/runs/{run_key}/prometheus/{rest:path}",
        methods=_ROUTE_METHODS,
        include_in_schema=False,
    )
    async def prometheus_unsupported(run_key: str, rest: str) -> JSONResponse:
        return prometheus_error(
            QueryError(
                UNSUPPORTED_ENDPOINT,
                f"the Prometheus endpoint {rest!r} is not open on the run-scoped router",
                status=404,
            )
        )

    @router.api_route(
        "/runs/{run_key}/tempo/{rest:path}",
        methods=_ROUTE_METHODS,
        include_in_schema=False,
    )
    async def tempo_unsupported(run_key: str, rest: str) -> JSONResponse:
        return tempo_error(
            QueryError(
                UNSUPPORTED_ENDPOINT,
                f"the Tempo endpoint {rest!r} is not open on the run-scoped router",
                status=404,
            )
        )

    return router


def _last(params: Params, key: str) -> str:
    values = params.get(key) or [""]
    return values[-1]
