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

"""Light run pages and the run view API (S2 §7).

Plain HTML strings and vanilla JS only: a run list with identity, lifecycle and
per-channel health, a single-run page that reuses the existing Grafana panels,
and a two-run compare page. Panels are always run-scoped; nothing aggregates or
does arithmetic across runs, and every dynamic value is HTML-escaped.
"""

from __future__ import annotations

import html
import time
from typing import Any

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse

from .grafana import RunViewService
from .run_registry import RegistryError

WINDOW_MS = 3600_000


def run_health(registry, run_key: str) -> dict:
    """Project one run's channel health for the pages."""
    return registry.run_health(run_key)


def create_view_router(registry, view_service: RunViewService) -> APIRouter:
    """Run view JSON plus the three UI pages, wired by the caller as one router."""
    router = APIRouter()

    @router.get("/runs/{run_key}/view")
    def run_view(
        run_key: str, from_ms: int | None = None, to_ms: int | None = None
    ) -> Any:
        try:
            return view_service.ensure_run_view(run_key, from_ms=from_ms, to_ms=to_ms)
        except RegistryError as exc:
            return JSONResponse(
                status_code=exc.status,
                content={"reason_code": exc.code, "message": exc.message},
            )

    @router.get("/ui/runs")
    def run_list(project: str = "", state: str = "") -> Any:
        runs = registry.list_runs(project=project or None, state=state or None)
        body = (
            _filter_form(project, state)
            + "<table><thead><tr><th>project</th><th>experiment</th><th>run</th>"
            "<th>state</th><th>health</th><th>open</th></tr></thead><tbody>"
            + "".join(_run_row(registry, run) for run in runs)
            + "</tbody></table>"
        )
        return HTMLResponse(_document("RL-Insight runs", body))

    @router.get("/ui/runs/{run_key}")
    def run_page(run_key: str) -> Any:
        from_ms, to_ms = _window()
        try:
            run = registry.get_run(run_key)
            view = view_service.ensure_run_view(run_key, from_ms=from_ms, to_ms=to_ms)
            health = run_health(registry, run_key)
        except RegistryError as exc:
            return HTMLResponse(_error_document(exc), status_code=exc.status)

        identity = run["identity"]
        header = (
            f"<h2>{html.escape(identity['project'])} /"
            f" {html.escape(identity['experiment_name'] or '')}</h2>"
            f"<p>run {html.escape(identity['id'][:8])} &middot; state"
            f" {html.escape(run['state'])} &middot; key"
            f" {html.escape(run_key)}</p>"
        )
        body = (
            header + f"<p><a href='/ui/runs'>all runs</a> &middot;"
            f" <a href='{html.escape(view['compare_url'], quote=True)}'>compare</a></p>"
            + "<h3>health</h3>"
            + _health_table(health)
            + "<h3 id='metrics'>metrics</h3>"
            + _panel(view, "metrics")
            + "<h3 id='traces'>traces</h3>"
            + _panel(view, "traces")
        )
        return HTMLResponse(_document("RL-Insight run", body))

    @router.get("/ui/compare")
    def compare_page(left: str = "", right: str = "") -> Any:
        # A missing ``left`` is an unknown run and takes the same error path.
        from_ms, to_ms = _window()
        try:
            views = [
                view_service.ensure_run_view(key, from_ms=from_ms, to_ms=to_ms)
                for key in [left] + ([right] if right else [])
            ]
        except RegistryError as exc:
            return HTMLResponse(_error_document(exc), status_code=exc.status)
        columns = "".join(f"<td>{_compare_column(view)}</td>" for view in views)
        return HTMLResponse(
            _document(
                "RL-Insight compare",
                f"<p><a href='/ui/runs'>all runs</a></p><table><tr>{columns}</tr></table>",
            )
        )

    return router


# -- page pieces --------------------------------------------------------
def _window() -> tuple[int, int]:
    """One shared initial time range for every panel opened in a request."""
    to_ms = int(time.time() * 1000)
    return to_ms - WINDOW_MS, to_ms


def _document(title: str, body: str) -> str:
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>{html.escape(title)}</title></head>"
        f"<body><h1>{html.escape(title)}</h1>{body}</body></html>"
    )


def _error_document(exc: RegistryError) -> str:
    return _document(
        "run unavailable",
        f"<p class='error'>{html.escape(exc.code)}: {html.escape(exc.message)}</p>",
    )


def _filter_form(project: str, state: str) -> str:
    return (
        "<form method='get' action='/ui/runs'>"
        f"project <input name='project' value='{html.escape(project, quote=True)}'>"
        f" state <input name='state' value='{html.escape(state, quote=True)}'>"
        " <button type='submit'>filter</button></form>"
    )


def _run_row(registry, run: dict) -> str:
    identity = run["identity"]
    run_key = run["run_key"]
    channels = ", ".join(
        f"{html.escape(name)}: {html.escape(channel['status'])}"
        + (
            f" ({html.escape(channel['reason_code'])})"
            if channel["reason_code"]
            else ""
        )
        for name, channel in run_health(registry, run_key)["channels"].items()
    )
    page = html.escape(f"/ui/runs/{run_key}", quote=True)
    return (
        "<tr>"
        f"<td>{html.escape(identity['project'])}</td>"
        f"<td>{html.escape(identity['experiment_name'] or '')}</td>"
        f"<td>{html.escape(identity['id'][:8])}</td>"
        f"<td>{html.escape(run['state'])}</td>"
        f"<td>{channels or '&mdash;'}</td>"
        f"<td><a href='{page}#metrics'>metrics</a>"
        f" / <a href='{page}#traces'>traces</a></td>"
        "</tr>"
    )


def _health_table(health: dict) -> str:
    rows = "".join(
        f"<tr><td>{html.escape(name)}</td><td>{html.escape(channel['status'])}</td>"
        f"<td>{html.escape(channel['reason_code'])}</td></tr>"
        for name, channel in health["channels"].items()
    )
    return (
        "<table><thead><tr><th>channel</th><th>status</th><th>reason</th></tr>"
        f"</thead><tbody>{rows}</tbody></table>"
    )


def _panel(view: dict, kind: str) -> str:
    url = view["metrics_url"] if kind == "metrics" else view["traces_url"]
    if view["status"] != "ready" or not url:
        return (
            f"<p class='error'>{html.escape(kind)} unavailable:"
            f" {html.escape(view['reason_code'])}</p>"
        )
    escaped = html.escape(url, quote=True)
    return (
        f"<iframe src='{escaped}' width='100%' height='400'></iframe>"
        f"<p><a href='{escaped}'>open {html.escape(kind)} in Grafana</a></p>"
    )


def _traces_link(view: dict) -> str:
    if view["status"] != "ready" or not view["traces_url"]:
        return (
            "<p class='error'>traces unavailable:"
            f" {html.escape(view['reason_code'])}</p>"
        )
    return f"<p><a href='{html.escape(view['traces_url'], quote=True)}'>open traces</a></p>"


def _compare_column(view: dict) -> str:
    identity = view["identity"]
    header = (
        f"<h3>{html.escape(identity['project'])} /"
        f" {html.escape(identity['experiment_name'] or '')}</h3>"
        f"<p>run {html.escape(identity['id'][:8])}</p>"
    )
    # One panel view per run: two columns, each requesting its own datasource
    # with the same initial time range.
    return header + _panel(view, "metrics") + _traces_link(view)
