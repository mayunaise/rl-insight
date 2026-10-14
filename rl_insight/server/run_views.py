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

"""Bind existing Grafana templates to a Run, preserving panel definitions."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from threading import Lock
from urllib.parse import urlencode, urlsplit

import requests
from fastapi import Request
from fastapi.responses import RedirectResponse
from omegaconf import OmegaConf

from ..protocol import RunContext, RunError
from ..utils.constants import MonitorPaths
from .network import format_host_port, local_addresses

DEFAULT_TEMPLATE = "verl/verl_tainer_v1_with_vllm_engine.json"
logger = logging.getLogger(__name__)


def run_dashboard(
    context: RunContext,
    identifiers: dict[str, str],
    template: Path | None = None,
    runs: list[RunContext] | None = None,
):
    dashboard = json.loads(
        (template or MonitorPaths.GRAFANA_DASHBOARDS_DIR / DEFAULT_TEMPLATE).read_text(
            encoding="utf-8"
        )
    )
    dashboard["metadata"] = {
        "name": dashboard["metadata"]["name"],
    }
    spec = dashboard["spec"]
    if runs is not None:
        choices = [
            {
                "text": run.run_id,
                "value": run.run_id,
                "prometheus": "rli-" + run.run_id + "-p",
                "tempo": "rli-" + run.run_id + "-t",
            }
            for run in runs
        ]
        options = [
            {
                "text": item["text"],
                "value": item["value"],
                "properties": {
                    "prometheus": item["prometheus"],
                    "tempo": item["tempo"],
                },
            }
            for item in choices
        ]
        current = next(item for item in options if item["value"] == context.run_id)
        spec["variables"].insert(
            0,
            {
                "kind": "CustomVariable",
                "spec": {
                    "name": "run_id",
                    "label": "Run ID",
                    "query": json.dumps(choices, ensure_ascii=False),
                    "valuesFormat": "json",
                    "current": current,
                    "options": options,
                    "multi": False,
                    "includeAll": False,
                    "allowCustomValue": False,
                    "hide": "dontHide",
                    "skipUrlSync": False,
                },
            },
        )

    def bind(value):
        if isinstance(value, dict):
            if value.get("kind") == "DataQuery":
                backend = value.get("group")
                if backend == "grafana":
                    return  # Built-in dashboard annotations, not training data.
                if backend not in identifiers:
                    raise RunError("Unsupported dashboard data source", 422)
                value["datasource"] = {
                    "name": "${run_id." + backend + "}"
                    if runs is not None
                    else identifiers[backend]
                }
            for child in value.values():
                bind(child)
        elif isinstance(value, list):
            for child in value:
                bind(child)

    bind(spec)
    for variable in spec.get("variables", []):
        value = variable["spec"]
        if variable["kind"] == "DatasourceVariable":
            uid = identifiers[value["pluginId"]]
            if runs is not None:
                value.update(
                    regex="^rli-${run_id}-" + value["pluginId"][0] + "$",
                    current={"text": uid, "value": uid},
                    allowCustomValue=False,
                )
            else:
                variable.update(
                    kind="ConstantVariable",
                    spec={
                        "name": value["name"],
                        "query": uid,
                        "current": {"text": uid, "value": uid},
                        "hide": "hideVariable",
                        "skipUrlSync": True,
                    },
                )
        elif runs is None and value["name"] in {"project", "experiment_name"}:
            fixed = getattr(context, value["name"])
            variable.update(
                kind="ConstantVariable",
                spec={
                    "name": value["name"],
                    "query": fixed,
                    "current": {"text": fixed, "value": fixed},
                    "hide": "dontHide",
                    "skipUrlSync": True,
                },
            )
    return dashboard


def install_views(app, registry, conf):
    lock = Lock()
    root = Path(
        str(
            OmegaConf.select(
                conf,
                "grafana.dashboards_dir",
                default=MonitorPaths.GRAFANA_DASHBOARDS_DIR,
            )
        )
    ).resolve()
    grafana = "http://" + format_host_port(
        local_addresses()["loopback"],
        int(OmegaConf.select(conf, "grafana.port", default=3000)),
    )

    def prepare(run_id, base_url, template):
        with lock:
            return update(run_id, base_url, template)

    def update(run_id, base_url, template):
        context = registry.get(run_id)
        if not bool(OmegaConf.select(conf, "grafana.enable", default=True)):
            raise RunError("Grafana disabled", 503)
        file = (root / template).resolve()
        if (
            not file.is_relative_to(root)
            or file.suffix != ".json"
            or not file.is_file()
        ):
            raise RunError("Unknown dashboard template", 404)
        base = str(OmegaConf.select(conf, "server.public_url") or base_url).rstrip("/")
        identifiers = {
            kind: "rli-" + run_id + "-" + kind[0] for kind in ("prometheus", "tempo")
        }
        runs = registry.list_runs()
        dashboard = run_dashboard(context, identifiers, file, runs)
        staged = json.dumps(dashboard, ensure_ascii=False, indent=2)
        with requests.Session() as session:
            session.trust_env = False
            token = OmegaConf.select(conf, "grafana.api_token", default="")
            if token:
                session.headers["Authorization"] = "Bearer " + str(token)
            username = OmegaConf.select(conf, "grafana.username", default="")
            if username:
                session.auth = (
                    str(username),
                    str(OmegaConf.select(conf, "grafana.password", default="")),
                )

            def call(method, path, **kwargs):
                try:
                    response = session.request(
                        method,
                        grafana + path,
                        timeout=(3, 5),
                        allow_redirects=False,
                        **kwargs,
                    )
                except requests.RequestException as exc:
                    raise RunError("Grafana request failed", 502) from exc
                if response.status_code == 404 and method == "GET":
                    return response
                if not 200 <= response.status_code < 300:
                    raise RunError("Grafana provisioning failed", 502)
                return response

            # Provision each selectable Run once; its backend URL remains Run-scoped.
            for run in runs:
                for kind in identifiers:
                    uid = "rli-" + run.run_id + "-" + kind[0]
                    current = call("GET", "/api/datasources/uid/" + uid)
                    payload = {
                        "uid": uid,
                        "name": uid,
                        "type": kind,
                        "access": "proxy",
                        "url": base + "/api/v1/runs/" + run.run_id + "/" + kind,
                        "editable": False,
                        "jsonData": {"httpMethod": "GET"},
                    }
                    if current.status_code == 404:
                        call("POST", "/api/datasources", json=payload)
                    elif current.json().get("url") != payload["url"]:
                        call("PUT", "/api/datasources/uid/" + uid, json=payload)
            path = "/apis/dashboard.grafana.app/v2/namespaces/default/dashboards"
            uid = dashboard["metadata"]["name"]
            current = call("GET", path + "/" + uid)
            if current.status_code == 404:
                raise RunError("Original dashboard is not provisioned in Grafana", 503)
            # Keep its folder, permissions and provisioning ownership.
            dashboard["metadata"] = current.json()["metadata"]
            call("PUT", path + "/" + uid, json=dashboard)
        runtime = OmegaConf.select(conf, "server.runtime_dir")
        if runtime:
            folder = (Path(str(runtime)) / "dashboards").resolve()
            target = (folder / template).resolve()
            if target.is_relative_to(folder) and target.is_file():
                temporary = target.with_suffix(".json.tmp")
                temporary.write_text(staged, encoding="utf-8")
                temporary.replace(target)
        address = urlsplit(base_url)
        public = str(
            OmegaConf.select(conf, "grafana.public_url")
            or (
                address.scheme
                + "://"
                + format_host_port(address.hostname, int(conf.grafana.port))
            )
        )
        selection = urlencode(
            {"var-run_id": run_id, "var-datasource": identifiers["prometheus"]}
        )
        return {
            "url": public.rstrip("/") + "/d/" + uid + "?" + selection,
            "datasources": identifiers,
        }

    def refresh(base_url=None):
        runs = registry.list_runs()
        if runs and bool(OmegaConf.select(conf, "grafana.enable", default=True)):
            try:
                prepare(
                    runs[0].run_id, base_url or runs[0].server_url, DEFAULT_TEMPLATE
                )
            except (RunError, OSError) as exc:
                logger.warning(
                    "[rl-insight] Could not refresh the original dashboard: %s", exc
                )

    @app.get("/api/v1/runs/{run_id}/view")
    def view(run_id: str, request: Request, template: str = DEFAULT_TEMPLATE):
        return prepare(run_id, str(request.base_url), template)

    @app.get("/ui/runs/{run_id}")
    def page(run_id: str, request: Request, template: str = DEFAULT_TEMPLATE):
        return RedirectResponse(prepare(run_id, str(request.base_url), template)["url"])

    return refresh
