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

"""HTTP API and remote helpers for the RL-Insight server."""

from __future__ import annotations

import argparse
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Sequence

import requests
import uvicorn
from fastapi import BackgroundTasks, Body, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from omegaconf import DictConfig, OmegaConf
from starlette.concurrency import run_in_threadpool

from ..protocol import RunContext, RunError
from ..utils.constants import MonitorEnv, MonitorPaths, MonitorServer, PrometheusScrape
from ..utils.monitor_config_loader import load_server_config_file
from ..utils.prometheus_utils import PrometheusTarget, PrometheusTargetStore
from .network import local_addresses
from .registry import RunRegistry

logger = logging.getLogger(__name__)


def server_url() -> str:
    """Return the configured RL-Insight server URL without a trailing slash."""
    return str(os.environ.get(MonitorEnv.SERVER_URL, "")).strip().rstrip("/")


def get_server_services() -> dict[str, Any]:
    """Fetch service endpoints from the RL-Insight server."""
    base_url = server_url()
    if not base_url:
        logger.error(
            "[rl-insight] RL-Insight server URL is required; set %s",
            MonitorEnv.SERVER_URL,
        )
        return {}

    url = f"{base_url}{MonitorServer.API_PREFIX}/services"
    last_error: requests.RequestException | ValueError | None = None
    for attempt in range(MonitorServer.SERVICE_DISCOVERY_RETRIES):
        try:
            response = requests.get(
                url,
                timeout=MonitorServer.SERVICE_DISCOVERY_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError(
                    f"services response must be an object, got {type(data).__name__}"
                )
            return data
        except (requests.RequestException, ValueError) as exc:
            last_error = exc

        if attempt + 1 < MonitorServer.SERVICE_DISCOVERY_RETRIES:
            time.sleep(MonitorServer.SERVICE_DISCOVERY_RETRY_DELAY_SECONDS)

    logger.error(
        "[rl-insight] Failed to fetch RL-Insight server services at %s: %s",
        url,
        last_error,
    )
    return {}


def create_app(conf: DictConfig) -> FastAPI:
    """Create the RL-Insight server application."""
    registry = RunRegistry(
        Path(
            str(
                OmegaConf.select(
                    conf, "server.data_dir", default=MonitorPaths.STATE_ROOT / "data"
                )
            )
        )
    )

    @asynccontextmanager
    async def lifespan(_app):
        try:
            await run_in_threadpool(refresh_views)
            yield
        finally:
            registry.close()

    app = FastAPI(title="RL-Insight server", version="0.1.0", lifespan=lifespan)
    store = PrometheusTargetStore.from_config(conf)
    from .queries import install_queries
    from .run_views import install_views

    install_queries(app, registry, conf)
    refresh_views = install_views(app, registry, conf)

    @app.exception_handler(RunError)
    async def run_error(_request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status)

    @app.post("/api/v1/runs")
    def create_run(
        request: Request,
        background: BackgroundTasks,
        payload: dict[str, Any] = Body(...),
    ):
        context = registry.create(
            payload.get("request_id", ""),
            payload.get("project", "uncategorized"),
            payload.get("experiment_name", "unnamed"),
            str(OmegaConf.select(conf, "server.public_url") or request.base_url),
            payload.get("config", {}),
        )
        background.add_task(refresh_views, str(request.base_url))
        return context.to_dict()

    @app.get("/api/v1/runs/{run_id}")
    def get_run(run_id: str):
        return registry.snapshot(run_id)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        """Return a lightweight liveness response for process checks."""
        return {"status": "ok"}

    @app.get(f"{MonitorServer.API_PREFIX}/services")
    def services() -> dict[str, Any]:
        """Expose enabled component ports for trainer-side endpoint discovery."""
        return {
            "status": "ok",
            "run_context_version": 1,
            "otlp_port": (
                int(OmegaConf.select(conf, "otel.otel_port"))
                if bool(OmegaConf.select(conf, "tempo.enable", default=True))
                else None
            ),
            "prometheus_port": (
                int(OmegaConf.select(conf, "prometheus.prometheus_port"))
                if bool(OmegaConf.select(conf, "prometheus.enable", default=True))
                else None
            ),
            "grafana_port": (
                int(OmegaConf.select(conf, "grafana.port"))
                if bool(OmegaConf.select(conf, "grafana.enable", default=True))
                else None
            ),
        }

    @app.post(f"{MonitorServer.API_PREFIX}/prometheus/targets")
    def register_prometheus_targets(
        payload: dict[str, Any] = Body(...),
    ) -> dict[str, Any]:
        """Register metric scrape targets into the runtime Prometheus config."""
        raw_targets = payload.get("targets")
        if not isinstance(raw_targets, list) or not raw_targets:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="targets must be a non-empty list",
            )

        default_labels = payload.get("labels") or {}
        if not isinstance(default_labels, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="labels must be an object",
            )

        targets: list[PrometheusTarget] = []
        # Apply request-level labels first, then let each target override them.
        for item in raw_targets:
            if isinstance(item, str):
                targets.append(
                    PrometheusTarget(target=item, labels=dict(default_labels))
                )
                continue
            if isinstance(item, dict):
                item_labels = item.get("labels") or {}
                if not isinstance(item_labels, dict):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="target labels must be an object",
                    )
                targets.append(
                    PrometheusTarget(
                        target=str(item.get("target")),
                        labels={**default_labels, **item_labels},
                    )
                )
                continue
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="each target must be either a string or an object",
            )

        job_name = str(payload.get("job_name") or PrometheusScrape.TRAINER_METRICS_JOB)
        if "run_context" in payload:
            supplied = RunContext.from_dict(payload["run_context"])
            context = registry.get(supplied.run_id)
            if context != supplied:
                raise RunError("RunContext conflicts with registered metadata", 409)
            targets = [
                PrometheusTarget(target.target, context.labels(target.labels))
                for target in targets
            ]
            if payload.get("identity_in_metrics") is True:
                # Hub samples already have run_id; avoid duplicate exported_run_id labels.
                targets = [
                    PrometheusTarget(
                        target.target,
                        {k: v for k, v in target.labels.items() if k != "run_id"},
                    )
                    for target in targets
                ]
        try:
            result = store.register(job_name, targets)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(exc),
            ) from exc

        reloaded = False
        try:
            reloaded = store.reload()
        except requests.RequestException as exc:
            logger.warning(
                "[rl-insight] Failed to reload Prometheus after target update: %s",
                exc,
            )
        return {"status": "ok", "prometheus_reloaded": reloaded, **result}

    return app


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m rl_insight.server.http_api")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Resolved server config YAML used by the RL-Insight server.",
    )
    args = parser.parse_args(argv)

    conf = load_server_config_file(args.config)
    port = int(OmegaConf.select(conf, "server.port", default=18080))
    uvicorn.run(create_app(conf), host=local_addresses()["bind"], port=port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
