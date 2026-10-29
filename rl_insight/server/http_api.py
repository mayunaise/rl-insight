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
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Sequence

import requests
import uvicorn
from fastapi import Body, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import Response
from omegaconf import DictConfig, OmegaConf

from .. import __version__
from ..utils.constants import MonitorEnv, MonitorServer, PrometheusScrape
from ..utils.monitor_config_loader import load_server_config_file
from ..utils.prometheus_utils import (
    PrometheusTarget,
    PrometheusTargetStore,
)
from ..utils.protocol import (
    SDK_VERSION_HEADER,
    InvalidVersionError,
    RunError,
    require_version,
)
from .network import local_addresses
from .dependencies import task_collector_enabled

logger = logging.getLogger(__name__)


def server_url() -> str:
    """Return the configured RL-Insight server URL without a trailing slash."""
    return str(os.environ.get(MonitorEnv.SERVER_URL, "")).strip().rstrip("/")


def get_server_services(base_url: str | None = None) -> dict[str, Any]:
    """Fetch the Server's service endpoints, retrying transient failures."""
    base_url = (base_url or server_url()).strip().rstrip("/")
    if not base_url:
        raise RunError(f"RL-Insight requires a Server URL; set {MonitorEnv.SERVER_URL}")

    url = f"{base_url}{MonitorServer.API_PREFIX}/services"
    last_error: requests.RequestException | ValueError | None = None
    for attempt in range(MonitorServer.SERVICE_DISCOVERY_RETRIES):
        try:
            response = requests.get(
                url,
                headers={SDK_VERSION_HEADER: __version__},
                timeout=MonitorServer.SERVICE_DISCOVERY_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError(
                    f"services response must be an object, got {type(data).__name__}"
                )
        except requests.HTTPError as exc:
            if (
                exc.response is not None
                and 400 <= exc.response.status_code < 500
                and exc.response.status_code not in {408, 429}
            ):
                raise RunError(
                    f"RL-Insight service discovery rejected at {url}: {exc.response.text[:512]}",
                    exc.response.status_code,
                ) from exc
            last_error = exc
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
        else:
            return data

        if attempt + 1 < MonitorServer.SERVICE_DISCOVERY_RETRIES:
            time.sleep(MonitorServer.SERVICE_DISCOVERY_RETRY_DELAY_SECONDS)

    raise RunError(
        f"Cannot fetch RL-Insight services at {url}: {last_error}", 503
    ) from last_error


def initialize_server_connection(base_url: str | None = None) -> dict[str, Any]:
    """Discover Server endpoints and validate compatibility during initialization."""
    try:
        services = get_server_services(base_url)
    except RunError as exc:
        if exc.status in {404, 405}:
            require_version(None, "Server (services API unavailable)")
        if exc.status == 409:
            raise InvalidVersionError(str(exc), 409) from exc
        raise
    require_version(services.get("server_version"), "Server")
    return services


def create_app(conf: DictConfig) -> FastAPI:
    """Create the RL-Insight server application."""
    if task_collector_enabled(conf):
        from ..client.push.sinks.prometheus import create_task_collector

        return create_task_collector(conf)
    app = FastAPI(title="RL-Insight server", version=__version__)
    store = PrometheusTargetStore.from_config(conf)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        """Return a lightweight liveness response for process checks."""
        return {"status": "ok"}

    @app.get(f"{MonitorServer.API_PREFIX}/services")
    def services(
        client_version: str | None = Header(None, alias=SDK_VERSION_HEADER),
    ) -> dict[str, Any]:
        """Verify the connecting SDK and expose the Server's enabled services."""
        try:
            require_version(client_version, "Client")
        except InvalidVersionError as exc:
            raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
        return {
            "status": "ok",
            "server_version": __version__,
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
    metrics_port = int(OmegaConf.select(conf, "server.metrics_port", default=port))
    config = uvicorn.Config(None, host=local_addresses()["bind"], port=port)
    with ExitStack() as listeners:
        api_socket = listeners.enter_context(config.bind_socket())
        conf.server.port = api_socket.getsockname()[1]
        sockets = [api_socket]
        if metrics_port != port or (
            metrics_port == 0 and "metrics_port" in conf.server
        ):
            metrics_config = uvicorn.Config(None, host=config.host, port=metrics_port)
            metrics_socket = listeners.enter_context(metrics_config.bind_socket())
            conf.server.metrics_port = metrics_socket.getsockname()[1]
            sockets.append(metrics_socket)
        app = create_app(conf)
        if len(sockets) == 2:

            @app.middleware("http")
            async def metrics_only(request: Request, call_next):
                if request.scope["server"][1] == conf.server.metrics_port and not (
                    request.method in {"GET", "HEAD"}
                    and (request.url.path in {"/metrics", "/healthz"})
                ):
                    return Response(status_code=404)
                return await call_next(request)

        config.app = app
        uvicorn.Server(config).run(sockets=sockets)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
