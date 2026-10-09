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

"""Per-run Grafana datasources and run view links (S2 §7).

Every run owns two identity-bound datasources whose URLs are permanently bound
to the run-scoped query-router entries, so the correct data is opened no matter
which browser selection produced the link. The Dashboard templates stay shared
and select the pair through the ``metrics_datasource``/``traces_datasource``
variables; nothing is copied per run.

A Grafana failure never rolls back the run and never blocks collection: it only
turns the view ``not_ready`` with a stable reason code.
"""

from __future__ import annotations

import requests

DATASOURCE_PATH = "/api/datasources"
UID_PREFIX = "rlrun-"
UID_KEY_CHARS = 12
# Grafana datasource UIDs are limited to 40 characters; this scheme stays far
# below that so the frozen version never rejects it.
UID_MAX_CHARS = 40
PROMETHEUS = "prometheus"
TEMPO = "tempo"
_LABELS = {PROMETHEUS: "Prometheus", TEMPO: "Tempo"}


class GrafanaError(RuntimeError):
    """A Grafana call failed; ``code`` is the reason code surfaced to the UI."""

    def __init__(self, code: str, message: str = "", *, status: int = 502):
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message
        self.status = status


def run_datasource_uids(run_key: str) -> tuple[str, str]:
    """Deterministic identity-bound datasource UIDs of one run.

    The truncation is only a display/length device: the collision it could cause
    is detected by :meth:`RunViewService._ensure_datasource` (same UID, different
    URL) and reported as an error, never resolved by overwriting. The raw run ID
    stays the identity; the Registry is always the authority for it.
    """
    prefix = f"{UID_PREFIX}{run_key[:UID_KEY_CHARS]}"
    return f"{prefix}-prom", f"{prefix}-tempo"


class GrafanaClient:
    """Minimal Grafana datasource API client (create/read by UID)."""

    def __init__(
        self,
        base_url: str,
        *,
        token: str = "",
        session: requests.Session | None = None,
        timeout: float = 5.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.session = session if session is not None else requests.Session()
        self.timeout = timeout

    def get_datasource(self, uid: str) -> dict | None:
        response = self._request("GET", f"/uid/{uid}")
        if response.status_code == 404:
            return None
        self._check(response)
        return response.json()

    def create_datasource(self, payload: dict) -> dict:
        response = self._request("POST", "", payload)
        self._check(response)
        return response.json()

    def _request(self, method: str, path: str, payload: dict | None = None):
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        try:
            return self.session.request(
                method,
                f"{self.base_url}{DATASOURCE_PATH}{path}",
                headers=headers,
                json=payload,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            # The view degrades to ``not_ready``; collection is unaffected.
            raise GrafanaError("GRAFANA_UNREACHABLE", str(exc)) from exc

    def _check(self, response) -> None:
        if response.status_code >= 400:
            raise GrafanaError(
                "GRAFANA_HTTP_ERROR",
                f"Grafana answered {response.status_code} for {response.request.method}"
                f" {response.request.path_url}: {response.text[:200]}",
            )


class RunViewService:
    """Owns the per-run datasources and the run-scoped Dashboard links."""

    def __init__(
        self,
        registry,
        grafana: GrafanaClient,
        *,
        router_base: str,
        grafana_base: str,
        dashboard_uid: str = "",
    ):
        self.registry = registry
        self.grafana = grafana
        self.router_base = router_base.rstrip("/")
        self.grafana_base = grafana_base.rstrip("/")
        self.dashboard_uid = dashboard_uid
        self._ensured: set[str] = set()

    def ensure_run_view(
        self, run_key: str, *, from_ms: int | None = None, to_ms: int | None = None
    ) -> dict:
        """Resolve one run and return its view links, ensuring its datasources.

        Unknown runs raise :class:`RegistryError` for the caller to map onto its
        own error shape. A Grafana failure only flips ``status`` to ``not_ready``.
        """
        identity = self.registry.get_run(run_key)["identity"]
        metrics_uid, traces_uid = run_datasource_uids(run_key)
        status = "ready"
        reason_code = ""
        if run_key not in self._ensured:
            try:
                for kind, uid in ((PROMETHEUS, metrics_uid), (TEMPO, traces_uid)):
                    self._ensure_datasource(
                        uid, self._payload(run_key, identity, kind, uid)
                    )
            except GrafanaError as exc:
                status, reason_code = "not_ready", exc.code
            else:
                self._ensured.add(run_key)

        dashboard_url = self._dashboard_link(metrics_uid, traces_uid, from_ms, to_ms)
        if not dashboard_url and not reason_code:
            reason_code = "DASHBOARD_NOT_CONFIGURED"
        return {
            "run_key": run_key,
            "identity": identity,
            "status": status,
            "reason_code": reason_code,
            "metrics_uid": metrics_uid,
            "traces_uid": traces_uid,
            "metrics_url": dashboard_url,
            "traces_url": dashboard_url,
            "compare_url": f"/ui/compare?left={run_key}",
        }

    # -- internals ------------------------------------------------------
    def _ensure_datasource(self, uid: str, payload: dict) -> None:
        """Create the config this application owns, or refuse to touch a foreign one."""
        existing = self.grafana.get_datasource(uid)
        if existing is None:
            self.grafana.create_datasource(payload)
            return
        if (existing.get("url"), existing.get("type")) != (
            payload["url"],
            payload["type"],
        ):
            raise GrafanaError(
                "DATASOURCE_UID_CONFLICT",
                f"datasource {uid} already exists with another configuration",
                status=409,
            )

    def _payload(self, run_key: str, identity: dict, kind: str, uid: str) -> dict:
        # Grafana refuses a second datasource with the name of an existing one, and
        # project/experiment_name are explicitly not unique (two runs may share
        # them), so the name carries the run's short id as well.
        short_id = identity["id"][:8]
        display = identity.get("experiment_name") or f"run-{short_id}"
        return {
            "name": f"RL-Insight {identity['project']}/{display} ({short_id})"
            f" {_LABELS[kind]}",
            "uid": uid,
            "type": kind,
            "access": "proxy",
            "url": f"{self.router_base}/api/v1/runs/{run_key}/{kind}",
            "editable": False,
            "jsonData": {"httpMethod": "POST"},
        }

    def _dashboard_link(
        self,
        metrics_uid: str,
        traces_uid: str,
        from_ms: int | None,
        to_ms: int | None,
    ) -> str:
        """Open the shared dashboard with BOTH run variables bound to this run.

        The templates select Prometheus and Tempo panels through the two
        datasource variables, so binding only one of them would leave the other
        free to resolve to a shared/legacy datasource. Both are always set to the
        same-run pair.
        """
        if not self.dashboard_uid:
            return ""
        window = (
            f"from={from_ms}&to={to_ms}&"
            if from_ms is not None and to_ms is not None
            else ""
        )
        return (
            f"{self.grafana_base}/d/{self.dashboard_uid}"
            f"?var-metrics_datasource={metrics_uid}"
            f"&var-traces_datasource={traces_uid}&{window}kiosk"
        )
