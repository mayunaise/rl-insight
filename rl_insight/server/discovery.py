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

"""Controlled file-SD projection of the Registry (S2 §4.1).

The Registry is the only authority. Its effective targets are projected into
one file-SD snapshot with an atomic replace, written only when the content
changed and always from a single coordinator thread, so a late snapshot can
never overwrite a newer one. Prometheus scrapes the controlled route, not the
exporter address directly; that route re-checks the live lease on every fetch.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit

import yaml

from ..sdk.identity import METRIC_LABEL
from ..utils.constants import PrometheusScrape
from .network import format_host_port

METRICS_TARGET_KIND = "metrics"
DEFAULT_INTERVAL_SECONDS = 5.0

logger = logging.getLogger(__name__)


class DiscoveryPublisher:
    """Project effective Registry targets into the controlled file-SD snapshot."""

    def __init__(
        self,
        registry: Any,
        path: Path | str,
        *,
        proxy_url: str,
        interval: float = DEFAULT_INTERVAL_SECONDS,
        now: Callable[[], float] = time.time,
    ) -> None:
        self.registry = registry
        self.path = Path(path)
        # file-SD targets are ``host:port``: the scheme and the path of the
        # controlled route are separate labels, never a URL in ``targets``
        # (Prometheus file_sd contract).
        parts = urlsplit(str(proxy_url).strip())
        if not parts.hostname or parts.port is None:
            raise ValueError(f"proxy_url {proxy_url!r} is not a scheme://host:port URL")
        self.proxy_target = format_host_port(parts.hostname, parts.port)
        self.proxy_scheme = parts.scheme or "http"
        self.interval = float(interval)
        self._now = now
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- projection ----------------------------------------------------
    def groups(self) -> list[dict[str, Any]]:
        """One file-SD group per effective metrics target."""
        groups: list[dict[str, Any]] = []
        for target in self.registry.effective_targets():
            if target["target_kind"] != METRICS_TARGET_KIND:
                continue
            identity_label = METRIC_LABEL[target["source"]]
            groups.append(
                {
                    "targets": [self.proxy_target],
                    "labels": {
                        # Prometheus reads the controlled route from these two
                        # labels, so the scrape goes through the lease gate
                        # instead of the exporter address (S2 §4.1/§4.2).
                        "__scheme__": self.proxy_scheme,
                        "__metrics_path__": (
                            f"{PrometheusScrape.SCRAPE_ROUTE}/{target['endpoint_id']}"
                        ),
                        identity_label: target["run_id"],
                        "project": target["project"],
                        "experiment_name": target["experiment_name"] or "",
                        "component": target["component"],
                        "instance_key": target["instance_key"],
                        # Keep the real business address visible instead of
                        # letting every target look like the proxy (S2 §4.2).
                        "instance": target["canonical_address"],
                    },
                }
            )
        return groups

    def refresh(self) -> dict[str, Any]:
        """Retire expired leases, then republish the snapshot if it changed."""
        with self._lock:
            expired = self.registry.expire_leases()
            groups = self.groups()
            payload = yaml.safe_dump(groups, sort_keys=True)
            changed = _write_if_changed(self.path, payload)
        return {
            "changed": changed,
            "targets": len(groups),
            "expired_leases": expired,
            "updated_at": self._now(),
        }

    # -- coordinator ---------------------------------------------------
    def wake(self) -> None:
        """Ask the coordinator to refresh now instead of waiting for the tick."""
        self._wake.set()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="rl-insight-discovery", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval + 1.0)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.refresh()
            except Exception:  # a failed tick must not stop the coordinator
                logger.exception("[rl-insight] discovery refresh failed")
            # Woken by a registration change, otherwise the interval also
            # covers a crash window (S2 §4.1).
            self._wake.wait(self.interval)
            self._wake.clear()


def _write_if_changed(path: Path, payload: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") == payload:
        return False
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(payload, encoding="utf-8")
    os.replace(temporary, path)
    return True


__all__ = ["DiscoveryPublisher", "METRICS_TARGET_KIND"]
