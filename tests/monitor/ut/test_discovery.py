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

"""Discovery projection, lease expiry and the health projection (S2 §4)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from rl_insight.sdk.identity import RunIdentity
from rl_insight.sdk.publisher import slot_key
from rl_insight.server.discovery import DiscoveryPublisher
from rl_insight.server.run_registry import (
    LEASE_TTL_SECONDS,
    RunRegistry,
)

SERVER_INSTANCE = "server-1"


class _Clock:
    """A manually advanced clock so lease TTLs are testable."""

    def __init__(self, start: float = 1_000.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value


def _facts(
    host: str = "10.0.0.4", port: int = 8080, **overrides: Any
) -> dict[str, Any]:
    facts: dict[str, Any] = {
        "target_kind": "metrics",
        "component": "rollout",
        "instance_key": "replica-0",
        "incarnation": "pid-1",
        "advertised_host": host,
        "actual_port": port,
        "scheme": "http",
        "path": "/metrics",
        "expected_revision": 0,
    }
    facts.update(overrides)
    return facts


def _registry(tmp_path: Path, clock: _Clock) -> RunRegistry:
    return RunRegistry(
        tmp_path / "registry.sqlite",
        server_instance_id=SERVER_INSTANCE,
        now=clock,
    )


def _publish(registry: RunRegistry, **facts: Any) -> dict[str, Any]:
    run = registry.create_run(
        project="proj", experiment_name="exp", idempotency_key="k"
    )
    identity = RunIdentity.from_dict(run["identity"])
    slot = slot_key(identity, "metrics", "rollout", "replica-0")
    published = registry.upsert_endpoint(run["run_key"], slot, _facts(**facts))
    return {"run": run, "slot": slot, "published": published}


def test_effective_targets_require_a_live_lease(tmp_path: Path):
    clock = _Clock()
    registry = _registry(tmp_path, clock)
    published = _publish(registry)["published"]

    targets = registry.effective_targets()
    assert len(targets) == 1
    target = targets[0]
    assert target["run_id"]
    assert target["target_kind"] == "metrics"
    assert target["canonical_address"] == "http://10.0.0.4:8080/metrics"
    assert target["lease_id"] == published["lease_id"]

    clock.value += LEASE_TTL_SECONDS + 1
    assert registry.effective_targets() == []
    assert registry.expire_leases() == 1
    assert registry.expire_leases() == 0


def test_discovery_publishes_a_controlled_snapshot_only_when_it_changes(
    tmp_path: Path,
):
    clock = _Clock()
    registry = _registry(tmp_path, clock)
    run = _publish(registry)["run"]
    endpoint_id = registry.effective_targets()[0]["endpoint_id"]
    publisher = DiscoveryPublisher(
        registry,
        tmp_path / "managed-targets.yml",
        proxy_url="http://127.0.0.1:18080",
        now=clock,
    )

    first = publisher.refresh()
    assert (first["changed"], first["targets"]) == (True, 1)
    groups = yaml.safe_load((tmp_path / "managed-targets.yml").read_text())
    labels = groups[0]["labels"]
    # file-SD targets are host:port; scheme and path are separate labels, so
    # Prometheus reaches the controlled route exactly once.
    assert groups[0]["targets"][0] == "127.0.0.1:18080"
    assert labels["__scheme__"] == "http"
    assert labels["__metrics_path__"] == f"/internal/scrape/{endpoint_id}"
    assert labels["run_id"] == run["identity"]["id"]
    assert labels["project"] == "proj"
    assert labels["experiment_name"] == "exp"
    assert labels["instance"] == "http://10.0.0.4:8080/metrics"

    # An unchanged projection is not rewritten.
    assert publisher.refresh()["changed"] is False

    clock.value += LEASE_TTL_SECONDS + 1
    third = publisher.refresh()
    assert (third["changed"], third["targets"], third["expired_leases"]) == (
        True,
        0,
        1,
    )
    assert yaml.safe_load((tmp_path / "managed-targets.yml").read_text()) == []


def test_health_never_claims_scrape_from_an_active_run_alone(tmp_path: Path):
    clock = _Clock()
    registry = _registry(tmp_path, clock)
    entry = _publish(registry)
    run_key = entry["run"]["run_key"]

    health = registry.run_health(run_key)
    assert health["state"] == "active"
    assert health["targets"] == 1
    assert health["channels"]["registration"] == {"status": "ready", "reason_code": ""}
    assert health["channels"]["metrics_scrape"] == {
        "status": "degraded",
        "reason_code": "SCRAPE_NOT_OBSERVED",
    }
    assert health["channels"]["trace_online"] == {
        "status": "not_ready",
        "reason_code": "FORWARDER_NOT_RUNNING",
    }

    target = registry.effective_targets()[0]
    registry.note_scrape(target["endpoint_id"], target["incarnation"])
    assert (
        registry.run_health(run_key)["channels"]["metrics_scrape"]["status"] == "ready"
    )

    registry.note_forwarding(run_key, backlog=0)
    assert registry.run_health(run_key)["channels"]["trace_online"]["status"] == "ready"
    registry.note_forwarding(run_key, backlog=7)
    assert registry.run_health(run_key)["channels"]["trace_online"] == {
        "status": "degraded",
        "reason_code": "FORWARD_BACKLOG",
    }
    registry.note_forwarding(run_key, backlog=7, error_code="TEMPO_UNAVAILABLE")
    assert registry.run_health(run_key)["channels"]["trace_online"] == {
        "status": "degraded",
        "reason_code": "TEMPO_UNAVAILABLE",
    }

    # An expired lease is not a scrape failure: it is no target at all.
    clock.value += LEASE_TTL_SECONDS + 1
    assert registry.run_health(run_key)["channels"]["metrics_scrape"] == {
        "status": "not_ready",
        "reason_code": "NO_ACTIVE_TARGET",
    }
