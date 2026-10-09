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

"""Unit tests for the run Registry: identity, idempotency, leases, ownership."""

from __future__ import annotations

from pathlib import Path

import pytest

from rl_insight.integrations.verl import split_address
from rl_insight.sdk.identity import RunIdentity
from rl_insight.server.run_registry import RegistryError, RunRegistry

SERVER_INSTANCE = "server-1"


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def registry(tmp_path: Path) -> RunRegistry:
    return RunRegistry(
        tmp_path / "registry" / "experiments.sqlite",
        server_instance_id=SERVER_INSTANCE,
        now=_Clock(),
    )


def _facts(
    incarnation: str = "pid-1", address: str = "http://10.0.0.4:8080/metrics", **extra
):
    host, port, path = split_address(address)
    facts = {
        "target_kind": "metrics",
        "component": "rollout",
        "instance_key": "replica-0",
        "incarnation": incarnation,
        "advertised_host": host,
        "actual_port": port,
        "scheme": "http",
        "path": path,
        "expected_revision": 0,
    }
    facts.update(extra)
    return facts


def test_two_same_named_runs_get_two_identities(registry: RunRegistry):
    first = registry.create_run(project="proj", experiment_name="exp")
    second = registry.create_run(project="proj", experiment_name="exp")
    assert first["identity"]["id"] != second["identity"]["id"]
    assert first["run_key"] != second["run_key"]
    assert first["identity"]["experiment_name"] == "exp"
    assert len(registry.list_runs()) == 2


def test_an_empty_project_falls_into_uncategorized(registry: RunRegistry):
    record = registry.create_run(project="", experiment_name="")
    assert record["identity"]["project"] == "uncategorized"
    assert record["identity"]["experiment_name"] is None


def test_create_retries_reuse_the_run_and_reject_a_changed_body(registry: RunRegistry):
    first = registry.create_run(project="proj", idempotency_key="key-1")
    retry = registry.create_run(project="proj", idempotency_key="key-1")
    assert retry["identity"]["id"] == first["identity"]["id"]
    assert len(registry.list_runs()) == 1

    with pytest.raises(RegistryError) as excinfo:
        registry.create_run(project="other", idempotency_key="key-1")
    assert (excinfo.value.code, excinfo.value.status) == ("IDEMPOTENCY_CONFLICT", 409)


def test_resume_requires_the_same_project_and_an_active_run(registry: RunRegistry):
    record = registry.create_run(project="proj")
    key = record["run_key"]
    assert (
        registry.resume_run(key, project="proj", expected_revision=0)["identity"]["id"]
        == record["identity"]["id"]
    )

    with pytest.raises(RegistryError) as excinfo:
        registry.resume_run(key, project="elsewhere", expected_revision=0)
    assert (excinfo.value.code, excinfo.value.status) == ("OWNERSHIP_CONFLICT", 409)

    with pytest.raises(RegistryError) as excinfo:
        registry.resume_run(key, project="proj", expected_revision=99)
    assert excinfo.value.code == "REVISION_CONFLICT"

    with pytest.raises(RegistryError) as excinfo:
        registry.resume_run("0" * 64, project="proj", expected_revision=0)
    assert (excinfo.value.code, excinfo.value.status) == ("RUN_NOT_FOUND", 404)


def test_an_archived_run_refuses_writes_and_plain_resume(registry: RunRegistry):
    identity = RunIdentity(id="archived-run", source="managed", project="proj")
    registry.install_imported_run(identity, state="archived")
    with pytest.raises(RegistryError) as excinfo:
        registry.writable_identity(identity.run_key)
    assert (excinfo.value.code, excinfo.value.status) == ("RUN_ARCHIVED", 410)
    with pytest.raises(RegistryError):
        registry.resume_run(identity.run_key, project="proj", expected_revision=0)
    with pytest.raises(RegistryError):
        registry.upsert_endpoint(identity.run_key, "slot", _facts())


def test_import_never_overwrites_an_existing_run(registry: RunRegistry):
    record = registry.create_run(project="proj")
    identity = RunIdentity.from_dict(record["identity"])
    with pytest.raises(RegistryError) as excinfo:
        registry.install_imported_run(identity)
    assert (excinfo.value.code, excinfo.value.status) == ("RUN_CONFLICT", 409)


def test_capacity_limit_refuses_new_runs_without_touching_existing_ones(tmp_path: Path):
    registry = RunRegistry(
        tmp_path / "r.sqlite", server_instance_id=SERVER_INSTANCE, active_run_limit=1
    )
    registry.create_run(project="proj")
    with pytest.raises(RegistryError) as excinfo:
        registry.create_run(project="proj")
    assert (excinfo.value.code, excinfo.value.status, excinfo.value.retryable) == (
        "CAPACITY_EXCEEDED",
        429,
        True,
    )
    assert len(registry.list_runs()) == 1


def test_endpoint_upsert_is_idempotent_for_the_same_incarnation(registry: RunRegistry):
    key = registry.create_run(project="proj")["run_key"]
    first = registry.upsert_endpoint(key, "slot-1", _facts())
    retry = registry.upsert_endpoint(key, "slot-1", _facts())
    assert first["endpoint_id"] == retry["endpoint_id"]
    assert first["revision"] == retry["revision"] == 1


def test_replacing_an_endpoint_needs_a_matching_revision(registry: RunRegistry):
    key = registry.create_run(project="proj")["run_key"]
    first = registry.upsert_endpoint(key, "slot-1", _facts())
    with pytest.raises(RegistryError) as excinfo:
        registry.upsert_endpoint(
            key, "slot-1", _facts(incarnation="pid-2", expected_revision=0)
        )
    assert excinfo.value.code == "REVISION_CONFLICT"

    replaced = registry.upsert_endpoint(
        key,
        "slot-1",
        _facts(
            incarnation="pid-2",
            address="http://10.0.0.4:9090/metrics",
            expected_revision=1,
        ),
    )
    assert replaced["revision"] == 2
    assert replaced["endpoint_id"] == first["endpoint_id"]
    assert replaced["lease_id"] != first["lease_id"]


def test_a_replaced_incarnation_can_never_publish_or_renew_again(registry: RunRegistry):
    key = registry.create_run(project="proj")["run_key"]
    first = registry.upsert_endpoint(key, "slot-1", _facts())
    registry.upsert_endpoint(
        key, "slot-1", _facts(incarnation="pid-2", expected_revision=1)
    )
    with pytest.raises(RegistryError) as excinfo:
        registry.upsert_endpoint(
            key,
            "slot-1",
            _facts(address="http://10.0.0.4:8080/metrics", expected_revision=2),
        )
    assert (excinfo.value.code, excinfo.value.status) == ("OWNERSHIP_CONFLICT", 409)

    with pytest.raises(RegistryError) as excinfo:
        registry.renew_lease(
            key,
            first["lease_id"],
            {"incarnation": "pid-1", "revision": first["revision"]},
        )
    assert excinfo.value.code in ("LEASE_EXPIRED", "OWNERSHIP_CONFLICT")


def test_a_port_change_keeps_the_slot_and_retires_the_old_incarnation(
    registry: RunRegistry,
):
    key = registry.create_run(project="proj")["run_key"]
    registry.upsert_endpoint(key, "slot-1", _facts())
    moved = registry.upsert_endpoint(
        key,
        "slot-1",
        _facts(
            incarnation="pid-2",
            address="http://10.0.0.4:9999/metrics",
            expected_revision=1,
        ),
    )
    assert moved["endpoint_id"]
    # same slot, so no second endpoint is created for the run
    assert len(registry.endpoints_for(key)) == 1
    assert (
        registry.endpoints_for(key)[0]["canonical_address"]
        == "http://10.0.0.4:9999/metrics"
    )


def test_one_address_cannot_belong_to_two_runs(registry: RunRegistry):
    first = registry.create_run(project="proj")["run_key"]
    second = registry.create_run(project="proj")["run_key"]
    registry.upsert_endpoint(first, "slot-1", _facts())
    with pytest.raises(RegistryError) as excinfo:
        registry.upsert_endpoint(second, "slot-1", _facts())
    assert (excinfo.value.code, excinfo.value.status) == ("OWNERSHIP_CONFLICT", 409)


def test_one_address_cannot_belong_to_two_slots_of_one_run(registry: RunRegistry):
    """Two slots on one address would be scraped twice for the same run."""
    key = registry.create_run(project="proj")["run_key"]
    registry.upsert_endpoint(key, "slot-1", _facts())
    with pytest.raises(RegistryError) as excinfo:
        registry.upsert_endpoint(key, "slot-2", _facts(instance_key="replica-1"))
    assert (excinfo.value.code, excinfo.value.status) == ("OWNERSHIP_CONFLICT", 409)


def test_a_client_supplied_address_is_never_the_authority(registry: RunRegistry):
    """The address is derived from the parts, so a second spelling is one owner."""
    key = registry.create_run(project="proj")["run_key"]
    registry.upsert_endpoint(key, "slot-1", _facts())
    with pytest.raises(RegistryError) as excinfo:
        registry.upsert_endpoint(
            key,
            "slot-2",
            _facts(
                instance_key="replica-1",
                canonical_address="10.0.0.4:8080",  # a different spelling, one owner
            ),
        )
    assert excinfo.value.code == "OWNERSHIP_CONFLICT"


def test_a_stale_publication_epoch_cannot_mint_a_lease(registry: RunRegistry):
    key = registry.create_run(project="proj")["run_key"]
    with pytest.raises(RegistryError) as excinfo:
        registry.upsert_endpoint(key, "slot-1", _facts(lifecycle_epoch=99))
    assert (excinfo.value.code, excinfo.value.status) == ("STALE_EPOCH", 409)


def test_renew_reports_the_endpoints_real_revision(registry: RunRegistry):
    """A renewal must not echo a revision that only the request invented."""
    key = registry.create_run(project="proj")["run_key"]
    registry.upsert_endpoint(key, "slot-1", _facts())
    moved = registry.upsert_endpoint(
        key, "slot-1", _facts(incarnation="pid-2", expected_revision=1)
    )
    renewed = registry.renew_lease(key, moved["lease_id"], {"incarnation": "pid-2"})
    assert renewed["revision"] == moved["revision"] == 2


def test_a_resumed_run_rejects_a_malformed_project(registry: RunRegistry):
    key = registry.create_run(project="proj")["run_key"]
    with pytest.raises(RegistryError) as excinfo:
        registry.resume_run(key, project="x" * 5000, expected_revision=0)
    assert excinfo.value.code == "INVALID_IDENTITY"


def test_an_imported_active_run_counts_against_the_active_run_limit(tmp_path: Path):
    registry = RunRegistry(
        tmp_path / "r.sqlite", server_instance_id=SERVER_INSTANCE, active_run_limit=1
    )
    registry.create_run(project="proj")
    with pytest.raises(RegistryError) as excinfo:
        registry.install_imported_run(
            RunIdentity(id="imported", source="managed", project="proj"), state="active"
        )
    assert (excinfo.value.code, excinfo.value.status) == ("CAPACITY_EXCEEDED", 429)


def test_a_new_incarnation_must_earn_its_own_scrape_observation(registry: RunRegistry):
    key = registry.create_run(project="proj")["run_key"]
    registry.upsert_endpoint(key, "slot-1", _facts())
    target = registry.effective_targets()[0]
    registry.note_scrape(target["endpoint_id"], target["incarnation"])
    assert registry.run_health(key)["channels"]["metrics_scrape"]["status"] == "ready"

    registry.upsert_endpoint(
        key, "slot-1", _facts(incarnation="pid-2", expected_revision=1)
    )
    # The lease holder changed; the old observation must not carry over.
    assert registry.run_health(key)["channels"]["metrics_scrape"] == {
        "status": "degraded",
        "reason_code": "SCRAPE_NOT_OBSERVED",
    }


def test_a_lease_expires_and_cannot_be_revived(tmp_path: Path):
    clock = _Clock()
    registry = RunRegistry(
        tmp_path / "r.sqlite", server_instance_id=SERVER_INSTANCE, now=clock
    )
    key = registry.create_run(project="proj")["run_key"]
    lease = registry.upsert_endpoint(key, "slot-1", _facts())
    clock.advance(31)
    with pytest.raises(RegistryError) as excinfo:
        registry.renew_lease(
            key, lease["lease_id"], {"incarnation": "pid-1", "revision": 1}
        )
    assert (excinfo.value.code, excinfo.value.status) == ("LEASE_EXPIRED", 410)


def test_an_epoch_mismatch_is_not_retried_with_a_fresh_epoch(registry: RunRegistry):
    key = registry.create_run(project="proj")["run_key"]
    with pytest.raises(RegistryError) as excinfo:
        registry.writable_identity(key, epoch=99)
    assert (excinfo.value.code, excinfo.value.status) == ("STALE_EPOCH", 409)
