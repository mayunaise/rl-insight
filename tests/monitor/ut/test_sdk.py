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

"""Unit tests for the S0 SDK contracts (identity, context, config, publisher, session)."""

from __future__ import annotations

from typing import Any

import pytest

from rl_insight.sdk import config as config_module
from rl_insight.sdk import publisher as publisher_module
from rl_insight.sdk import session as session_module
from rl_insight.sdk.identity import (
    Context,
    ContextError,
    IdentityError,
    RunIdentity,
    platform_identity,
    run_key,
    validate_run_id,
)

TRACE_ID = "11111111-2222-3333-4444-555555555555"


# --------------------------------------------------------------------------- identity


def test_run_id_is_kept_verbatim():
    for raw in ("Run/A", "  spaced  ", "Ünïcode", "A" * 256):
        assert validate_run_id(raw) == raw


@pytest.mark.parametrize("raw", ["", "bad\nid"])
def test_invalid_run_id_is_rejected(raw: Any):
    with pytest.raises(IdentityError):
        validate_run_id(raw)


def test_managed_identity_uses_run_id_label_and_uncategorized_project():
    identity = RunIdentity(id="abc12345-rest", source="managed")
    assert identity.project == "uncategorized"
    assert identity.experiment_name is None
    assert identity.display_name == "run-abc12345"
    assert identity.metrics_label == "run_id"
    assert identity.trace_attribute == "rl_insight.run_id"


def test_platform_identity_keeps_task_id_and_uses_task_labels():
    identity = platform_identity("job-1", project="proj", experiment_name="exp")
    assert identity.id == "job-1"
    assert identity.display_name == "exp"
    assert identity.metrics_label == "task_id"
    assert identity.trace_attribute == "rl_insight.task_id"


def test_identity_target_labels_carry_the_authoritative_id():
    identity = RunIdentity(id="id-1", source="managed", experiment_name="exp")
    assert identity.target_labels == {
        "run_id": "id-1",
        "project": "uncategorized",
        "experiment_name": "exp",
    }


# --------------------------------------------------------------------------- context


def test_context_round_trips_and_keeps_values_only():
    context = Context(
        identity=RunIdentity(id="id-1", source="managed"),
        services={"server": "http://127.0.0.1:18080"},
        capabilities=("identity_v1",),
        publisher={"adapter": "managed"},
        lifecycle_epoch=3,
    )
    restored = Context.from_json(context.to_json())
    assert restored == context
    assert '"adapter": "managed"' in context.to_json()


def test_context_rejects_unsupported_version_and_bad_payload():
    with pytest.raises(ContextError):
        Context(identity=RunIdentity(id="id-1", source="managed"), context_version=2)
    with pytest.raises(ContextError):
        Context.from_dict(
            {"context_version": 2, "identity": {"id": "a", "source": "managed"}}
        )
    with pytest.raises(ContextError):
        Context.from_dict({"context_version": 1})
    with pytest.raises(ContextError):
        Context.from_json("{not json")


# --------------------------------------------------------------------------- config


def test_config_priority_is_explicit_env_file_then_default(tmp_path):
    config_file = tmp_path / "sdk.yaml"
    config_file.write_text(
        "server.url: http://file:1\nplatform.task_id: from-file\n", encoding="utf-8"
    )
    env = {
        "RL_INSIGHT_SERVER_URL": "http://env:2",
        "RL_INSIGHT_CONFIG_FILE": str(config_file),
    }

    resolved = config_module.resolve_config({}, env=env)
    assert resolved.server_url == "http://env:2"
    assert resolved.origins["server.url"] == "env"
    assert resolved.mode == "managed"
    assert any("ignored" in item for item in resolved.diagnostics)

    explicit = config_module.resolve_config(
        {"server.url": "http://explicit:3"}, env=env
    )
    assert explicit.server_url == "http://explicit:3"
    assert explicit.origins["server.url"] == "explicit"

    from_file = config_module.resolve_config(
        {}, env={"RL_INSIGHT_CONFIG_FILE": str(config_file)}
    )
    assert from_file.server_url == "http://file:1"
    assert from_file.origins["server.url"] == "file"


def test_explicit_none_clears_a_lower_priority_value():
    env = {"RL_INSIGHT_SERVER_URL": "http://env:2"}
    resolved = config_module.resolve_config({"server.url": None}, env=env)
    assert resolved.server_url is None
    assert resolved.mode == "disabled"


def test_task_id_selects_platform_and_is_validated():
    resolved = config_module.resolve_config({}, env={"RL_INSIGHT_TASK_ID": "task-9"})
    assert resolved.mode == "platform"
    assert resolved.task_id == "task-9"
    with pytest.raises(config_module.ConfigError):
        config_module.resolve_config({}, env={"RL_INSIGHT_TASK_ID": "bad\nid"})


def test_server_url_wins_over_task_id_without_generating_a_platform_session():
    resolved = config_module.resolve_config(
        {}, env={"RL_INSIGHT_SERVER_URL": "http://s:1", "RL_INSIGHT_TASK_ID": "task-9"}
    )
    assert resolved.mode == "managed"
    assert resolved.task_id is None


@pytest.mark.parametrize("url", ["ftp://s:1"])
def test_bad_server_url_is_an_error_not_a_fallback(url: str):
    with pytest.raises(config_module.ConfigError):
        config_module.resolve_config({"server.url": url})


def test_trace_endpoint_assembly_and_conflict():
    assembled = config_module.resolve_config(
        {
            "platform.task_id": TRACE_ID,
            "trace.host": "10.0.0.4",
            "trace.port": 4318,
        }
    )
    assert assembled.trace_endpoint == "http://10.0.0.4:4318/v1/traces"

    custom_path = config_module.resolve_config(
        {
            "platform.task_id": TRACE_ID,
            "trace.endpoint": "http://10.0.0.4:4318/custom/traces",
        }
    )
    assert custom_path.trace_endpoint == "http://10.0.0.4:4318/custom/traces"

    conflicting = config_module.resolve_config(
        {
            "platform.task_id": TRACE_ID,
            "trace.endpoint": "http://10.0.0.4:4318/a",
            "trace.host": "10.0.0.4",
            "trace.port": 4318,
            "trace.path": "/b",
        }
    )
    assert conflicting.trace_endpoint is None
    assert conflicting.trace_reason_code == "TRACE_ENDPOINT_CONFLICT"
    assert conflicting.readiness()["trace_accept"].status == "not_ready"


def test_resume_id_requires_the_resume_flag_and_managed_mode():
    with pytest.raises(config_module.ConfigError):
        config_module.resolve_config(
            {"server.url": "http://s:1", "run.resume_id": "id-1"}
        )
    with pytest.raises(config_module.ConfigError):
        config_module.resolve_config({"server.url": "http://s:1", "run.resume": True})
    with pytest.raises(config_module.ConfigError):
        config_module.resolve_config(
            {"platform.task_id": "t", "run.resume_id": "id-1", "run.resume": True}
        )
    ok = config_module.resolve_config(
        {"server.url": "http://s:1", "run.resume_id": "id-1", "run.resume": True}
    )
    assert (ok.resume_id, ok.resume) == ("id-1", True)


def test_disabled_configuration_reports_every_channel_and_keeps_no_trace():
    resolved = config_module.resolve_config({}, env={})
    assert resolved.mode == "disabled"
    assert resolved.enabled is False
    assert resolved.trace_endpoint is None
    readiness = resolved.readiness()
    assert set(readiness) == set(config_module.CHANNELS)
    assert readiness["registration"].status == "unsupported"
    assert readiness["export"].status == "unsupported"


def test_platform_trace_only_config_reports_metrics_as_unsupported():
    resolved = config_module.resolve_config(
        {"platform.task_id": "t-1", "trace.endpoint": "http://h:4318/v1/traces"}
    )
    readiness = resolved.readiness()
    assert readiness["trace_accept"].status == "ready"
    assert readiness["metrics_scrape"].status == "unsupported"
    assert readiness["export"].status == "unsupported"


# --------------------------------------------------------------------------- publisher


def _facts(**overrides: Any) -> dict[str, Any]:
    facts = {
        "target_kind": "metrics",
        "component": "rollout",
        "instance_key": "replica-0",
        "incarnation": "pid-42",
        "advertised_host": "10.0.0.4",
        "actual_port": 8080,
        "scheme": "http",
        "path": "/metrics",
        "expected_revision": 0,
    }
    facts.update(overrides)
    return facts


def test_publication_facts_reject_wildcards_and_bad_ports():
    normalized = publisher_module.validate_publication_facts(_facts())
    assert normalized["actual_port"] == 8080
    for bad in (
        {"advertised_host": "0.0.0.0"},
        {"advertised_host": "::"},
        {"advertised_host": ""},
    ):
        with pytest.raises(publisher_module.ProtocolError) as excinfo:
            publisher_module.validate_publication_facts(_facts(**bad))
        assert excinfo.value.code == "INVALID_CONFIG"
    for bad in ({"actual_port": 0}, {"actual_port": 70000}, {"actual_port": "x"}):
        with pytest.raises(publisher_module.ProtocolError):
            publisher_module.validate_publication_facts(_facts(**bad))
    with pytest.raises(publisher_module.ProtocolError):
        publisher_module.validate_publication_facts(_facts(component=""))
    with pytest.raises(publisher_module.ProtocolError):
        publisher_module.validate_publication_facts(_facts(scheme="grpc"))


def test_canonical_address_is_normalized_for_ownership_checks():
    assert (
        publisher_module.canonical_address("http", "10.0.0.4", 8080, "metrics")
        == "http://10.0.0.4:8080/metrics"
    )
    assert (
        publisher_module.canonical_address("http", "10.0.0.4", 8080, "/metrics")
        == "http://10.0.0.4:8080/metrics"
    )


def test_unsupported_publisher_never_silently_succeeds():
    identity = RunIdentity(id="id-1", source="managed")
    publisher = publisher_module.UnsupportedPublisher("NO_PUBLISHER")
    result = publisher.upsert(identity, _facts())
    assert result.accepted is False
    assert result.code == "UNSUPPORTED_CAPABILITY"


# --------------------------------------------------------------------------- session


class FakeClient:
    """Minimal ServerClient stand-in that records the protocol calls it sees."""

    def __init__(
        self, *, server_instance_id: str = "server-1", fail_delete: bool = False
    ):
        self.server_instance_id = server_instance_id
        self.fail_delete = fail_delete
        self.calls: list[tuple[Any, ...]] = []
        self.deleted: list[str] = []

    def create_run(
        self, *, project: str, experiment_name: str | None, idempotency_key: str
    ):
        self.calls.append(("create_run", project, experiment_name, idempotency_key))
        return {
            "identity": {
                "id": "run-1",
                "source": "managed",
                "project": project or "uncategorized",
                "experiment_name": experiment_name,
            },
            "services": {"otlp_http": "http://s:1/api/v1/otlp/v1/traces"},
            "capabilities": ["identity_v1"],
            "server_instance_id": self.server_instance_id,
            "lifecycle_epoch": 1,
        }

    def services(self) -> dict[str, Any]:
        """Discovery payload; this stand-in advertises no extra service."""
        return {"services": {}}

    def resume_run(self, run_key: str, *, project: str, expected_revision: int):
        self.calls.append(("resume_run", run_key, project, expected_revision))
        return {
            "identity": {
                "id": "run-9",
                "source": "managed",
                "project": project or "uncategorized",
                "experiment_name": None,
            },
            "services": {},
            "server_instance_id": self.server_instance_id,
            "lifecycle_epoch": 4,
        }

    def upsert_endpoint(self, run_key: str, slot: str, payload: Any):
        self.calls.append(("upsert_endpoint", run_key, slot, dict(payload)))
        return {
            "endpoint_id": "ep-1",
            "lease_id": "lease-1",
            "revision": 1,
            "lifecycle_epoch": 4,
            "expires_at": 100.0,
            "renew_after": 10.0,
        }

    def delete_lease(
        self, run_key: str, lease_id: str, payload: Any, *, timeout: float | None = None
    ):
        self.calls.append(("delete_lease", run_key, lease_id, dict(payload)))
        if self.fail_delete:
            raise publisher_module.ProtocolError(
                "LEASE_EXPIRED", "gone", retryable=False
            )
        self.deleted.append(lease_id)
        return {"deleted": True, "endpoint_id": "ep-1"}


def test_open_session_creates_one_run_and_hands_context_to_workers():
    client = FakeClient()
    session = session_module.open_session(
        {"project": "proj", "experiment_name": "exp", "idempotency_key": "key-1"},
        config={"server.url": "http://s:1"},
        client=client,
    )
    assert session.identity.id == "run-1"
    assert session.context.services["server"] == "http://s:1"
    assert client.calls[0][0] == "create_run"

    worker = session_module.attach(session.context.to_json())
    assert worker.identity == session.identity
    assert worker is not session


class DiscoveryClient(FakeClient):
    """A managed server that advertises its OTLP address via discovery alone."""

    def services(self) -> dict[str, Any]:
        return {"services": {"otlp_http": "/api/v1/otlp/v1/traces"}}

    def create_run(self, **kwargs: Any) -> dict[str, Any]:
        return {**super().create_run(**kwargs), "services": {}}


def test_managed_session_resolves_its_otlp_address_from_the_server_url():
    session = session_module.open_session(
        {"project": "proj"},
        config={"server.url": "http://s:1"},
        client=DiscoveryClient(),
    )
    try:
        endpoint = "http://s:1/api/v1/otlp/v1/traces"
        # Pointing the SDK at the server is enough: the driver exports to the
        # address that server advertises ...
        assert session.config.trace_endpoint == endpoint
        # ... and a worker inherits it from the serialized context.
        assert session.context.services["otlp_http"] == endpoint
        worker = session_module.attach(session.context.to_json())
        assert worker.tracer.enabled is True
    finally:
        session.close()


def test_open_session_resumes_only_with_an_explicit_id():
    client = FakeClient()
    session = session_module.open_session(
        {"project": "proj"},
        config={
            "server.url": "http://s:1",
            "run.resume_id": "run-9",
            "run.resume": True,
        },
        client=client,
    )
    assert session.identity.id == "run-9"
    # resume addresses the run by its irreversible key, never by the raw id
    assert client.calls[0] == ("resume_run", run_key("managed", "run-9"), "proj", 0)
    assert "run-9" not in client.calls[0][1]


def test_publish_endpoint_records_the_lease_and_close_revokes_it():
    client = FakeClient()
    session = session_module.open_session(
        {}, config={"server.url": "http://s:1"}, client=client
    )
    result = session.publish_endpoint(_facts())
    assert result.accepted is True
    assert list(session.leases) == [
        publisher_module.slot_key(session.identity, "metrics", "rollout", "replica-0")
    ]

    report = session.close()
    assert report.closed is True
    assert report.revoked_leases == ("lease-1",)
    assert client.deleted == ["lease-1"]
    assert session.readiness()["registration"].reason_code == "SESSION_CLOSED"

    # close is idempotent and does not revoke twice
    assert session.close().revoked_leases == ()
    assert client.deleted == ["lease-1"]


def test_close_reports_a_failed_revocation_instead_of_hiding_it():
    client = FakeClient(fail_delete=True)
    session = session_module.open_session(
        {}, config={"server.url": "http://s:1"}, client=client
    )
    session.publish_endpoint(_facts())
    report = session.close()
    assert report.revoked_leases == ()
    assert report.errors == ("lease-1: LEASE_EXPIRED",)


def test_same_process_double_session_keeps_identities_and_registries_separate():
    first = session_module.open_session(
        {}, config={"server.url": "http://s:1"}, client=FakeClient()
    )
    second = session_module.open_session(
        {"project": "other"}, config={"server.url": "http://s:1"}, client=FakeClient()
    )
    second.close()
    assert second.closed is True
    assert first.closed is False
    assert first.leases == {}
    assert first.identity is not second.identity
