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

"""Unit tests for the Ray monitor client."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from omegaconf import OmegaConf

from rl_insight.client import ray_monitor_client as client_module
from rl_insight.utils.constants import MonitorRayActor

_JOB_ACTOR_NAME = f"{MonitorRayActor.NAME}_test123"


def _patch_actor_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub ``_current_job_actor_name`` to return a predictable job-scoped name."""
    monkeypatch.setattr(
        client_module,
        "_current_job_actor_name",
        lambda: _JOB_ACTOR_NAME,
    )


def test_create_ray_monitor_client_should_return_none_when_ray_is_not_initialized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module.ray, "is_initialized", lambda: False)

    assert client_module.create_ray_monitor_client(OmegaConf.create({})) is None


def test_get_or_create_monitor_hub_should_reuse_actor_when_actor_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_actor_name(monkeypatch)

    actor = MagicMock()
    get_actor = MagicMock(return_value=actor)
    monkeypatch.setattr(client_module.ray, "get_actor", get_actor)

    assert client_module.get_or_create_monitor_hub(OmegaConf.create({})) is actor
    get_actor.assert_called_once_with(
        _JOB_ACTOR_NAME, namespace=MonitorRayActor.NAMESPACE
    )


def test_get_or_create_monitor_hub_should_create_actor_when_actor_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_actor_name(monkeypatch)

    conf = OmegaConf.create({"server": {"url": "http://server"}})
    actor = MagicMock()
    remote = MagicMock(return_value=actor)
    options = MagicMock(return_value=MagicMock(remote=remote))
    monkeypatch.setattr(
        client_module.ray, "get_actor", MagicMock(side_effect=ValueError)
    )
    monkeypatch.setattr(client_module, "MonitorHubActor", MagicMock(options=options))

    assert client_module.get_or_create_monitor_hub(conf) is actor
    options.assert_called_once_with(
        name=_JOB_ACTOR_NAME,
        namespace=MonitorRayActor.NAMESPACE,
        runtime_env={
            "env_vars": {"RL_INSIGHT_SERVER_URL": "http://server"},
        },
    )
    remote.assert_called_once_with(conf)


def test_get_or_create_monitor_hub_should_reuse_winner_when_creation_races(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_actor_name(monkeypatch)

    conf = OmegaConf.create({"server": {"url": "http://server"}})
    winner = MagicMock()
    get_actor = MagicMock(side_effect=[winner])
    remote = MagicMock(side_effect=ValueError)
    monkeypatch.setattr(client_module.ray, "get_actor", get_actor)
    monkeypatch.setattr(
        client_module,
        "MonitorHubActor",
        MagicMock(options=MagicMock(return_value=MagicMock(remote=remote))),
    )

    assert client_module.get_or_create_monitor_hub(conf) is winner
    get_actor.assert_called_with(_JOB_ACTOR_NAME, namespace=MonitorRayActor.NAMESPACE)


def test_apply_event_should_submit_without_waiting_when_client_has_actor() -> None:
    actor = MagicMock()
    client = client_module.MonitorRayClient(actor)
    event = {"kind": "counter", "name": "steps", "value": 1}

    client.apply_event(event)

    actor.apply_event.remote.assert_called_once_with(event)


# ------------------------------------------------------------------ S3 session mode

_CONTEXT = {
    "context_version": 1,
    "identity": {
        "id": "run-1",
        "source": "managed",
        "project": "proj",
        "experiment_name": "exp",
    },
    "services": {"server": "http://s:1", "otlp_http": "http://s:1/v1/traces"},
    "capabilities": ["identity_v1"],
    "publisher": {"adapter": "managed"},
    "server_instance_id": "srv-1",
    "lifecycle_epoch": 3,
}


def _patch_job(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        client_module, "_current_job_actor_name", lambda: _JOB_ACTOR_NAME
    )


def _run_scoped_name() -> str:
    from rl_insight.sdk.identity import RunIdentity

    return f"{_JOB_ACTOR_NAME}_{RunIdentity.from_dict(_CONTEXT['identity']).run_key}"


def test_hub_actor_name_is_run_scoped(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_job(monkeypatch)
    from rl_insight.sdk.identity import Context

    first = client_module._hub_actor_name(Context.from_dict(_CONTEXT))
    other = dict(_CONTEXT)
    other["identity"] = {**_CONTEXT["identity"], "id": "run-2"}
    second = client_module._hub_actor_name(Context.from_dict(other))

    assert first == _run_scoped_name()
    assert "run-1" not in first and "exp" not in first
    assert first != second


def test_get_or_create_monitor_hub_rejects_a_hub_serving_another_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_job(monkeypatch)
    monkeypatch.setattr(client_module, "_HUB_HANDLES", {})
    handle = MagicMock()
    monkeypatch.setattr(client_module.ray, "get_actor", MagicMock(return_value=handle))
    monkeypatch.setattr(
        client_module.ray,
        "get",
        MagicMock(
            return_value={
                "id": "run-9",
                "source": "managed",
                "project": "proj",
                "experiment_name": "exp",
            }
        ),
    )

    with pytest.raises(client_module.HubIdentityConflict):
        client_module.get_or_create_monitor_hub(OmegaConf.create({}), _CONTEXT)


def test_get_or_create_monitor_hub_reuses_matching_run_and_retains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_job(monkeypatch)
    monkeypatch.setattr(client_module, "_HUB_HANDLES", {})
    handle = MagicMock()
    monkeypatch.setattr(client_module.ray, "get_actor", MagicMock(return_value=handle))
    monkeypatch.setattr(
        client_module.ray, "get", MagicMock(return_value=_CONTEXT["identity"])
    )

    assert (
        client_module.get_or_create_monitor_hub(OmegaConf.create({}), _CONTEXT)
        is handle
    )
    assert (
        client_module.get_or_create_monitor_hub(OmegaConf.create({}), _CONTEXT)
        is handle
    )
    assert client_module._HUB_HANDLES[_run_scoped_name()] == (handle, 2)

    client_module.release_monitor_hub(_run_scoped_name())
    handle.shutdown.remote.assert_not_called()
    client_module.release_monitor_hub(_run_scoped_name())
    handle.shutdown.remote.assert_called_once_with()
    assert client_module._HUB_HANDLES == {}


def test_session_mode_creation_passes_the_context_without_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_job(monkeypatch)
    monkeypatch.setattr(client_module, "_HUB_HANDLES", {})
    actor = MagicMock()
    remote = MagicMock(return_value=actor)
    options = MagicMock(return_value=MagicMock(remote=remote))
    monkeypatch.setattr(
        client_module.ray, "get_actor", MagicMock(side_effect=ValueError)
    )
    monkeypatch.setattr(client_module, "MonitorHubActor", MagicMock(options=options))
    conf = OmegaConf.create({"server": {"url": "http://server"}})

    created = client_module.get_or_create_monitor_hub(conf, _CONTEXT)

    assert created is actor
    options.assert_called_once_with(
        name=_run_scoped_name(), namespace=MonitorRayActor.NAMESPACE
    )
    remote.assert_called_once_with(conf, _CONTEXT)


def test_monitor_client_close_releases_one_reference_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_job(monkeypatch)
    monkeypatch.setattr(client_module, "_HUB_HANDLES", {})
    handle = MagicMock()
    monkeypatch.setattr(client_module.ray, "get_actor", MagicMock(return_value=handle))
    monkeypatch.setattr(
        client_module.ray, "get", MagicMock(return_value=_CONTEXT["identity"])
    )
    monkeypatch.setattr(client_module.ray, "is_initialized", lambda: True)

    first = client_module.create_ray_monitor_client(OmegaConf.create({}), _CONTEXT)
    second = client_module.create_ray_monitor_client(OmegaConf.create({}), _CONTEXT)
    assert first is not None and second is not None

    first.close()
    first.close()  # idempotent
    handle.shutdown.remote.assert_not_called()

    second.close()
    handle.shutdown.remote.assert_called_once_with()
