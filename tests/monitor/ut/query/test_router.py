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

"""Unit tests for the run-scoped Prometheus/Tempo query router (S2 §6).

The Registry and the backend HTTP session are both fakes: no socket is opened
and no Prometheus/Tempo/Grafana process is involved.
"""

from __future__ import annotations

import json
import time

import pytest
import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rl_insight.query import router as router_module
from rl_insight.query.router import create_query_router
from rl_insight.server.run_registry import RegistryError

PREFIX = "/api/v1"
REGISTRY_ERROR = 404
RUN_A = {"id": "run-a", "source": "managed", "project": "p", "experiment_name": None}
RUN_B = {"id": "run-b", "source": "managed", "project": "p", "experiment_name": None}
RUN_PLATFORM = {
    "id": "task-9",
    "source": "platform",
    "project": "p",
    "experiment_name": None,
}
KEY_A = "key-a"
KEY_B = "key-b"
KEY_PLATFORM = "key-platform"


class FakeRegistry:
    """The ``get_run`` contract of ``rl_insight.server.run_registry``."""

    def __init__(self) -> None:
        self.runs = {
            KEY_A: RUN_A,
            KEY_B: RUN_B,
            KEY_PLATFORM: RUN_PLATFORM,
        }

    def get_run(self, run_key: str) -> dict:
        if run_key not in self.runs:
            raise RegistryError(
                "RUN_NOT_FOUND", f"unknown run key {run_key!r}", status=REGISTRY_ERROR
            )
        return {"identity": self.runs[run_key], "state": "active"}


class FakeResponse:
    def __init__(self, status_code: int = 200, payload=None, content: bytes = b"{}"):
        self.status_code = status_code
        self.content = content if payload is None else json.dumps(payload).encode()
        self.headers = {"Content-Type": "application/json"}

    def json(self):
        return json.loads(self.content)


class FakeSession:
    """A requests-like session that records the request it was given."""

    def __init__(self, *responses) -> None:
        self.responses = list(responses)
        self.calls: list[dict] = []

    def request(self, method, url, params=None, headers=None, timeout=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
            }
        )
        response = self.responses.pop(0) if self.responses else FakeResponse()
        if isinstance(response, Exception):
            raise response
        return response

    @property
    def last_params(self) -> dict:
        return self.calls[-1]["params"]


@pytest.fixture()
def session() -> FakeSession:
    return FakeSession()


@pytest.fixture()
def client(session: FakeSession) -> TestClient:
    app = FastAPI()
    app.include_router(
        create_query_router(
            FakeRegistry(),
            prometheus_url="http://prometheus:9090",
            tempo_url="http://tempo:3200",
            session=session,
        ),
        prefix=PREFIX,
    )
    return TestClient(app)


def _trace_payload(*run_ids: str) -> dict:
    attribute = "rl_insight.run_id"
    return {
        "batches": [
            {
                "resource": {
                    "attributes": [{"key": attribute, "value": {"stringValue": run_id}}]
                }
            }
            for run_id in run_ids
        ]
    }


def _search_payload(*trace_ids: str) -> dict:
    """A Tempo search answer: the candidates carry no Resource of their own."""
    return {
        "traces": [
            {
                "traceID": trace_id,
                "rootServiceName": "actor",
                "rootTraceName": "generate",
                "durationMs": 10,
            }
            for trace_id in trace_ids
        ]
    }


def _query_range_url(run_key: str) -> str:
    return (
        f"{PREFIX}/runs/{run_key}/prometheus/api/v1/query_range"
        "?query=sum(rate(a[1m]))%2Fsum(rate(b[1m]))&start=1&end=2&step=15"
    )


def test_query_range_is_isolated_between_two_runs(client, session):
    assert client.get(_query_range_url(KEY_A)).status_code == 200
    first = dict(session.last_params)
    assert client.get(_query_range_url(KEY_B)).status_code == 200
    second = dict(session.last_params)

    assert first["query"] == [
        'sum(rate(a{run_id="run-a"}[1m]))/sum(rate(b{run_id="run-a"}[1m]))'
    ]
    assert second["query"] == [
        'sum(rate(a{run_id="run-b"}[1m]))/sum(rate(b{run_id="run-b"}[1m]))'
    ]
    assert not any("run-b" in value for value in first["query"])
    assert first["start"] == ["1"] and first["step"] == ["15"]
    assert session.calls[0]["url"] == "http://prometheus:9090/api/v1/query_range"


@pytest.mark.parametrize("query", ['a{run_id=~"run-.*"}'])
def test_a_conflicting_identity_selector_is_rejected(client, session, query):
    response = client.get(
        f"{PREFIX}/runs/{KEY_A}/prometheus/api/v1/query", params={"query": query}
    )
    assert response.status_code == 422
    assert response.json()["reason_code"] == "IDENTITY_SELECTOR_CONFLICT"
    assert response.json()["status"] == "error"
    assert session.calls == []


@pytest.mark.parametrize("match", ['a{run_id="run-b"}'])
def test_a_match_selector_that_cannot_be_scoped_is_rejected(client, session, match):
    response = client.get(
        f"{PREFIX}/runs/{KEY_A}/prometheus/api/v1/series", params={"match[]": match}
    )
    assert response.status_code == 422
    assert session.calls == []


def test_labels_without_match_gains_the_identity_selector(client, session):
    response = client.get(f"{PREFIX}/runs/{KEY_A}/prometheus/api/v1/labels")
    assert response.status_code == 200
    assert session.last_params["match[]"] == ['{run_id="run-a"}']

    response = client.get(
        f"{PREFIX}/runs/{KEY_A}/prometheus/api/v1/labels",
        params={"match[]": 'a{b="c"}'},
    )
    assert response.status_code == 200
    assert session.last_params["match[]"] == ['a{b="c", run_id="run-a"}']


def test_a_client_chosen_backend_or_identity_is_refused(client, session):
    upstream = client.get(
        f"{PREFIX}/runs/{KEY_A}/prometheus/api/v1/query",
        params={"query": "up", "upstream": "http://other:9090"},
    )
    assert upstream.status_code == 400
    assert upstream.json()["reason_code"] == "CLIENT_UPSTREAM_REJECTED"

    override = client.get(
        f"{PREFIX}/runs/{KEY_A}/prometheus/api/v1/query",
        params={"query": "up"},
        headers={"X-RLInsight-Run-ID": "cnVuLWI"},
    )
    assert override.status_code == 400
    assert override.json()["reason_code"] == "IDENTITY_OVERRIDE_REJECTED"
    assert session.calls == []


def test_an_unknown_run_key_keeps_the_registry_status(client, session):
    response = client.get(_query_range_url("no-such-key"))
    assert response.status_code == REGISTRY_ERROR
    assert response.json()["reason_code"] == "RUN_NOT_FOUND"
    assert session.calls == []


def test_a_backend_transport_failure_is_never_an_empty_success(client, session):
    session.responses.append(requests.ConnectionError("boom"))
    response = client.get(_query_range_url(KEY_A))
    assert response.status_code == 502
    assert response.json()["reason_code"] == "BACKEND_UNAVAILABLE"

    session.responses.append(requests.Timeout("slow"))
    response = client.get(_query_range_url(KEY_A))
    assert response.status_code == 504
    assert response.json()["reason_code"] == "BACKEND_TIMEOUT"


def test_tempo_search_is_scoped_and_its_candidates_are_verified(client, session):
    session.responses.append(FakeResponse(200, _search_payload("a" * 32)))
    session.responses.append(FakeResponse(200, _trace_payload("run-a")))
    response = client.get(
        f"{PREFIX}/runs/{KEY_A}/tempo/api/search",
        params={"q": "{}", "limit": "20"},
    )
    assert response.status_code == 200
    assert session.calls[0]["params"]["q"] == ['{resource.rl_insight.run_id = "run-a"}']
    assert session.calls[0]["params"]["limit"] == ["20"]
    assert session.calls[0]["url"] == "http://tempo:3200/api/search"
    assert session.calls[1]["url"] == f"http://tempo:3200/api/traces/{'a' * 32}"
    assert [trace["traceID"] for trace in response.json()["traces"]] == ["a" * 32]

    session.responses.append(FakeResponse(200, _search_payload()))
    response = client.get(
        f"{PREFIX}/runs/{KEY_A}/tempo/api/search",
        params={"q": '{span.a="1"} | select(span.a)'},
    )
    assert response.status_code == 200
    assert session.calls[2]["params"]["q"] == [
        '{span.a="1" && resource.rl_insight.run_id = "run-a"} | select(span.a)'
    ]


def test_a_search_candidate_of_another_run_is_not_described(client, session):
    """S2 §6.3: titles and tags of a foreign candidate must not reach the caller."""
    session.responses.append(FakeResponse(200, _search_payload("a" * 32, "b" * 32)))
    session.responses.append(FakeResponse(200, _trace_payload("run-a")))
    session.responses.append(FakeResponse(200, _trace_payload("run-b")))

    response = client.get(f"{PREFIX}/runs/{KEY_A}/tempo/api/search", params={"q": "{}"})

    assert response.status_code == 200
    assert [trace["traceID"] for trace in response.json()["traces"]] == ["a" * 32]


def test_a_search_candidate_mixing_runs_is_a_conflict(client, session):
    session.responses.append(FakeResponse(200, _search_payload("a" * 32)))
    session.responses.append(FakeResponse(200, _trace_payload("run-a", "run-b")))

    response = client.get(f"{PREFIX}/runs/{KEY_A}/tempo/api/search", params={"q": "{}"})

    assert response.status_code == 409
    assert response.json()["reason_code"] == "TRACE_ID_CONFLICT"


def test_a_search_beyond_the_verification_budget_is_refused(client, session):
    """S2 §6.3: the budget is reported, not spent on a silently shorter answer."""
    session.responses.append(
        FakeResponse(200, _search_payload(*[f"{index:032x}" for index in range(21)]))
    )
    response = client.get(f"{PREFIX}/runs/{KEY_A}/tempo/api/search", params={"q": "{}"})

    assert response.status_code == 502
    assert response.json()["reason_code"] == "SEARCH_VERIFY_BUDGET_EXCEEDED"
    assert len(session.calls) == 1  # no candidate was fetched


def test_a_search_whose_verification_times_out_is_refused(client, session, monkeypatch):
    """S2 §6.3: an exhausted time budget is an error, not a partial answer."""
    monkeypatch.setattr(router_module, "SEARCH_VERIFY_BUDGET_SECONDS", -1.0)
    session.responses.append(FakeResponse(200, _search_payload("a" * 32)))

    response = client.get(f"{PREFIX}/runs/{KEY_A}/tempo/api/search", params={"q": "{}"})

    assert response.status_code == 504
    assert response.json()["reason_code"] == "SEARCH_VERIFY_TIMEOUT"
    assert len(session.calls) == 1  # the candidate was not fetched


def test_a_candidate_that_consumed_the_budget_is_refused_not_truncated(
    client, session, monkeypatch
):
    """S2 §6.3: the remaining budget caps the request, and the budget is
    re-checked after the answer, so the last candidate cannot slip through."""
    monkeypatch.setattr(router_module, "SEARCH_VERIFY_BUDGET_SECONDS", 0.05)
    real_request = session.request

    def slow_request(method, url, params=None, headers=None, timeout=None):
        response = real_request(
            method, url, params=params, headers=headers, timeout=timeout
        )
        if "/api/traces/" in url:
            time.sleep(0.1)
        return response

    monkeypatch.setattr(session, "request", slow_request)
    session.responses.append(FakeResponse(200, _search_payload("a" * 32)))
    session.responses.append(FakeResponse(200, _trace_payload("run-a")))

    response = client.get(f"{PREFIX}/runs/{KEY_A}/tempo/api/search", params={"q": "{}"})

    assert response.status_code == 504
    assert response.json()["reason_code"] == "SEARCH_VERIFY_TIMEOUT"
    # the verification request may not outlive the budget it was given
    assert session.calls[-1]["timeout"] <= 0.05


def test_tempo_tag_values_are_derived_from_a_constrained_search(client, session):
    response = client.get(
        f"{PREFIX}/runs/{KEY_A}/tempo/api/v2/search/tag/span.turn/values"
    )
    assert response.status_code == 200
    assert session.last_params["q"] == ['{resource.rl_insight.run_id = "run-a"}']
    assert session.calls[0]["url"] == (
        "http://tempo:3200/api/v2/search/tag/span.turn/values"
    )


def test_a_global_tempo_interface_is_rejected_with_a_reason(client, session):
    for path in ("search/tags", "v2/search/tags"):
        response = client.get(f"{PREFIX}/runs/{KEY_A}/tempo/api/{path}")
        assert response.status_code == 422
        assert response.json()["reason_code"] == "UNSCOPED_TEMPO_ENDPOINT"

    # The tag-only form of the v1 search has no spanset to constrain either.
    response = client.get(
        f"{PREFIX}/runs/{KEY_A}/tempo/api/search", params={"tags": "service.name=x"}
    )
    assert response.status_code == 422
    assert response.json()["reason_code"] == "UNSCOPED_TEMPO_ENDPOINT"
    assert session.calls == []


def test_the_platform_source_uses_the_task_identity(client, session):
    session.responses.append(FakeResponse(200, _search_payload()))
    client.get(
        f"{PREFIX}/runs/{KEY_PLATFORM}/tempo/api/search",
        params={"q": '{span.a="1"}'},
    )
    assert session.last_params["q"] == [
        '{span.a="1" && resource.rl_insight.task_id = "task-9"}'
    ]

    client.get(f"{PREFIX}/runs/{KEY_PLATFORM}/prometheus/api/v1/query?query=up")
    assert session.last_params["query"] == ['up{task_id="task-9"}']


def test_a_trace_owned_by_another_run_is_not_found(client, session):
    session.responses.append(FakeResponse(200, _trace_payload("run-b")))
    response = client.get(f"{PREFIX}/runs/{KEY_A}/tempo/api/traces/{'a' * 32}")
    assert response.status_code == 404
    assert response.json()["reason_code"] == "TRACE_NOT_FOUND"


def test_a_trace_mixing_runs_is_a_conflict(client, session):
    session.responses.append(FakeResponse(200, _trace_payload("run-a", "run-b")))
    response = client.get(f"{PREFIX}/runs/{KEY_A}/tempo/api/traces/{'a' * 32}")
    assert response.status_code == 409
    assert response.json()["reason_code"] == "TRACE_ID_CONFLICT"


def test_a_trace_owned_by_this_run_is_returned_unchanged(client, session):
    payload = _trace_payload("run-a")
    session.responses.append(FakeResponse(200, payload))
    response = client.get(f"{PREFIX}/runs/{KEY_A}/tempo/api/traces/{'a' * 32}")
    assert response.status_code == 200
    assert response.json() == payload
