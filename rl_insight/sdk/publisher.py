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

"""Endpoint publisher protocol and the managed adapter (RFC v4.1 §5).

The logical interface is ``upsert(request) -> Lease``, ``renew(lease)`` and
``delete(lease)``. Managed mode talks to the RL-Insight run registry over
HTTP; platform mode uses a registered adapter or reports the channel as
unsupported. The stable registration slot is
``(source, id, target_kind, component, instance_key)`` — a port change or a new
Pod never creates a new slot.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import requests

from ..utils.constants import MonitorServer
from .identity import WILDCARD_HOSTS, RunIdentity

REQUEST_TIMEOUT_SECONDS = 3.0
# Prototype lease defaults; the server may return its own renew_after suggestion.
RENEW_AFTER_SECONDS = 10.0


def slot_key(
    identity: RunIdentity, target_kind: str, component: str, instance_key: str
) -> str:
    """Stable, filesystem- and URL-safe key for one logical endpoint slot."""
    raw = "\x00".join(
        [
            identity.source,
            identity.id,
            str(target_kind),
            str(component),
            str(instance_key),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def canonical_address(scheme: str, host: str, port: int, path: str) -> str:
    """Normalized address used for one-run ownership checks."""
    normalized_path = "/" + str(path).lstrip("/")
    return f"{scheme}://{host}:{int(port)}{normalized_path}"


class ProtocolError(RuntimeError):
    """A registry call failed with a structured protocol error."""

    def __init__(
        self,
        code: str,
        message: str = "",
        *,
        status_code: int = 0,
        retryable: bool = False,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.retryable = retryable
        self.details = dict(details or {})


@dataclass(frozen=True)
class PublicationResult:
    """Outcome of one endpoint publication."""

    accepted: bool
    code: str = ""
    message: str = ""
    retryable: bool = False
    endpoint_id: str | None = None
    lease_id: str | None = None
    revision: int | None = None
    lifecycle_epoch: int | None = None
    expires_at: float | None = None
    renew_after: float | None = None
    diagnostic: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "endpoint_id": self.endpoint_id,
            "lease_id": self.lease_id,
            "revision": self.revision,
            "lifecycle_epoch": self.lifecycle_epoch,
            "expires_at": self.expires_at,
            "renew_after": self.renew_after,
        }


@dataclass(frozen=True)
class Lease:
    """Client-side view of one owned endpoint lease."""

    lease_id: str
    endpoint_id: str
    revision: int
    lifecycle_epoch: int
    incarnation: str
    slot: str


def validate_publication_facts(facts: Mapping[str, Any]) -> dict[str, Any]:
    """Validate endpoint facts before they leave the process.

    Returns a normalized request body; raises :class:`ProtocolError` with
    ``INVALID_CONFIG`` for wildcard hosts, bad ports or missing slots.
    """
    request: dict[str, Any] = {
        "target_kind": str(facts.get("target_kind") or "").strip(),
        "component": str(facts.get("component") or "").strip(),
        "instance_key": str(facts.get("instance_key") or "").strip(),
        "incarnation": str(facts.get("incarnation") or "").strip(),
        "advertised_host": str(facts.get("advertised_host") or "").strip(),
        "scheme": str(facts.get("scheme") or "http").strip(),
        "path": str(facts.get("path") or "").strip(),
        "expected_revision": int(facts.get("expected_revision") or 0),
    }
    for key in ("target_kind", "component", "instance_key", "incarnation"):
        if not request[key]:
            raise ProtocolError("INVALID_CONFIG", f"{key} is required")
    host = request["advertised_host"]
    if not host or host in WILDCARD_HOSTS:
        raise ProtocolError(
            "INVALID_CONFIG",
            "advertised_host must be the real bound address, not a wildcard",
        )
    raw_port = facts.get("actual_port")
    if raw_port is None:
        raise ProtocolError("INVALID_CONFIG", "actual_port must be an integer")
    try:
        port = int(raw_port)
    except (TypeError, ValueError) as exc:
        raise ProtocolError("INVALID_CONFIG", "actual_port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ProtocolError("INVALID_CONFIG", "actual_port must be 1..65535")
    request["actual_port"] = port
    if request["scheme"] not in ("http", "https"):
        raise ProtocolError("INVALID_CONFIG", "scheme must be http or https")
    if request["path"] and not request["path"].startswith("/"):
        raise ProtocolError("INVALID_CONFIG", "path must start with '/'")
    for key in ("replica", "node", "pod"):
        if facts.get(key) is not None:
            request[key] = str(facts[key])
    return request


class Publisher(Protocol):
    """Adapter contract shared by managed and platform deployments."""

    def upsert(
        self, identity: RunIdentity, facts: Mapping[str, Any]
    ) -> PublicationResult:
        """Register or refresh one endpoint; the server decides the slot."""

    def delete(
        self, lease: Lease, *, timeout: float | None = None
    ) -> PublicationResult:
        """Best-effort deregistration of one owned lease.

        ``timeout`` is the caller's remaining shutdown budget: the transport may
        not outlive it. ``None`` keeps the adapter's own default.
        """

    def renew(self, lease: Lease) -> PublicationResult:
        """Extend one owned lease; the server may suggest a new interval."""


# Platform adapters are registered in source at import time: no plugin discovery,
# no arbitrary Python class paths. ``open_session`` and ``attach`` resolve the
# same factory for one adapter name (S3 §8.2).
PublisherFactory = Callable[..., Publisher]
_ADAPTER_FACTORIES: dict[str, PublisherFactory] = {}


def register_publisher_adapter(name: str, factory: PublisherFactory) -> None:
    """Register the factory for one ``publisher.adapter`` name."""
    _ADAPTER_FACTORIES[str(name)] = factory


def create_publisher_adapter(
    adapter: str | None, **kwargs: Any
) -> Publisher | UnsupportedPublisher:
    """Build the registered adapter, or an explicit unsupported publisher."""
    factory = _ADAPTER_FACTORIES.get(str(adapter or ""))
    if factory is None:
        return UnsupportedPublisher("PUBLISHER_ADAPTER_NOT_REGISTERED")
    return factory(**kwargs)


class UnsupportedPublisher:
    """Publisher used when no adapter is available: explicit, never silent."""

    def __init__(self, reason: str = "NO_PUBLISHER") -> None:
        self.reason = reason

    def upsert(
        self, identity: RunIdentity, facts: Mapping[str, Any]
    ) -> PublicationResult:
        return PublicationResult(
            accepted=False,
            code="UNSUPPORTED_CAPABILITY",
            message="no endpoint publisher adapter is configured",
            diagnostic=self.reason,
        )

    def delete(
        self, lease: Lease, *, timeout: float | None = None
    ) -> PublicationResult:
        return PublicationResult(
            accepted=False, code="UNSUPPORTED_CAPABILITY", message=self.reason
        )

    def renew(self, lease: Lease) -> PublicationResult:
        return PublicationResult(
            accepted=False, code="UNSUPPORTED_CAPABILITY", message=self.reason
        )


class ServerClient:
    """Thin HTTP client for the managed run/endpoint protocol."""

    def __init__(
        self,
        base_url: str,
        *,
        session: requests.Session | None = None,
        timeout: float = REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout

    # -- run lifecycle -------------------------------------------------
    def create_run(
        self,
        *,
        project: str,
        experiment_name: str | None,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/runs",
            json_body={"project": project, "experiment_name": experiment_name},
            headers={"Idempotency-Key": idempotency_key},
            expected=(200, 201),
        )

    def get_run(self, run_key: str) -> dict[str, Any]:
        return self._request("GET", f"/runs/{run_key}")

    def resume_run(
        self, run_key: str, *, project: str, expected_revision: int
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/runs/{run_key}/resume",
            json_body={"project": project, "expected_revision": expected_revision},
        )

    # -- discovery -----------------------------------------------------
    def services(self) -> dict[str, Any]:
        """Service discovery payload: capabilities, readiness and addresses."""
        return self._request("GET", "/services")

    # -- endpoint leases -----------------------------------------------
    def upsert_endpoint(
        self, run_key: str, slot: str, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        return self._request(
            "PUT",
            f"/runs/{run_key}/endpoints/{slot}",
            json_body=dict(payload),
            expected=(200, 201),
        )

    def renew_lease(
        self, run_key: str, lease_id: str, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/runs/{run_key}/leases/{lease_id}/renew",
            json_body=dict(payload),
        )

    def delete_lease(
        self,
        run_key: str,
        lease_id: str,
        payload: Mapping[str, Any],
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        return self._request(
            "DELETE",
            f"/runs/{run_key}/leases/{lease_id}",
            json_body=dict(payload),
            timeout=timeout,
        )

    # -- transport -----------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        expected: tuple[int, ...] = (200,),
        timeout: float | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{MonitorServer.API_PREFIX}{path}"
        try:
            response = self.session.request(
                method,
                url,
                json=None if json_body is None else dict(json_body),
                headers={"Content-Type": "application/json", **dict(headers or {})},
                timeout=self.timeout if timeout is None else max(0.0, float(timeout)),
            )
        except requests.RequestException as exc:
            raise ProtocolError(
                "BACKEND_UNAVAILABLE", str(exc), retryable=True
            ) from exc
        if response.status_code not in expected:
            raise _protocol_error(response)
        try:
            data = response.json()
        except ValueError as exc:
            raise ProtocolError(
                "MALFORMED_RESPONSE", "server returned non-JSON body"
            ) from exc
        if not isinstance(data, dict):
            raise ProtocolError("MALFORMED_RESPONSE", "server returned a non-object")
        return data


class ManagedPublisher:
    """Publisher adapter for the managed run registry."""

    def __init__(self, client: ServerClient, identity: RunIdentity) -> None:
        self.client = client
        self.identity = identity

    def upsert(
        self, identity: RunIdentity, facts: Mapping[str, Any]
    ) -> PublicationResult:
        request = validate_publication_facts(facts)
        slot = slot_key(
            identity,
            request["target_kind"],
            request["component"],
            request["instance_key"],
        )
        try:
            data = self.client.upsert_endpoint(identity.run_key, slot, request)
        except ProtocolError as exc:
            return PublicationResult(
                accepted=False,
                code=exc.code,
                message=exc.message,
                retryable=exc.retryable,
            )
        return PublicationResult(
            accepted=True,
            endpoint_id=data.get("endpoint_id"),
            lease_id=data.get("lease_id"),
            revision=data.get("revision"),
            lifecycle_epoch=data.get("lifecycle_epoch"),
            expires_at=data.get("expires_at"),
            renew_after=data.get("renew_after"),
        )

    def delete(
        self, lease: Lease, *, timeout: float | None = None
    ) -> PublicationResult:
        payload = {
            "revision": lease.revision,
            "lifecycle_epoch": lease.lifecycle_epoch,
            "incarnation": lease.incarnation,
        }
        try:
            data = self.client.delete_lease(
                self.identity.run_key, lease.lease_id, payload, timeout=timeout
            )
        except ProtocolError as exc:
            return PublicationResult(
                accepted=False,
                code=exc.code,
                message=exc.message,
                retryable=exc.retryable,
            )
        return PublicationResult(
            accepted=bool(data.get("deleted")),
            endpoint_id=data.get("endpoint_id"),
            lease_id=lease.lease_id,
        )

    def renew(self, lease: Lease) -> PublicationResult:
        payload = {"incarnation": lease.incarnation, "revision": lease.revision}
        try:
            data = self.client.renew_lease(
                self.identity.run_key, lease.lease_id, payload
            )
        except ProtocolError as exc:
            return PublicationResult(
                accepted=False,
                code=exc.code,
                message=exc.message,
                retryable=exc.retryable,
            )
        return PublicationResult(
            accepted=True,
            endpoint_id=data.get("endpoint_id"),
            lease_id=lease.lease_id,
            revision=data.get("revision"),
            lifecycle_epoch=data.get("lifecycle_epoch"),
            expires_at=data.get("expires_at"),
            renew_after=data.get("renew_after"),
        )


def _protocol_error(response: requests.Response) -> ProtocolError:
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    if not isinstance(payload, Mapping):
        payload = {}
    return ProtocolError(
        str(payload.get("code") or f"HTTP_{response.status_code}"),
        str(payload.get("message") or response.text[:200]),
        status_code=response.status_code,
        retryable=bool(payload.get("retryable")),
        details=payload.get("details")
        if isinstance(payload.get("details"), Mapping)
        else None,
    )
