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

"""Run identity and context v1 (RFC v4.1 §2, S0 design §3.1).

``RunIdentity.id`` is the single observation key: ``run_id`` in managed mode and
``task_id`` in platform mode. ``project`` / ``experiment_name`` only group and
label data; they never decide uniqueness. IDs are opaque: stored exactly as
given, never trimmed, lower-cased or Unicode-normalized.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

MANAGED = "managed"
PLATFORM = "platform"
SOURCES = (MANAGED, PLATFORM)

CONTEXT_VERSION = 1
MAX_ID_BYTES = 256
MAX_NAME_BYTES = 200
SLUG_MAX_CHARS = 48
SLUG_DEFAULT = "x"
UNCATEGORIZED = "uncategorized"

METRIC_LABEL = {MANAGED: "run_id", PLATFORM: "task_id"}
TRACE_ATTRIBUTE = {MANAGED: "rl_insight.run_id", PLATFORM: "rl_insight.task_id"}

# Bind addresses and wildcards that must never be used as an advertised address.
WILDCARD_HOSTS = ("0.0.0.0", "::", "[::]", "*")

# Identity headers carried by managed traffic: the OTLP request headers, the
# metrics response headers checked by the controlled scrape route, and the OTLP
# epoch/instance headers (S2 §4.2).
RUN_ID_HEADER = "X-RLInsight-Run-ID"
EPOCH_HEADER = "X-RLInsight-Epoch"
INCARNATION_HEADER = "X-RLInsight-Incarnation"
SERVER_INSTANCE_HEADER = "X-RLInsight-Server-Instance"


class IdentityError(ValueError):
    """Raised for an invalid run identity."""


class ContextError(ValueError):
    """Raised for a malformed or unsupported SDK context."""


def validate_run_id(value: Any) -> str:
    """Return the ID unchanged, or raise.

    A v1 ID is 1-256 UTF-8 bytes with no control characters. Everything else is
    returned as-is: no trimming, case folding or Unicode normalization.
    """
    if not isinstance(value, str):
        raise IdentityError("run id must be a string")
    if not value:
        raise IdentityError("run id must not be empty")
    if len(value.encode("utf-8")) > MAX_ID_BYTES:
        raise IdentityError(f"run id exceeds {MAX_ID_BYTES} UTF-8 bytes")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        raise IdentityError("run id must not contain control characters")
    return value


def normalize_project(value: Any) -> str:
    """Return a validated project name; empty means ``uncategorized``."""
    project = str(value or "").strip()
    return validate_identity_name(project, "project") if project else UNCATEGORIZED


def normalize_experiment_name(value: Any) -> str | None:
    """Return a validated experiment name or ``None``; the empty value is kept empty."""
    name = str(value or "").strip()
    return validate_identity_name(name, "experiment_name") if name else None


def validate_identity_name(value: Any, field_name: str) -> str:
    """Trim and validate one display name (project / experiment_name).

    Display names are not the run ID: they are trimmed and bounded, and they
    never decide run uniqueness (S0 §3.1). The run ID keeps its own rules in
    :func:`validate_run_id`.
    """
    name = str(value or "").strip()
    if not name:
        raise IdentityError(f"{field_name} must be a non-empty name")
    if len(name.encode("utf-8")) > MAX_NAME_BYTES:
        raise IdentityError(f"{field_name} exceeds {MAX_NAME_BYTES} UTF-8 bytes")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in name):
        raise IdentityError(f"{field_name} must not contain control characters")
    return name


def _slug(raw: str) -> str:
    """Return a readable, cross-platform-safe slug; uniqueness comes from the hash."""
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "-", raw).strip("-")
    cleaned = re.sub(r"-{2,}", "-", cleaned)[:SLUG_MAX_CHARS].strip("-")
    return cleaned or SLUG_DEFAULT


def safe_directory_segment(prefix: str, raw: str) -> str:
    """Build ``<prefix>-<slug>--<sha256>`` from a validated display name.

    The hash is computed over the full raw UTF-8 name, so names that differ only
    in case (``Project`` vs ``project``) never collide on case-insensitive
    filesystems. The slug is decorative and never part of uniqueness; the raw
    name never reaches the filesystem.
    """
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{prefix}-{_slug(raw)}--{digest}"


def run_key(source: str, run_id: str) -> str:
    """Irreversible URL- and path-safe key for one run.

    The raw ID may contain ``/`` or Unicode and must never be spliced into a
    path segment (S0 §3.1); the Registry keeps the ID -> key mapping.
    """
    return hashlib.sha256(f"{source}\x00{run_id}".encode()).hexdigest()


def encode_run_id_header(run_id: Any) -> str:
    """UTF-8 base64url without padding: the wire form of every identity header."""
    return (
        base64.urlsafe_b64encode(validate_run_id(run_id).encode("utf-8"))
        .decode("ascii")
        .rstrip("=")
    )


def decode_run_id_header(value: str) -> str:
    """Decode one identity header; the decoded value is still an opaque ID."""
    padding = "=" * (-len(value) % 4)
    try:
        decoded = base64.urlsafe_b64decode(value + padding).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise IdentityError(
            f"identity header is not valid base64url UTF-8: {exc}"
        ) from exc
    return validate_run_id(decoded)


@dataclass(frozen=True)
class RunIdentity:
    """Immutable identity shared by every component of one observation run."""

    id: str
    source: str
    project: str = UNCATEGORIZED
    experiment_name: str | None = None

    def __post_init__(self) -> None:
        validate_run_id(self.id)
        if self.source not in SOURCES:
            raise IdentityError(f"unsupported identity source {self.source!r}")
        object.__setattr__(self, "project", normalize_project(self.project))
        object.__setattr__(
            self, "experiment_name", normalize_experiment_name(self.experiment_name)
        )

    @property
    def display_name(self) -> str:
        """Human label: the experiment name, else ``run-<first 8 of the id>``."""
        return self.experiment_name or f"run-{self.id[:8]}"

    @property
    def metrics_label(self) -> str:
        """Authoritative target label carrying the identity key."""
        return METRIC_LABEL[self.source]

    @property
    def trace_attribute(self) -> str:
        """OTLP Resource attribute carrying the identity key."""
        return TRACE_ATTRIBUTE[self.source]

    @property
    def target_labels(self) -> dict[str, str]:
        """Labels the server injects into scrape targets for this run."""
        return {
            self.metrics_label: self.id,
            "project": self.project,
            "experiment_name": self.experiment_name or "",
        }

    @property
    def run_key(self) -> str:
        """URL- and filesystem-safe key for this run; the Registry maps it back."""
        return run_key(self.source, self.id)

    @property
    def project_key(self) -> str:
        """Filesystem-safe directory segment for this project."""
        return safe_directory_segment("project", self.project)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "project": self.project,
            "experiment_name": self.experiment_name,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RunIdentity:
        if not isinstance(data, Mapping):
            raise IdentityError("run identity must be an object")
        missing = [key for key in ("id", "source") if key not in data]
        if missing:
            raise IdentityError(f"run identity is missing {', '.join(missing)}")
        return cls(
            id=data["id"],
            source=data["source"],
            project=data.get("project", UNCATEGORIZED),
            experiment_name=data.get("experiment_name"),
        )


def platform_identity(
    task_id: Any, project: Any = "", experiment_name: Any = ""
) -> RunIdentity:
    """Wrap an externally injected ``task_id``; no second ID is generated."""
    return RunIdentity(
        id=validate_run_id(task_id),
        source=PLATFORM,
        project=project,
        experiment_name=experiment_name,
    )


@dataclass(frozen=True)
class Context:
    """Serializable session context handed from the driver to every worker.

    Only value types cross the process boundary: no connections, threads,
    tokens or environment dumps.
    """

    identity: RunIdentity
    services: Mapping[str, str] = field(default_factory=dict)
    capabilities: tuple[str, ...] = ()
    publisher: Mapping[str, Any] = field(default_factory=dict)
    server_instance_id: str | None = None
    lifecycle_epoch: int | None = None
    context_version: int = CONTEXT_VERSION

    def __post_init__(self) -> None:
        if self.context_version != CONTEXT_VERSION:
            raise ContextError(
                f"unsupported context version {self.context_version!r}; "
                f"this SDK speaks {CONTEXT_VERSION}"
            )
        if not isinstance(self.identity, RunIdentity):
            raise ContextError("context identity must be a RunIdentity")
        for url in self.services.values():
            if not isinstance(url, str):
                raise ContextError("context service entries must be strings")

    def to_dict(self) -> dict[str, Any]:
        return {
            "context_version": self.context_version,
            "identity": self.identity.to_dict(),
            "services": dict(self.services),
            "capabilities": list(self.capabilities),
            "publisher": dict(self.publisher),
            "server_instance_id": self.server_instance_id,
            "lifecycle_epoch": self.lifecycle_epoch,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Context:
        if not isinstance(data, Mapping):
            raise ContextError("context must be an object")
        version = data.get("context_version", CONTEXT_VERSION)
        if version != CONTEXT_VERSION:
            raise ContextError(f"unsupported context version {version!r}")
        if "identity" not in data:
            raise ContextError("context is missing identity")
        return cls(
            identity=RunIdentity.from_dict(data["identity"]),
            services=dict(data.get("services") or {}),
            capabilities=tuple(data.get("capabilities") or ()),
            publisher=dict(data.get("publisher") or {}),
            server_instance_id=data.get("server_instance_id"),
            lifecycle_epoch=data.get("lifecycle_epoch"),
            context_version=version,
        )

    @classmethod
    def from_json(cls, raw: str) -> Context:
        try:
            data = json.loads(raw)
        except ValueError as exc:
            raise ContextError(f"context is not valid JSON: {exc}") from exc
        return cls.from_dict(data)
