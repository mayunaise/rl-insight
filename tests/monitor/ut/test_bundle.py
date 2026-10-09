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

"""Unit tests for single-run bundle export and import (S1 §8)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from rl_insight.archive.barrier import capture_boundary, reconcile
from rl_insight.archive.records import MetricRecord, SpanRecord, encode_value
from rl_insight.archive.writer import ArchiveStore
from rl_insight.bundle import (
    MANIFEST_NAME,
    PATH_TRAVERSAL,
    RUN_CONFLICT,
    UNSUPPORTED_FORMAT,
    export_bundle,
    import_bundle,
)
from rl_insight.sdk.identity import RunIdentity
from rl_insight.server.run_registry import RegistryError, RunRegistry

SERVER_INSTANCE = "server-1"


def _identity(run_id: str = "run-a") -> RunIdentity:
    return RunIdentity(
        id=run_id, source="managed", project="proj", experiment_name="exp"
    )


def _metric(value: float, stamp: int) -> MetricRecord:
    return MetricRecord(
        labels={"run_id": "run-a", "job": "trainer", "replica": "0"},
        timestamp_ms=stamp,
        value_bits=encode_value(value),
    )


def _span(span_id: str, payload: str = "Zm9v") -> SpanRecord:
    return SpanRecord(trace_id="aa" * 16, span_id=span_id, payload_b64=payload)


class _Source:
    """Source-side fixture: what the run was supposed to archive."""

    def __init__(self) -> None:
        self.metrics = [_metric(1.0, 10), _metric(2.0, 20)]
        self.spans = [_span("01" * 8)]


def _populate(
    tmp_path: Path, identity: RunIdentity
) -> tuple[ArchiveStore, _Source, dict]:
    store = ArchiveStore(tmp_path / "data")
    source = _Source()
    store.append(identity, "metrics", source.metrics)
    store.append(identity, "traces", source.spans)
    store.seal(identity, "metrics")

    registry = RunRegistry(
        tmp_path / "data" / "registry.sqlite", server_instance_id=SERVER_INSTANCE
    )
    run = registry.create_run(project="proj")
    # the fixture run is the one the store archived under
    run["run_key"] = identity.run_key
    barriers = {
        "metrics": capture_boundary(
            source.metrics,
            source_generation="gen-1",
            committed_frontier=2,
            captured_at=1.0,
        ),
        "traces": capture_boundary(
            source.spans,
            source_generation="gen-1",
            committed_frontier=1,
            captured_at=1.0,
            supported_types=("span",),
        ),
    }
    reconciliations = {
        "metrics": reconcile(
            barriers["metrics"], expected=source.metrics, received=source.metrics
        ),
        "traces": reconcile(
            barriers["traces"], expected=source.spans, received=source.spans
        ),
    }
    return store, source, {"barriers": barriers, "reconciliations": reconciliations}


def _export(tmp_path: Path, *, prepare: Any = None, **overrides):
    identity = _identity()
    store, source, fixtures = _populate(tmp_path, identity)
    if prepare is not None:
        prepare(store, identity)
    registry_run = {"server_instance_id": SERVER_INSTANCE, "run_key": identity.run_key}
    kwargs = {
        "store": store,
        "identity": identity,
        "registry_run": registry_run,
        "dest_root": tmp_path / "out",
        "boundaries": fixtures["barriers"],
        "reconciliations": fixtures["reconciliations"],
        "accept_seq": {"metrics": 2, "traces": 1},
    }
    kwargs.update(overrides)
    return store, source, export_bundle(**kwargs)


def _manifest(bundle: Path) -> dict:
    return json.loads((bundle / MANIFEST_NAME).read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- export


def test_export_publishes_a_complete_bundle_atomically(tmp_path: Path):
    _, _, result = _export(tmp_path)
    assert result.ok is True
    assert result.complete is True
    bundle = Path(result.path)
    assert bundle.is_dir()
    assert not (tmp_path / "out" / "staging").exists() or not list(
        (tmp_path / "out" / "staging").iterdir()
    )

    manifest = _manifest(bundle)
    assert manifest["schema_version"] == "0.1-prototype"
    assert manifest["identity"]["id"] == "run-a"
    assert manifest["complete"] is True
    assert manifest["streams"]["metrics"]["records"] == 2
    assert manifest["streams"]["metrics"]["boundary"]["committed_frontier"] == 2
    assert set(manifest["files"]) == {
        "records/metrics.jsonl",
        "records/traces.jsonl",
        "targets/history.json",
    }
    for entry in manifest["files"].values():
        assert entry["size"] > 0 and len(entry["sha256"]) == 64


def test_an_unproven_barrier_blocks_a_complete_export(tmp_path: Path):
    _, _, result = _export(tmp_path, reconciliations=_failed_reconciliation())
    assert result.ok is False
    assert result.complete is False
    assert any(
        "INCOMPLETE" in reason or "MISSING" in reason for reason in result.reasons
    )
    assert not (tmp_path / "out" / "bundles").exists()


def test_a_partial_export_must_be_requested_and_carries_its_reasons(tmp_path: Path):
    _, _, result = _export(
        tmp_path,
        reconciliations=_failed_reconciliation(),
        allow_partial=True,
    )
    assert result.ok is True
    assert result.complete is False
    assert result.reasons
    assert _manifest(Path(result.path))["complete"] is False


def test_an_archived_conflict_blocks_a_complete_export(tmp_path: Path):
    """S1 §4.3: a recorded conflict is not a complete export, barrier or not."""

    def conflict(store: ArchiveStore, identity: RunIdentity) -> None:
        store.append(identity, "metrics", [_metric(9.0, 10)])

    _, _, blocked = _export(tmp_path, prepare=conflict)
    assert blocked.ok is False
    assert "metrics:RECORD_CONFLICTS" in blocked.reasons
    assert not (tmp_path / "out" / "bundles").exists()

    _, _, partial = _export(tmp_path, prepare=conflict, allow_partial=True)
    assert partial.complete is False
    assert _manifest(Path(partial.path))["streams"]["metrics"]["complete"] is False


def test_the_bundle_never_carries_leases_epochs_or_pids(tmp_path: Path):
    _, _, result = _export(tmp_path)
    bundle = Path(result.path)
    text = (bundle / "targets" / "history.json").read_text(encoding="utf-8")
    for forbidden in ("lease", "epoch", "incarnation", "pid", "expires_at"):
        assert forbidden not in text.lower()
    assert json.loads(text)["endpoints"] == []


def test_export_does_not_stop_the_other_stream_or_the_run(tmp_path: Path):
    store, _, result = _export(tmp_path)
    identity = _identity()
    # the run keeps accepting writes after the snapshot
    assert store.append(identity, "metrics", [_metric(3.0, 30)]).accepted == 1
    assert store.coverage(identity, "metrics")["last_seq"] == 3
    assert result.complete is True


# --------------------------------------------------------------------------- import


def test_import_installs_the_original_id_as_archived_and_stays_readable(tmp_path: Path):
    _, source, result = _export(tmp_path)
    bundle = Path(result.path)

    target = tmp_path / "target"
    store = ArchiveStore(target)
    registry = RunRegistry(target / "registry.sqlite", server_instance_id="server-2")
    imported = import_bundle(bundle, store=store, registry=registry)

    assert imported.ok is True
    assert imported.run_id == "run-a"
    assert imported.state == "archived"
    assert imported.records == 3

    identity = _identity()
    assert [record.value_bits for record in store.read(identity, "metrics")] == [
        record.value_bits for record in source.metrics
    ]
    assert store.read(identity, "traces") == source.spans
    stored = registry.get_run(identity.run_key)["state"]
    assert stored == "archived"
    with pytest.raises(RegistryError):
        registry.writable_identity(identity.run_key)


def test_importing_the_same_id_twice_is_refused_and_changes_nothing(tmp_path: Path):
    _, _, result = _export(tmp_path)
    target = tmp_path / "target"
    store = ArchiveStore(target)
    registry = RunRegistry(target / "registry.sqlite", server_instance_id="server-2")
    assert import_bundle(Path(result.path), store=store, registry=registry).ok is True

    again = import_bundle(Path(result.path), store=store, registry=registry)
    assert again.ok is False
    assert again.code == RUN_CONFLICT
    assert len(registry.list_runs()) == 1


def test_a_corrupted_payload_is_rejected_before_the_run_becomes_visible(tmp_path: Path):
    _, _, result = _export(tmp_path)
    bundle = Path(result.path)
    (bundle / "records" / "metrics.jsonl").write_text(
        '{"seq": 1, "run_id": "run-a", "record": {}}\n'
    )

    store, registry, failure = _import_into_new_instance(tmp_path, bundle)
    assert failure.ok is False
    assert failure.code == "HASH_MISMATCH"
    assert registry.list_runs() == []
    assert not store.run_dir(_identity()).exists()


def test_a_bundle_with_another_runs_records_is_rejected(tmp_path: Path):
    _, _, result = _export(tmp_path)
    bundle = Path(result.path)
    metrics = bundle / "records" / "metrics.jsonl"
    rows = [json.loads(line) for line in metrics.read_text().splitlines()]
    for row in rows:
        row["run_id"] = "run-b"
    metrics.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
    _rewrite_manifest_for(bundle, metrics)

    store, registry, failure = _import_into_new_instance(tmp_path, bundle)
    assert failure.ok is False
    assert failure.code == "MIXED_IDENTITY"
    assert registry.list_runs() == []


def test_a_path_traversal_member_is_rejected(tmp_path: Path):
    _, _, result = _export(tmp_path)
    bundle = Path(result.path)
    manifest = _manifest(bundle)
    manifest["files"]["../escape.txt"] = {"size": 1, "sha256": "0" * 64}
    (bundle / MANIFEST_NAME).write_text(json.dumps(manifest, sort_keys=True))

    _, registry, failure = _import_into_new_instance(tmp_path, bundle)
    assert failure.ok is False
    assert failure.code == PATH_TRAVERSAL
    assert registry.list_runs() == []


def test_an_unknown_schema_version_is_rejected(tmp_path: Path):
    _, _, result = _export(tmp_path)
    bundle = Path(result.path)
    manifest = _manifest(bundle)
    manifest["schema_version"] = "1.0-final"
    (bundle / MANIFEST_NAME).write_text(json.dumps(manifest, sort_keys=True))

    _, registry, failure = _import_into_new_instance(tmp_path, bundle)
    assert failure.ok is False
    assert failure.code == UNSUPPORTED_FORMAT
    assert registry.list_runs() == []


def test_import_does_not_register_the_imported_target_history(tmp_path: Path):
    _, _, result = _export(tmp_path)
    target = tmp_path / "target"
    store = ArchiveStore(target)
    registry = RunRegistry(target / "registry.sqlite", server_instance_id="server-2")
    import_bundle(Path(result.path), store=store, registry=registry)
    # history metadata explains the source but must never re-activate scraping
    assert registry.endpoints_for(_identity().run_key) == []


def _failed_reconciliation():
    metrics = [_metric(1.0, 10), _metric(2.0, 20)]
    barrier = capture_boundary(
        metrics, source_generation="gen-1", committed_frontier=2, captured_at=1.0
    )
    return {"metrics": reconcile(barrier, expected=metrics, received=[metrics[0]])}


def _import_into_new_instance(tmp_path: Path, bundle: Path):
    target = tmp_path / "target"
    store = ArchiveStore(target)
    registry = RunRegistry(target / "registry.sqlite", server_instance_id="server-2")
    return store, registry, import_bundle(bundle, store=store, registry=registry)


def _rewrite_manifest_for(bundle: Path, member: Path) -> None:
    import hashlib

    manifest = _manifest(bundle)
    relative = str(member.relative_to(bundle))
    payload = member.read_bytes()
    manifest["files"][relative] = {
        "size": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    (bundle / MANIFEST_NAME).write_text(json.dumps(manifest, sort_keys=True))
