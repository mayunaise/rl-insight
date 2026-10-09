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

"""Unit tests for Grafana dashboard staging and template datasource binding."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator

import pytest
from omegaconf import OmegaConf

from rl_insight.server import runtime as runtime_module
from rl_insight.utils.constants import MonitorPaths

# The plugin family each template must bind to its own run-scoped variable.
DATASOURCE_VARIABLES = {
    "prometheus": "${metrics_datasource}",
    "tempo": "${traces_datasource}",
}
DASHBOARD_FILES = sorted(MonitorPaths.GRAFANA_DASHBOARDS_DIR.rglob("*.json"))


def _spec(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))["spec"]


def _data_queries(node: Any) -> Iterator[dict]:
    """Yield every ``DataQueryKind`` object nested anywhere under ``node``.

    Panels, variables and annotations all carry their plugin family on
    ``group``; there is no other place a datasource can be selected.
    """
    if isinstance(node, dict):
        if node.get("kind") == "DataQuery":
            yield node
        for value in node.values():
            yield from _data_queries(value)
    elif isinstance(node, list):
        for value in node:
            yield from _data_queries(value)


def _conf(builtin: Path, extra: Path | None = None):
    grafana = {"dashboards_dir": str(builtin)}
    if extra is not None:
        grafana["extra_dashboard_dir"] = str(extra)
    return OmegaConf.create({"grafana": grafana})


def test_stage_copies_builtin_and_extra_without_parsing_json(tmp_path) -> None:
    builtin = tmp_path / "builtin"
    extra = tmp_path / "extra"
    (builtin / "verl").mkdir(parents=True)
    (extra / "custom").mkdir(parents=True)
    (builtin / "verl" / "board.json").write_text("{}", encoding="utf-8")
    (extra / "custom" / "router.json").write_text("not valid json", encoding="utf-8")

    staged = runtime_module._stage_grafana_dashboards(
        _conf(builtin, extra), tmp_path / "runtime"
    )

    assert (staged / "verl" / "board.json").is_file()
    assert (staged / "custom" / "router.json").read_text(encoding="utf-8") == (
        "not valid json"
    )


def test_stage_rejects_json_collision_before_copying_extra(tmp_path) -> None:
    builtin = tmp_path / "builtin"
    extra = tmp_path / "extra"
    (builtin / "shared").mkdir(parents=True)
    (extra / "shared").mkdir(parents=True)
    (builtin / "shared" / "board.json").write_text("builtin", encoding="utf-8")
    (extra / "unique.json").write_text("extra", encoding="utf-8")
    (extra / "shared" / "board.json").write_text("conflict", encoding="utf-8")

    runtime_dir = tmp_path / "runtime"
    with pytest.raises(RuntimeError, match="already exists in runtime dashboards"):
        runtime_module._stage_grafana_dashboards(
            _conf(builtin, extra),
            runtime_dir,
        )

    staged = runtime_dir / "dashboards"
    assert (staged / "shared" / "board.json").read_text(encoding="utf-8") == "builtin"
    assert not (staged / "unique.json").exists()


def test_stage_refreshes_extra_in_shared_builtin_directory(tmp_path) -> None:
    builtin = tmp_path / "builtin"
    extra = tmp_path / "extra"
    (builtin / "shared").mkdir(parents=True)
    (extra / "shared").mkdir(parents=True)
    (builtin / "shared" / "builtin.json").write_text("builtin", encoding="utf-8")
    extra_dashboard = extra / "shared" / "extra.json"
    extra_dashboard.write_text("first", encoding="utf-8")
    runtime_dir = tmp_path / "runtime"

    runtime_module._stage_grafana_dashboards(_conf(builtin, extra), runtime_dir)
    extra_dashboard.write_text("second", encoding="utf-8")
    staged = runtime_module._stage_grafana_dashboards(
        _conf(builtin, extra), runtime_dir
    )

    assert (staged / "shared" / "extra.json").read_text(encoding="utf-8") == "second"


@pytest.mark.parametrize(
    ("entry_type", "expected_error"),
    [("missing", "does not exist"), ("file", "is not a directory")],
)
def test_stage_rejects_invalid_extra_directory(
    tmp_path, entry_type: str, expected_error: str
) -> None:
    builtin = tmp_path / "builtin"
    builtin.mkdir()
    extra = tmp_path / entry_type
    if entry_type == "file":
        extra.write_text("not a directory", encoding="utf-8")

    with pytest.raises(RuntimeError, match=expected_error):
        runtime_module._stage_grafana_dashboards(
            _conf(builtin, extra), tmp_path / "runtime"
        )


@pytest.mark.parametrize("path", DASHBOARD_FILES, ids=lambda path: path.stem)
def test_every_query_binds_the_variable_of_its_plugin(path: Path) -> None:
    bound = {
        (query["group"], (query.get("datasource") or {}).get("name"))
        for query in _data_queries(_spec(path))
        if query.get("group") in DATASOURCE_VARIABLES
    }

    assert bound == set(DATASOURCE_VARIABLES.items())


@pytest.mark.parametrize("path", DASHBOARD_FILES, ids=lambda path: path.stem)
def test_every_template_declares_both_datasource_variables(path: Path) -> None:
    declared = {
        variable["spec"]["name"]: variable["spec"]["pluginId"]
        for variable in _spec(path)["variables"]
        if variable.get("kind") == "DatasourceVariable"
    }

    assert declared == {
        "metrics_datasource": "prometheus",
        "traces_datasource": "tempo",
    }
