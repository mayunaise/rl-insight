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

"""Installation and real process lifecycle regressions across platforms."""

import io
import json
import os
import sys
import tarfile
import time
import zipfile

import pytest
from omegaconf import OmegaConf

from rl_insight.server.dependencies import DependencyManager
from rl_insight.server.installer import ServiceInstaller
from rl_insight.server.runtime import (
    LocalServiceRuntime,
    _spawn_service,
    _terminate_process,
    is_process_running,
    load_active_state,
)
from rl_insight.utils.monitor_config_loader import load_server_config_file


@pytest.mark.parametrize(
    "system,arch", [("Linux", "aarch64"), ("Linux", "x86_64"), ("Windows", "AMD64")]
)
def test_release_archives(monkeypatch, tmp_path, system, arch):
    monkeypatch.setattr("platform.system", lambda: system)
    monkeypatch.setattr("platform.machine", lambda: arch)
    manager = DependencyManager(load_server_config_file(), tmp_path)
    plans = manager.plan_install(targets=["prometheus", "tempo", "grafana"])
    for plan in plans:
        extension = (
            ".zip" if system == "Windows" and plan["name"] != "tempo" else ".tar.gz"
        )
        assert plan["asset"].endswith(extension)
        assert plan["url"].endswith(plan["asset"])
        assert system.lower() in plan["asset"]


@pytest.mark.parametrize("name", ["prometheus", "grafana"])
@pytest.mark.parametrize("suffix", [".zip", ".download"])
def test_zip_install_and_binary_discovery(tmp_path, name, suffix):
    archive_path = tmp_path / ("service" + suffix)
    executable = "prometheus.exe" if os.name == "nt" and name == "prometheus" else name
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("package/" + executable, b"binary")
    target = tmp_path / "install with spaces"
    ServiceInstaller._extract_archive(archive_path, target)
    (target / "package" / executable).chmod(0o755)
    assert (
        DependencyManager._find_binary_under(name, target)
        == target / "package" / executable
    )


def test_zip_rejects_parent_paths(tmp_path):
    archive_path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("../outside", b"unsafe")
    with pytest.raises(RuntimeError, match="Unsafe archive member"):
        ServiceInstaller._extract_archive(archive_path, tmp_path / "install")
    assert not (tmp_path / "outside").exists()


@pytest.mark.parametrize("member_name", ["package/tempo", "../outside"])
def test_tar_content_detection_and_path_validation(tmp_path, member_name):
    archive_path = tmp_path / "service.download"
    with tarfile.open(archive_path, "w:gz") as archive:
        member = tarfile.TarInfo(member_name)
        member.size = 6
        archive.addfile(member, io.BytesIO(b"binary"))
    target = tmp_path / "install"
    if member_name == "../outside":
        with pytest.raises(RuntimeError, match="Unsafe archive member"):
            ServiceInstaller._extract_archive(archive_path, target)
        assert not (tmp_path / "outside").exists()
    else:
        ServiceInstaller._extract_archive(archive_path, target)
        assert (target / member_name).read_bytes() == b"binary"


def test_custom_download_template_preserves_platform_fields(monkeypatch, tmp_path):
    monkeypatch.setattr("platform.system", lambda: "Windows")
    monkeypatch.setattr("platform.machine", lambda: "AMD64")
    conf = load_server_config_file()
    conf.prometheus.download_url_template = (
        "https://mirror.example/{os}/{arch}/{version}/{asset}"
    )
    plan = DependencyManager(conf, tmp_path).plan_install(targets=["prometheus"])[0]
    assert plan["url"] == (
        f"https://mirror.example/windows/amd64/{plan['version']}/{plan['asset']}"
    )


@pytest.mark.parametrize("recorded", [False, True])
def test_status_preserves_process_and_stop_reaps_it(tmp_path, recorded):
    process = _spawn_service(
        "test",
        [sys.executable, "-c", "import time; time.sleep(120)"],
        tmp_path / "service log.txt",
    )
    state_file = tmp_path / "state.json"
    state_file.write_text(
        json.dumps({"services": [{"name": "test", "pid": process.pid}]})
    )
    try:
        for _ in range(3):
            assert is_process_running(process.pid)
            assert load_active_state(state_file)
            assert process.poll() is None
        if recorded:
            runtime = LocalServiceRuntime(
                OmegaConf.create({"server": {"state_file": str(state_file)}}), tmp_path
            )
            assert runtime.stop()[0] == 0
            assert not state_file.exists()
        else:
            _terminate_process(process)
        assert process.wait(timeout=10) is not None
        assert not is_process_running(process.pid)
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows process tree termination")
def test_stop_terminates_child_processes(tmp_path):
    child_pid_file = tmp_path / "child.pid"
    code = (
        "import subprocess, sys, time; from pathlib import Path; "
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)']); "
        "Path(sys.argv[1]).write_text(str(child.pid)); time.sleep(120)"
    )
    process = _spawn_service(
        "parent",
        [sys.executable, "-c", code, str(child_pid_file)],
        tmp_path / "parent.log",
    )
    try:
        deadline = time.monotonic() + 10
        while not child_pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        child_pid = int(child_pid_file.read_text())
        assert is_process_running(child_pid)
        _terminate_process(process)
        assert not is_process_running(child_pid)
        assert process.poll() is not None
    finally:
        _terminate_process(process)
