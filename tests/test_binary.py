import os
import stat

import pytest

from ray_pixi import binary


def _make_fake_pixi(dir_path: str) -> str:
    os.makedirs(dir_path, exist_ok=True)
    exe = os.path.join(dir_path, "pixi")
    with open(exe, "w") as f:
        f.write("#!/bin/sh\necho pixi\n")
    os.chmod(exe, stat.S_IRWXU)
    return exe


def test_resolve_uses_path_when_no_version(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    exe = _make_fake_pixi(str(bin_dir))
    monkeypatch.setenv("PATH", str(bin_dir))
    resolved = binary.resolve_pixi(str(tmp_path / "target"), pixi_version=None)
    assert resolved == exe


def test_resolve_missing_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    with pytest.raises(RuntimeError, match="pixi executable not found"):
        binary.resolve_pixi(str(tmp_path / "target"), pixi_version=None)


def test_bootstrap_skips_download_when_already_installed(tmp_path, monkeypatch):
    # A previous create() left the installer layout (.pixi-bin/bin/pixi); the
    # next bootstrap must reuse it instead of re-downloading every time.
    target = tmp_path / "target"
    installed = target / ".pixi-bin" / "bin"
    exe = _make_fake_pixi(str(installed))

    def boom(*a, **k):
        raise AssertionError("must not download when already bootstrapped")

    monkeypatch.setattr("urllib.request.urlopen", boom)
    monkeypatch.setattr("urllib.request.urlretrieve", boom)
    assert binary._bootstrap_pixi(str(target), "0.40.0") == exe


def test_bootstrap_raises_when_installer_leaves_no_exe(tmp_path, monkeypatch):
    target = tmp_path / "target"
    monkeypatch.setattr(binary, "_download", lambda url, dest: open(dest, "w").close())
    monkeypatch.setattr(binary.subprocess, "check_call", lambda *a, **k: None)
    with pytest.raises(RuntimeError, match="no executable"):
        binary._bootstrap_pixi(str(target), "0.40.0")


def test_existing_bootstrap_exe_windows_naming(tmp_path, monkeypatch):
    monkeypatch.setattr(binary.sys, "platform", "win32")
    bin_dir = tmp_path / ".pixi-bin"
    (bin_dir / "bin").mkdir(parents=True)
    exe = bin_dir / "bin" / "pixi.exe"
    exe.write_text("")
    assert binary._existing_bootstrap_exe(str(bin_dir)) == str(exe)


def test_download_passes_timeout(tmp_path, monkeypatch):
    seen = {}

    class FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self, *a):
            return b""

    def fake_urlopen(url, timeout=None):
        seen["timeout"] = timeout
        return FakeResp()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    binary._download("https://example.com/x", str(tmp_path / "x"))
    assert seen["timeout"] is not None and seen["timeout"] > 0


def test_find_bootstrapped_pixi_never_downloads(tmp_path, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("find_bootstrapped_pixi must not download")

    monkeypatch.setattr("urllib.request.urlopen", boom)
    assert binary.find_bootstrapped_pixi(str(tmp_path)) is None
    exe = _make_fake_pixi(str(tmp_path / ".pixi-bin" / "bin"))
    assert binary.find_bootstrapped_pixi(str(tmp_path)) == exe


def test_resolve_with_version_bootstraps(tmp_path, monkeypatch):
    target = tmp_path / "target"
    target.mkdir()
    calls = {}

    def fake_bootstrap(target_dir, version):
        calls["args"] = (target_dir, version)
        path = os.path.join(target_dir, ".pixi-bin", "pixi")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, "w").close()
        return path

    monkeypatch.setattr(binary, "_bootstrap_pixi", fake_bootstrap)
    resolved = binary.resolve_pixi(str(target), pixi_version="0.40.0")
    assert calls["args"] == (str(target), "0.40.0")
    assert resolved.endswith(os.path.join(".pixi-bin", "pixi"))
