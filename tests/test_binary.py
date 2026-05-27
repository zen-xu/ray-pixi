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
