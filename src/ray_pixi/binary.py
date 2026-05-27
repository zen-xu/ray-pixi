"""Resolve the pixi executable: look it up on PATH or bootstrap a version locally."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import urllib.request


def resolve_pixi(target_dir: str, pixi_version: str | None) -> str:
    """Return a usable pixi executable path.

    With pixi_version set, bootstrap that version into target_dir/.pixi-bin.
    Otherwise use the pixi on PATH; raise RuntimeError if it is not found.
    """
    if pixi_version:
        return _bootstrap_pixi(target_dir, pixi_version)
    found = shutil.which("pixi")
    if not found:
        raise RuntimeError(
            "pixi executable not found on PATH. Install pixi on every node "
            "(https://pixi.sh) or set runtime_env['pixi']['pixi_version'] to "
            "bootstrap a specific version."
        )
    return found


def _bootstrap_pixi(target_dir: str, version: str) -> str:
    """Download the given pixi version into target_dir/.pixi-bin and return its path."""
    bin_dir = os.path.join(target_dir, ".pixi-bin")
    os.makedirs(bin_dir, exist_ok=True)
    exe = os.path.join(bin_dir, "pixi.exe" if sys.platform == "win32" else "pixi")
    if os.path.exists(exe):
        return exe

    if sys.platform == "win32":
        script = os.path.join(bin_dir, "install.ps1")
        urllib.request.urlretrieve("https://pixi.sh/install.ps1", script)
        env = {**os.environ, "PIXI_VERSION": f"v{version}", "PIXI_HOME": bin_dir}
        subprocess.check_call(
            ["powershell", "-ExecutionPolicy", "ByPass", "-File", script], env=env
        )
    else:
        script = os.path.join(bin_dir, "install.sh")
        urllib.request.urlretrieve("https://pixi.sh/install.sh", script)
        env = {**os.environ, "PIXI_VERSION": f"v{version}", "PIXI_HOME": bin_dir}
        subprocess.check_call(["sh", script], env=env)

    # The official installer places the executable at $PIXI_HOME/bin/pixi.
    installed = os.path.join(bin_dir, "bin", "pixi")
    if os.path.exists(installed):
        return installed
    return exe
