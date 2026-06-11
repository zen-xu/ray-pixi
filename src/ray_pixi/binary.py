"""Resolve the pixi executable: look it up on PATH or bootstrap a version locally."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import urllib.request


def find_bootstrapped_pixi(target_dir: str) -> str | None:
    """Return the pixi previously bootstrapped under target_dir, or None.

    Pure lookup -- never downloads. For synchronous contexts (modify_context)
    where create() has already done the bootstrap.
    """
    return _existing_bootstrap_exe(os.path.join(target_dir, ".pixi-bin"))


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


_DOWNLOAD_TIMEOUT_SECONDS = 60.0


def _existing_bootstrap_exe(bin_dir: str) -> str | None:
    """Return the pixi executable already bootstrapped under bin_dir, or None.

    The official installer places it at ``$PIXI_HOME/bin/pixi``; the flat
    ``$PIXI_HOME/pixi`` layout is also accepted.
    """
    exe_name = "pixi.exe" if sys.platform == "win32" else "pixi"
    for candidate in (
        os.path.join(bin_dir, "bin", exe_name),
        os.path.join(bin_dir, exe_name),
    ):
        if os.path.exists(candidate):
            return candidate
    return None


def _download(url: str, dest: str) -> None:
    """Download url to dest with a timeout so a hung mirror cannot block forever."""
    with (
        urllib.request.urlopen(url, timeout=_DOWNLOAD_TIMEOUT_SECONDS) as resp,
        open(dest, "wb") as f,
    ):
        shutil.copyfileobj(resp, f)


def _bootstrap_pixi(target_dir: str, version: str) -> str:
    """Download the given pixi version into target_dir/.pixi-bin and return its path."""
    bin_dir = os.path.join(target_dir, ".pixi-bin")
    os.makedirs(bin_dir, exist_ok=True)
    existing = _existing_bootstrap_exe(bin_dir)
    if existing:
        return existing

    env = {**os.environ, "PIXI_VERSION": f"v{version}", "PIXI_HOME": bin_dir}
    if sys.platform == "win32":
        script = os.path.join(bin_dir, "install.ps1")
        _download("https://pixi.sh/install.ps1", script)
        subprocess.check_call(
            ["powershell", "-ExecutionPolicy", "ByPass", "-File", script], env=env
        )
    else:
        script = os.path.join(bin_dir, "install.sh")
        _download("https://pixi.sh/install.sh", script)
        subprocess.check_call(["sh", script], env=env)

    installed = _existing_bootstrap_exe(bin_dir)
    if installed is None:
        raise RuntimeError(
            f"pixi installer for v{version} completed but left no executable "
            f"under {bin_dir}."
        )
    return installed
