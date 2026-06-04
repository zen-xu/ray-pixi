"""Synthesize an inline pixi spec into pixi.toml and probe installed versions."""

from __future__ import annotations

import glob
import os
import platform
import sys

import tomli_w

from ray_pixi.spec import PixiSpec


def current_python_version() -> str:
    """Return the running interpreter's full version, e.g. ``3.13.12``."""
    return platform.python_version()


def current_python_minor() -> str:
    """Return the running interpreter's ``major.minor``, e.g. ``3.13``."""
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def installed_python_minor(target_dir: str, environment: str) -> str | None:
    """Return the ``major.minor`` of the python installed in the pixi env.

    Reads it from the ``lib/python3.X`` directory of the installed environment.
    Returns None when it cannot be determined (e.g. a non-POSIX layout).
    """
    pattern = os.path.join(target_dir, ".pixi", "envs", environment, "lib", "python3.*")
    for path in glob.glob(pattern):
        name = os.path.basename(path).removeprefix("python")
        if name[:1].isdigit():
            return name
    return None


def current_ray_version() -> str:
    """Return the version of ray installed alongside this process."""
    import ray

    return ray.__version__


def installed_ray_version(target_dir: str, environment: str) -> str | None:
    """Return the ray version installed in the pixi env, or None if not found.

    Reads it from the ``ray-<version>.dist-info`` directory under site-packages.
    """
    pattern = os.path.join(
        target_dir,
        ".pixi",
        "envs",
        environment,
        "lib",
        "python3.*",
        "site-packages",
        "ray-*.dist-info",
    )
    for path in glob.glob(pattern):
        name = os.path.basename(path)
        version = name[len("ray-") : -len(".dist-info")]
        if version[:1].isdigit():
            return version
    return None


def current_pixi_platform() -> str:
    """Return the pixi platform string for the running machine (e.g. linux-64)."""
    system = {"Linux": "linux", "Darwin": "osx", "Windows": "win"}.get(
        platform.system(), platform.system().lower()
    )
    machine = platform.machine().lower()
    if machine in ("x86_64", "amd64"):
        arch = "64"
    elif machine in ("aarch64", "arm64"):
        arch = "arm64" if system == "osx" else "aarch64"
    else:
        arch = machine
    return f"{system}-{arch}"


def synthesize_pixi_toml(pixi_spec: PixiSpec) -> str:
    """Build pixi.toml text from an inline spec.

    Fills sensible defaults the inline form usually omits: ``channels`` to
    ``["conda-forge"]`` (needed to resolve the conda ``python``), ``platforms`` to
    the current machine's platform, and pins ``python`` / ``ray`` to the running
    versions, since a Ray worker's python and ray must match the cluster.
    """
    dependencies = dict(pixi_spec.dependencies)
    dependencies.setdefault("python", f"=={current_python_version()}")
    pypi_dependencies = dict(pixi_spec.pypi_dependencies)
    pypi_dependencies.setdefault(
        "ray", {"version": f"=={current_ray_version()}", "extras": ["default"]}
    )
    return tomli_w.dumps(
        {
            "workspace": {
                "channels": pixi_spec.channels or ["conda-forge"],
                "platforms": pixi_spec.platforms or [current_pixi_platform()],
            },
            "dependencies": dependencies,
            "pypi-dependencies": pypi_dependencies,
        }
    )


def materialize(pixi_spec: PixiSpec, target_dir: str) -> str:
    """Synthesize an inline spec into target_dir/pixi.toml and return its path."""
    manifest_path = os.path.join(target_dir, "pixi.toml")
    with open(manifest_path, "w", encoding="utf-8") as f:
        f.write(synthesize_pixi_toml(pixi_spec))
    return manifest_path
