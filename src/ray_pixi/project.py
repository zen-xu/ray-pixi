"""Project mode: collect the env-defining file subset from the working_dir.

The cache hash is computed from this subset only (manifest + include globs +
pixi.lock), so editing files outside ``include`` (e.g. driver scripts) does not
invalidate the installed pixi environment.
"""

from __future__ import annotations

import glob
import hashlib
import json
import os

from ray_pixi.spec import PixiSpec

WORKING_DIR_ENV_VAR = "RAY_RUNTIME_ENV_CREATE_WORKING_DIR"


def resolve_working_dir() -> str | None:
    """Return the local working_dir path Ray exposes to plugins, or None."""
    return os.environ.get(WORKING_DIR_ENV_VAR)


def _assert_within(path: str, base: str, label: str) -> None:
    """Raise ValueError if path escapes base (blocks ``..`` traversal)."""
    abs_base = os.path.abspath(base)
    abs_path = os.path.abspath(path)
    if abs_path != abs_base and not abs_path.startswith(abs_base + os.sep):
        raise ValueError(
            f"pixi {label} {path!r} resolves outside {base!r}; paths must stay "
            "within the working_dir."
        )


def main_manifest_path(pixi_spec: PixiSpec, base_dir: str) -> str:
    """Resolve the primary manifest path under base_dir.

    Uses ``pixi_spec.manifest`` when set (must exist and stay within base_dir),
    else auto-discovers ``pixi.toml`` then ``pyproject.toml``. Raises ValueError
    if none is found.
    """
    if pixi_spec.manifest:
        candidate = os.path.join(base_dir, pixi_spec.manifest)
        _assert_within(candidate, base_dir, "manifest")
        if not os.path.exists(candidate):
            raise ValueError(
                f"pixi manifest {pixi_spec.manifest!r} not found under {base_dir!r}."
            )
        return candidate
    for name in ("pixi.toml", "pyproject.toml"):
        candidate = os.path.join(base_dir, name)
        if os.path.exists(candidate):
            return candidate
    raise ValueError(
        f"pixi project mode found no pixi.toml/pyproject.toml in {base_dir!r}. "
        "Set runtime_env['pixi']['manifest'] or place one at the working_dir root."
    )


def _read_text(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"pixi include matched a non-text file: {path}. Narrow the glob so it "
            "only selects text files (manifests, sources)."
        ) from exc


def collect_files(pixi_spec: PixiSpec, working_dir: str) -> dict[str, str]:
    """Collect {relpath: content} for the env-defining subset under working_dir."""
    files: dict[str, str] = {}

    manifest_path = main_manifest_path(pixi_spec, working_dir)
    files[os.path.relpath(manifest_path, working_dir)] = _read_text(manifest_path)

    lock_path = os.path.join(os.path.dirname(manifest_path), "pixi.lock")
    if os.path.exists(lock_path):
        files[os.path.relpath(lock_path, working_dir)] = _read_text(lock_path)

    # include globs may re-match the manifest/lock already collected above;
    # dict-key dedup makes that idempotent.
    for pattern in pixi_spec.include:
        for match in glob.glob(os.path.join(working_dir, pattern), recursive=True):
            if os.path.isfile(match):
                _assert_within(match, working_dir, "include")
                files[os.path.relpath(match, working_dir)] = _read_text(match)

    return files


def compute_project_uri(pixi_spec: PixiSpec, working_dir: str) -> str:
    """Compute ``pixi://<sha1>`` from the include subset + install-affecting keys."""
    payload = {
        "files": dict(sorted(collect_files(pixi_spec, working_dir).items())),
        "environment": pixi_spec.environment,
        "locked": pixi_spec.locked,
        "pixi_version": pixi_spec.pixi_version,
        "pixi_install_options": pixi_spec.pixi_install_options,
    }
    canonical = json.dumps(payload, sort_keys=True)
    digest = hashlib.sha1(canonical.encode("utf-8")).hexdigest()
    return f"pixi://{digest}"


def compute_project_uri_from_working_dir_uri(
    pixi_spec: PixiSpec, working_dir_uri: str
) -> str:
    """Compute ``pixi://<sha1>`` from Ray's working_dir URI + install keys.

    Used in the ``get_uris`` phase, where the working_dir is not yet downloaded
    so the files (including ``pixi.lock``) cannot be read. Ray's working_dir URI
    already embeds a content hash of the whole working_dir, so deriving from it
    keeps the cache keyed on the project contents (lock included).
    """
    payload = {
        "working_dir_uri": working_dir_uri,
        "environment": pixi_spec.environment,
        "locked": pixi_spec.locked,
        "pixi_version": pixi_spec.pixi_version,
        "pixi_install_options": pixi_spec.pixi_install_options,
    }
    canonical = json.dumps(payload, sort_keys=True)
    digest = hashlib.sha1(canonical.encode("utf-8")).hexdigest()
    return f"pixi://{digest}"


def materialize_project(pixi_spec: PixiSpec, working_dir: str, target_dir: str) -> str:
    """Copy the include subset into target_dir, preserving relpaths.

    Returns the primary manifest path inside target_dir.

    Project mode requires a ``pixi.lock`` next to the manifest so every worker
    installs the exact same environment; without it pixi would re-solve per
    worker (slow and non-reproducible).
    """
    manifest_path = main_manifest_path(pixi_spec, working_dir)
    lock_path = os.path.join(os.path.dirname(manifest_path), "pixi.lock")
    if not os.path.exists(lock_path):
        raise ValueError(
            "pixi project mode requires a pixi.lock next to the manifest "
            f"({lock_path}). Run `pixi lock` and include it in working_dir."
        )
    for relpath, content in collect_files(pixi_spec, working_dir).items():
        dest = os.path.join(target_dir, relpath)
        _assert_within(dest, target_dir, "materialized file")
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "w", encoding="utf-8") as f:
            f.write(content)
    return main_manifest_path(pixi_spec, target_dir)
