"""Parse and validate the pixi runtime_env field and compute its cache URI."""

from __future__ import annotations

import hashlib
import os
from typing import Any, Literal, overload

from pydantic import BaseModel, ConfigDict, Field, model_validator

INLINE_KEYS = ("channels", "dependencies", "pypi_dependencies", "platforms")


class PixiSpec(BaseModel):
    """Normalized pixi runtime_env configuration (the contract used throughout)."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    manifest_content: str | None = None
    manifest_format: Literal["pixi.toml", "pyproject.toml"] = "pixi.toml"
    manifest_path: str | None = Field(default=None, alias="manifest")
    lock_content: str | None = None
    channels: list[str] = []
    # Values are a version string ("3.13.*") or a pixi match-spec table
    # ({"version": ">=1.0", "build": "...", "channel": "..."} / {"git": ...}).
    dependencies: dict[str, str | dict] = {}
    pypi_dependencies: dict[str, str | dict] = {}
    platforms: list[str] = []
    environment: str = "default"
    locked: bool = False
    pixi_version: str | None = None
    pixi_install_options: list[str] = []

    @property
    def source(self) -> Literal["manifest", "inline"]:
        """Whether the spec is backed by a manifest or by inline dependency keys."""
        return "manifest" if (self.manifest_content or self.manifest_path) else "inline"

    @model_validator(mode="after")
    def _require_exactly_one_source(self) -> PixiSpec:
        """Require exactly one of: a manifest, or inline dependency keys."""
        has_manifest = bool(self.manifest_content or self.manifest_path)
        has_inline = bool(
            self.channels
            or self.dependencies
            or self.pypi_dependencies
            or self.platforms
        )
        if has_manifest and has_inline:
            raise ValueError(
                "runtime_env['pixi'] cannot specify both a manifest "
                "(manifest/manifest_content) and inline spec keys "
                f"({', '.join(INLINE_KEYS)})."
            )
        if not has_manifest and not has_inline:
            raise ValueError(
                "runtime_env['pixi'] must specify either a manifest "
                "(manifest=...) or inline spec keys "
                f"({', '.join(INLINE_KEYS)})."
            )
        return self


def validate(field: Any) -> None:
    """Validate a pixi field, raising ValueError/TypeError when invalid."""
    normalize(field)


def normalize(field: str | dict) -> PixiSpec:
    """Normalize a pixi field into a PixiSpec model."""
    if isinstance(field, str):
        field = {"manifest": field}
    if not isinstance(field, dict):
        raise TypeError(
            f"runtime_env['pixi'] must be a str or dict, got {type(field).__name__}."
        )
    return PixiSpec.model_validate(field)


def compute_uri(field: str | dict) -> str:
    """Compute a deterministic cache URI ``pixi://<sha1>`` from the field content.

    Only depends on the serialized field content (no local file reads) so every
    node's agent derives the same URI. Note: a bare-path field (``{"manifest":
    "p.toml"}`` not built via :func:`pixi`) is hashed by its path string, not the
    file content, so edits won't invalidate the cache and different nodes sharing
    a path may collide. Build the field with :func:`pixi` to hash content instead.
    """
    canonical = normalize(field).model_dump_json()
    digest = hashlib.sha1(canonical.encode("utf-8")).hexdigest()
    return f"pixi://{digest}"


@overload
def pixi(
    manifest: str,
    *,
    environment: str = ...,
    locked: bool = ...,
    pixi_version: str | None = ...,
    pixi_install_options: list[str] | None = ...,
) -> dict:
    """Build the pixi field from a manifest file (manifest mode).

    Reads ``manifest`` (a ``pixi.toml`` / ``pyproject.toml``) and, if present, the
    sibling ``pixi.lock`` on the driver, inlining their content into the returned
    dict so the worker nodes need not have the files. Cannot be combined with the
    inline spec keys.

    Args:
        manifest: Path to a ``pixi.toml`` / ``pyproject.toml``.
        environment: pixi environment to select (pixi ``-e``).
        locked: Reproduce strictly from ``pixi.lock`` (``pixi install --locked``).
        pixi_version: Bootstrap this exact pixi version on the node if set.
        pixi_install_options: Extra flags forwarded to ``pixi install``.

    Returns:
        A dict suitable as ``runtime_env["pixi"]``.
    """


@overload
def pixi(
    *,
    channels: list[str] | None = ...,
    dependencies: dict[str, str | dict] | None = ...,
    pypi_dependencies: dict[str, str | dict] | None = ...,
    platforms: list[str] | None = ...,
    environment: str = ...,
    locked: bool = ...,
    pixi_version: str | None = ...,
    pixi_install_options: list[str] | None = ...,
) -> dict:
    """Build the pixi field from inline dependency keys (inline mode).

    The plugin synthesizes a minimal ``pixi.toml`` from these keys on each node.
    Cannot be combined with a manifest path.

    Args:
        channels: Conda channels, e.g. ``["conda-forge"]``.
        dependencies: Conda dependencies; each value is a version string
            (``"==3.13.12"``) or a match-spec table
            (``{"version": "3.13.*", "channel": "conda-forge"}``).
        pypi_dependencies: PyPI dependencies; each value is a version string or a
            table (``{"version": ">=22", "extras": ["d"]}`` or
            ``{"git": "https://...", "rev": "main"}``).
        platforms: Target platforms; defaults to the building node's platform.
        environment: pixi environment to select (pixi ``-e``).
        locked: Reproduce strictly from ``pixi.lock`` (``pixi install --locked``).
        pixi_version: Bootstrap this exact pixi version on the node if set.
        pixi_install_options: Extra flags forwarded to ``pixi install``.

    Returns:
        A dict suitable as ``runtime_env["pixi"]``.
    """


def pixi(
    manifest: str | None = None,
    *,
    channels: list[str] | None = None,
    dependencies: dict[str, str | dict] | None = None,
    pypi_dependencies: dict[str, str | dict] | None = None,
    platforms: list[str] | None = None,
    environment: str = "default",
    locked: bool = False,
    pixi_version: str | None = None,
    pixi_install_options: list[str] | None = None,
) -> dict:
    """Build a self-contained ``runtime_env["pixi"]`` field on the driver.

    This is the recommended way to construct the pixi field. It runs on the driver
    and produces a plain dict that Ray serializes and ships to every node, where the
    ``PixiPlugin`` installs the environment and launches workers inside it.

    There are two mutually exclusive modes:

    * **Manifest mode** -- pass ``manifest``, a path to a ``pixi.toml`` or
      ``pyproject.toml``. The file is read on the driver and its content is inlined
      into the returned dict; if a ``pixi.lock`` sits next to it, that is inlined
      too (so ``locked=True`` can reproduce it exactly). Because the content travels
      with the field, the manifest does not need to exist on the worker nodes.
    * **Inline mode** -- pass any of ``channels`` / ``dependencies`` /
      ``pypi_dependencies`` / ``platforms``. The plugin synthesizes a minimal
      ``pixi.toml`` from them on each node.

    Prefer this helper over putting a bare path in ``runtime_env["pixi"]``
    (e.g. ``{"pixi": "pixi.toml"}``): inlined content is cached by content, so edits
    invalidate the cache and different nodes never collide on the same hash, whereas
    a bare path is cached by its path string. See :func:`compute_uri`.

    Args:
        manifest: Path to a ``pixi.toml`` / ``pyproject.toml`` (manifest mode).
            Mutually exclusive with the inline keys below.
        channels: Conda channels, e.g. ``["conda-forge"]`` (inline mode).
        dependencies: Conda dependencies mapping a package name to a version string
            (``{"python": "==3.13.12"}``) or a pixi match-spec table
            (``{"python": {"version": "3.13.*", "channel": "conda-forge"}}``).
        pypi_dependencies: PyPI dependencies, each value a version string or a table
            (``{"black": {"version": ">=22", "extras": ["d"]}}``,
            ``{"pkg": {"git": "https://...", "rev": "main"}}``).
        platforms: Target platforms, e.g. ``["linux-64"]``. Defaults to the building
            node's platform when omitted.
        environment: Name of the pixi environment to select (pixi ``-e``).
        locked: Reproduce strictly from ``pixi.lock`` (passes ``--locked`` to
            ``pixi install``).
        pixi_version: If set, bootstrap this exact pixi version on the node instead
            of using the ``pixi`` already on ``PATH``.
        pixi_install_options: Extra flags forwarded verbatim to ``pixi install``.

    Returns:
        A dict suitable as ``runtime_env["pixi"]``.

    Raises:
        ValueError: If both a manifest and inline keys are given, or neither.
        FileNotFoundError: If ``manifest`` does not point to an existing file.

    Note:
        The pixi environment must provide a ``python`` and ``ray`` that match the
        cluster exactly, down to the micro version; ray-pixi does not inject them.

    Examples:
        From a manifest, reproduced from its lockfile::

            ray.init(runtime_env={"pixi": pixi("pixi.toml", locked=True)})

        Declared inline, pinned to the cluster's versions::

            ray.init(
                runtime_env={
                    "pixi": pixi(
                        channels=["conda-forge"],
                        dependencies={"python": "==3.13.12"},
                        pypi_dependencies={"ray": "==2.55.1"},
                    )
                }
            )
    """
    inline = {
        "channels": channels,
        "dependencies": dependencies,
        "pypi_dependencies": pypi_dependencies,
        "platforms": platforms,
    }
    has_inline = any(v is not None for v in inline.values())

    if manifest is not None and has_inline:
        raise ValueError(
            "pixi() cannot take both a manifest path and inline spec keys."
        )
    if manifest is None and not has_inline:
        raise ValueError("pixi() must take either a manifest path or inline spec keys.")

    common = {
        "environment": environment,
        "locked": locked,
        "pixi_version": pixi_version,
        "pixi_install_options": list(pixi_install_options or []),
    }

    if manifest is not None:
        with open(manifest, encoding="utf-8") as f:
            manifest_content = f.read()
        manifest_format = (
            "pyproject.toml"
            if os.path.basename(manifest) == "pyproject.toml"
            else "pixi.toml"
        )
        lock_path = os.path.join(
            os.path.dirname(os.path.abspath(manifest)), "pixi.lock"
        )
        lock_content = None
        if os.path.exists(lock_path):
            with open(lock_path, encoding="utf-8") as f:
                lock_content = f.read()
        return {
            "manifest_content": manifest_content,
            "manifest_format": manifest_format,
            "lock_content": lock_content,
            **common,
        }

    return {
        "channels": list(channels or []),
        "dependencies": dict(dependencies or {}),
        "pypi_dependencies": dict(pypi_dependencies or {}),
        "platforms": list(platforms or []),
        **common,
    }
