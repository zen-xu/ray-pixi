"""Parse and validate the pixi runtime_env field and compute its cache URI."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Literal, overload

from pydantic import BaseModel, ConfigDict, model_validator

INLINE_KEYS = ("channels", "dependencies", "pypi_dependencies", "platforms")


class PixiSpec(BaseModel):
    """Normalized pixi runtime_env configuration (the contract used throughout)."""

    model_config = ConfigDict(extra="forbid")

    # Project mode: a manifest + include/exclude resolved against the working_dir.
    manifest: str | None = None
    include: list[str] = []
    exclude: list[str] = []
    # Inline mode: dependency keys synthesized into a pixi.toml on each node.
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
    def _has_inline(self) -> bool:
        return bool(
            self.channels
            or self.dependencies
            or self.pypi_dependencies
            or self.platforms
        )

    @property
    def source(self) -> Literal["inline", "project"]:
        """Inline dependency keys -> "inline"; otherwise a working_dir project."""
        return "inline" if self._has_inline else "project"

    @model_validator(mode="after")
    def _reject_inline_with_project(self) -> PixiSpec:
        """Inline dependency keys and project keys are mutually exclusive."""
        has_project = bool(self.manifest or self.include or self.exclude)
        if self._has_inline and has_project:
            raise ValueError(
                "runtime_env['pixi'] cannot specify both inline spec keys "
                f"({', '.join(INLINE_KEYS)}) and project keys "
                "(manifest/include/exclude)."
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
    """Compute a deterministic cache URI ``pixi://<sha1>`` from an inline field.

    Hashes the serialized normalized field so every node derives the same URI for
    the same inline spec. For project mode, use ``project.compute_project_uri``
    instead, which hashes the working_dir file subset.
    """
    # json.dumps with sort_keys so dict key order does not change the URI
    # (model_dump_json preserves insertion order).
    canonical = json.dumps(normalize(field).model_dump(), sort_keys=True)
    digest = hashlib.sha1(canonical.encode("utf-8")).hexdigest()
    return f"pixi://{digest}"


@overload
def pixi(
    manifest: str | None = ...,
    *,
    include: list[str] | None = ...,
    exclude: list[str] | None = ...,
    environment: str = ...,
    locked: bool = ...,
    pixi_version: str | None = ...,
    pixi_install_options: list[str] | None = ...,
) -> dict:
    """Build the pixi field in project mode (manifest + include via working_dir)."""


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
    """Build the pixi field in inline mode (dependency keys)."""


def pixi(
    manifest: str | None = None,
    *,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    channels: list[str] | None = None,
    dependencies: dict[str, str | dict] | None = None,
    pypi_dependencies: dict[str, str | dict] | None = None,
    platforms: list[str] | None = None,
    environment: str = "default",
    locked: bool = False,
    pixi_version: str | None = None,
    pixi_install_options: list[str] | None = None,
) -> dict:
    """Assemble a ``runtime_env["pixi"]`` dict. Optional convenience over a raw dict.

    Two mutually exclusive modes:

    * **Project mode** -- pass ``manifest`` (a working_dir-relative path, or omit for
      auto-discovery) and/or ``include`` (globs selecting the env-defining files:
      pyproject.toml, local package sources). Files are NOT read here; they travel
      via Ray's ``working_dir`` and are collected on each node. Set
      ``runtime_env["working_dir"]`` so the files reach the workers.
    * **Inline mode** -- pass any of ``channels`` / ``dependencies`` /
      ``pypi_dependencies`` / ``platforms``. A minimal pixi.toml is synthesized on
      each node.

    Args:
        manifest: working_dir-relative path to a pixi.toml / pyproject.toml
            (project mode). Mutually exclusive with the inline keys.
        include: globs or directories (relative to working_dir) selecting files
            into the env cache hash (project mode). A directory entry includes
            its whole subtree, dotfiles included.
        exclude: globs or directories (relative to working_dir) removed from
            the include selection; the manifest and pixi.lock are always kept
            (project mode).
        channels/dependencies/pypi_dependencies/platforms: inline mode keys.
        environment: pixi environment to select (pixi ``-e``).
        locked: reproduce strictly from pixi.lock (``pixi install --locked``).
        pixi_version: bootstrap this exact pixi version on the node if set.
        pixi_install_options: extra flags forwarded to ``pixi install``.

    Returns:
        A dict suitable as ``runtime_env["pixi"]``.

    Raises:
        ValueError: if both project keys and inline keys are given.
    """
    inline = {
        "channels": channels,
        "dependencies": dependencies,
        "pypi_dependencies": pypi_dependencies,
        "platforms": platforms,
    }
    has_inline = any(v is not None for v in inline.values())
    has_project = manifest is not None or include is not None or exclude is not None

    if has_inline and has_project:
        raise ValueError(
            "pixi() cannot take both project keys (manifest/include/exclude) "
            "and inline spec keys."
        )

    common = {
        "environment": environment,
        "locked": locked,
        "pixi_version": pixi_version,
        "pixi_install_options": list(pixi_install_options or []),
    }

    if has_inline:
        return {
            "channels": list(channels or []),
            "dependencies": dict(dependencies or {}),
            "pypi_dependencies": dict(pypi_dependencies or {}),
            "platforms": list(platforms or []),
            **common,
        }

    return {
        "manifest": manifest,
        "include": list(include or []),
        "exclude": list(exclude or []),
        **common,
    }
