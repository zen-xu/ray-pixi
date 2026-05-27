"""Orchestrate: resolve pixi executable -> materialize manifest -> run pixi install."""

from __future__ import annotations

import logging
import os
import shutil
from collections.abc import Awaitable, Callable

from ray_pixi import binary, manifest, spec

Runner = Callable[..., Awaitable[None]]


async def _default_runner(cmd: list[str], *, cwd: str) -> None:
    # Imported lazily: only the agent side (where ray is installed) needs it.
    from ray._private.runtime_env.utils import check_output_cmd

    await check_output_cmd(cmd, logger=logging.getLogger("ray_pixi"), cwd=cwd)


class PixiProcessor:
    def __init__(
        self,
        target_dir: str,
        runtime_env: dict,
        logger: logging.Logger,
        *,
        runner: Runner | None = None,
    ) -> None:
        self._target_dir = target_dir
        self._spec = spec.normalize(runtime_env["pixi"])
        self._logger = logger
        self._runner = runner or _default_runner

    async def run(self) -> None:
        target = self._target_dir
        os.makedirs(target, exist_ok=True)
        try:
            pixi_exe = binary.resolve_pixi(target, self._spec.pixi_version)
            manifest_path = manifest.materialize(self._spec, target)

            cmd = [
                pixi_exe,
                "install",
                "--manifest-path",
                manifest_path,
                "-e",
                self._spec.environment,
            ]
            if self._spec.locked:
                cmd.append("--locked")
            cmd += self._spec.pixi_install_options

            self._logger.info("Installing pixi environment into %s", target)
            await self._runner(cmd, cwd=target)
            self._verify_versions(target)
        except Exception:
            self._logger.exception("Failed to install pixi environment.")
            shutil.rmtree(target, ignore_errors=True)
            raise

    def _verify_versions(self, target: str) -> None:
        """Fail if the env's python/ray are missing or do not match the cluster.

        Ray refuses to connect a worker whose python or ray differs from the
        cluster, so we reject the problem up front with a clear error. Inline specs
        auto-fill python/ray, but a user-provided manifest must declare both, so in
        manifest mode a missing python/ray is also an error.
        """
        env = self._spec.environment
        expected_py = manifest.current_python_minor()
        expected_ray = manifest.current_ray_version()
        found_py = manifest.installed_python_minor(target, env)
        found_ray = manifest.installed_ray_version(target, env)

        if self._spec.source == "manifest":
            if found_py is None:
                raise RuntimeError(
                    "The pixi manifest does not provide python. Declare it in "
                    f"[dependencies] matching the cluster, e.g. python = "
                    f'"=={expected_py}.*".'
                )
            if found_ray is None:
                raise RuntimeError(
                    "The pixi manifest does not provide ray. Declare it in "
                    f"[pypi-dependencies] matching the cluster, e.g. "
                    f'ray = "=={expected_ray}".'
                )

        if found_py is not None and found_py != expected_py:
            raise RuntimeError(
                f"The pixi environment's python {found_py} does not match the "
                f"cluster's python {expected_py}. Ray requires the worker python to "
                "match the cluster (at least to the minor version). Pin python to "
                f"{expected_py}.* in your pixi manifest or dependencies."
            )

        if found_ray is not None and found_ray != expected_ray:
            raise RuntimeError(
                f"The pixi environment's ray {found_ray} does not match the "
                f"cluster's ray {expected_ray}. Ray requires the worker ray version "
                f"to match the cluster exactly. Pin ray to =={expected_ray} in your "
                "pixi manifest or pypi_dependencies."
            )
