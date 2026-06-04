"""Run pixi install into a prepared target dir and verify python/ray match."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from ray_pixi import manifest
from ray_pixi.spec import PixiSpec

Runner = Callable[..., Awaitable[None]]


async def _default_runner(cmd: list[str], *, cwd: str) -> None:
    # Imported lazily: only the agent side (where ray is installed) needs it.
    from ray._private.runtime_env.utils import check_output_cmd

    await check_output_cmd(cmd, logger=logging.getLogger("ray_pixi"), cwd=cwd)


class PixiProcessor:
    """Install the pixi env for an already-materialized manifest, then verify."""

    def __init__(
        self,
        target_dir: str,
        manifest_path: str,
        pixi_spec: PixiSpec,
        pixi_exe: str,
        logger: logging.Logger,
        *,
        runner: Runner | None = None,
    ) -> None:
        self._target_dir = target_dir
        self._manifest_path = manifest_path
        self._spec = pixi_spec
        self._pixi_exe = pixi_exe
        self._logger = logger
        self._runner = runner or _default_runner

    async def run(self) -> None:
        cmd = [
            self._pixi_exe,
            "install",
            "--manifest-path",
            self._manifest_path,
            "-e",
            self._spec.environment,
        ]
        if self._spec.locked:
            cmd.append("--locked")
        cmd += self._spec.pixi_install_options

        self._logger.info("Installing pixi environment into %s", self._target_dir)
        await self._runner(cmd, cwd=self._target_dir)
        self._verify_versions()

    def _verify_versions(self) -> None:
        """Fail if the env's python/ray are missing or do not match the cluster.

        Ray refuses to connect a worker whose python or ray differs from the
        cluster, so we reject the problem up front with a clear error.
        """
        env = self._spec.environment
        expected_py = manifest.current_python_minor()
        expected_ray = manifest.current_ray_version()
        found_py = manifest.installed_python_minor(self._target_dir, env)
        found_ray = manifest.installed_ray_version(self._target_dir, env)

        if found_py is None:
            raise RuntimeError(
                "The pixi environment does not provide python. Declare it matching "
                f'the cluster, e.g. python = "=={expected_py}.*".'
            )
        if found_ray is None:
            raise RuntimeError(
                "The pixi environment does not provide ray. Declare it matching the "
                f'cluster, e.g. ray = "=={expected_ray}".'
            )
        if found_py != expected_py:
            raise RuntimeError(
                f"The pixi environment's python {found_py} does not match the "
                f"cluster's python {expected_py}. Pin python to {expected_py}.*."
            )
        if found_ray != expected_ray:
            raise RuntimeError(
                f"The pixi environment's ray {found_ray} does not match the "
                f"cluster's ray {expected_ray}. Pin ray to =={expected_ray}."
            )
