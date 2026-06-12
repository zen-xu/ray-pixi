"""Run pixi install into a prepared target dir and verify python/ray match."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
from collections.abc import Awaitable, Callable

from ray_pixi import manifest
from ray_pixi.spec import PixiSpec

Runner = Callable[..., Awaitable[None]]

_LOG_TAIL_BYTES = 4096


def _looks_like_lock_mismatch(error_text: str) -> bool:
    """Match pixi's --locked failure across wording variants
    ("lock-file"/"lockfile", "not up-to-date")."""
    normalized = error_text.lower().replace("-", " ").replace("_", " ")
    return "lock" in normalized and "up to date" in normalized


def _tail(log_path: str, max_bytes: int = _LOG_TAIL_BYTES) -> str:
    try:
        with open(log_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - max_bytes))
            return f.read().decode("utf-8", "replace")
    except OSError:
        return "<log unavailable>"


async def _stream_to_log_runner(cmd: list[str], *, cwd: str, log_path: str) -> None:
    """Run cmd with stdout/stderr appended to a dedicated log file.

    Deliberately NOT a ray logger: runtime_env setup logs
    (runtime_env_setup-*.log) can be streamed to drivers/clients, and pixi's
    verbose install output does not belong there. Redirecting the file
    descriptor also keeps the agent's event loop out of the data path and the
    file live-tailable on the node.
    """
    env = {**os.environ, "NO_COLOR": "1"}
    with open(log_path, "ab") as f:
        f.write(f"$ {' '.join(cmd)}\n".encode())
        f.flush()
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=cwd, env=env, stdout=f, stderr=asyncio.subprocess.STDOUT
        )
        try:
            returncode = await proc.wait()
        except asyncio.CancelledError:
            # delete_uri cancels in-flight creates; the install must die with
            # the task or it keeps writing into the (re)created dir as an
            # orphan, racing any subsequent install of the same env.
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
            raise
    if returncode != 0:
        raise RuntimeError(
            f"`{' '.join(cmd)}` failed with exit code {returncode}. "
            f"Last output:\n{_tail(log_path)}\nFull log: {log_path}"
        )


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
        log_path: str | None = None,
    ) -> None:
        self._target_dir = target_dir
        self._manifest_path = manifest_path
        self._spec = pixi_spec
        self._pixi_exe = pixi_exe
        self._logger = logger
        # A sibling of the env dir: survives the env dir's removal on failure.
        self._log_path = log_path or f"{target_dir}.install.log"
        self._runner = runner or functools.partial(
            _stream_to_log_runner, log_path=self._log_path
        )

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

        self._logger.info(
            "Installing pixi environment into %s (pixi output -> %s)",
            self._target_dir,
            self._log_path,
        )
        try:
            await self._runner(cmd, cwd=self._target_dir)
        except RuntimeError as e:
            if self._spec.locked and _looks_like_lock_mismatch(str(e)):
                raise RuntimeError(
                    f"{e}\n"
                    "Hint: pixi.lock is out of date with the manifest. Run "
                    "`pixi lock` (or `pixi install`) locally, include the "
                    "updated pixi.lock in your working_dir, and resubmit. "
                    "(Project mode installs with --locked by default; pass "
                    "locked=False to opt out.)"
                ) from e
            raise
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
