"""PixiPlugin: the Ray runtime_env plugin for the 'pixi' field."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import sys
import tempfile

from ray._common.utils import try_to_create_directory
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import get_directory_size_bytes

from ray_pixi import binary, manifest, project, spec
from ray_pixi.processor import PixiProcessor

default_logger = logging.getLogger(__name__)


def _agent_resources_dir() -> str:
    """Resolve the runtime_env resource dir the agent was started with.

    Third-party plugins loaded via RAY_RUNTIME_ENV_PLUGINS are constructed with
    no arguments, so the dir is not injected; the agent receives it as the
    ``--runtime-env-dir`` CLI argument, which we read from ``sys.argv``.
    """
    argv = sys.argv
    flag = "--runtime-env-dir"
    if flag in argv:
        return argv[argv.index(flag) + 1]
    for arg in argv:
        if arg.startswith(f"{flag}="):
            return arg.split("=", 1)[1]
    return os.path.join(tempfile.gettempdir(), "ray_pixi")


class PixiPlugin(RuntimeEnvPlugin):
    name = "pixi"

    def __init__(self, resources_dir: str | None = None):
        if resources_dir is None:
            resources_dir = _agent_resources_dir()
        self._resource_dir = os.path.join(resources_dir, "pixi")
        self._creating_task: dict[str, asyncio.Task] = {}
        # One lock per URI prevents concurrent installs of the same environment.
        self._create_locks: dict[str, asyncio.Lock] = {}
        # Maps a created hash to the size in bytes of its installed directory.
        self._created_hash_bytes: dict[str, int] = {}
        try_to_create_directory(self._resource_dir)

    @staticmethod
    def validate(runtime_env_dict: dict) -> None:
        field = runtime_env_dict.get("pixi")
        if field is not None:
            spec.validate(field)

    def _hash_of(self, uri: str) -> str:
        return uri.split("://", 1)[1]

    def _target_dir(self, uri: str) -> str:
        return os.path.join(self._resource_dir, self._hash_of(uri))

    def get_uris(self, runtime_env) -> list[str]:
        field = runtime_env.get("pixi")
        if not field:
            return []
        pixi_spec = spec.normalize(field)
        if pixi_spec.source == "inline":
            return [spec.compute_uri(field)]
        # get_uris runs in the agent's increase_reference phase, before Ray has
        # downloaded the working_dir, so its files cannot be read here. Derive
        # the cache key from the working_dir URI Ray already computed (it embeds
        # a content hash of the whole working_dir, pixi.lock included).
        working_dir_uri = runtime_env.get("working_dir", "")
        if not working_dir_uri:
            raise ValueError(
                "pixi project mode requires runtime_env['working_dir'] so the "
                "manifest and sources reach the workers."
            )
        return [
            project.compute_project_uri_from_working_dir_uri(
                pixi_spec, working_dir_uri
            )
        ]

    def _resolve_pixi(self, pixi_spec: spec.PixiSpec, target_dir: str) -> str:
        if pixi_spec.pixi_version:
            bootstrap_dir = os.path.join(
                self._resource_dir, "pixi-bin", pixi_spec.pixi_version
            )
            return binary.resolve_pixi(bootstrap_dir, pixi_spec.pixi_version)
        return binary.resolve_pixi(target_dir, None)

    async def create(self, uri, runtime_env, context, logger=default_logger) -> int:
        if not runtime_env.get("pixi"):
            return 0
        pixi_spec = spec.normalize(runtime_env["pixi"])
        target_dir = self._target_dir(uri)

        async def _create() -> int:
            os.makedirs(target_dir, exist_ok=True)
            try:
                pixi_exe = self._resolve_pixi(pixi_spec, target_dir)
                if pixi_spec.source == "inline":
                    manifest_path = manifest.materialize(pixi_spec, target_dir)
                else:
                    # create runs inside Ray's with_working_dir_env context, so
                    # the working_dir is downloaded and its local path is exposed
                    # via the env var (unlike get_uris, which only sees the URI).
                    working_dir = project.resolve_working_dir()
                    if not working_dir:
                        raise ValueError(
                            "pixi project mode requires runtime_env['working_dir']."
                        )
                    manifest_path = project.materialize_project(
                        pixi_spec, working_dir, target_dir
                    )
                await PixiProcessor(
                    target_dir, manifest_path, pixi_spec, pixi_exe, logger
                ).run()
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(
                    None, get_directory_size_bytes, target_dir
                )
            except Exception:
                logger.exception("Failed to install pixi environment.")
                # Remove the broken target dir by default to reclaim space. Set
                # RAY_PIXI_KEEP_ON_FAILURE=1 to keep it so the failed install can
                # be inspected (e.g. a worker pod's pixi build error).
                if os.environ.get("RAY_PIXI_KEEP_ON_FAILURE") == "1":
                    logger.warning(
                        "Keeping %s for inspection; unset "
                        "RAY_PIXI_KEEP_ON_FAILURE to remove it on failure.",
                        target_dir,
                    )
                else:
                    shutil.rmtree(target_dir, ignore_errors=True)
                raise

        if uri not in self._create_locks:
            self._create_locks[uri] = asyncio.Lock()
        async with self._create_locks[uri]:
            hash_val = self._hash_of(uri)
            if hash_val in self._created_hash_bytes:
                return self._created_hash_bytes[hash_val]
            self._creating_task[hash_val] = task = asyncio.create_task(_create())
            task.add_done_callback(lambda _: self._creating_task.pop(hash_val, None))
            size = await task
            self._created_hash_bytes[hash_val] = size
            return size

    def modify_context(self, uris, runtime_env, context, logger=default_logger) -> None:
        if not runtime_env.get("pixi"):
            return
        uri = uris[0]
        target_dir = self._target_dir(uri)
        pixi_spec = spec.normalize(runtime_env["pixi"])
        env_name = pixi_spec.environment

        env_dir = os.path.join(target_dir, ".pixi", "envs", env_name)
        if not os.path.exists(env_dir):
            raise ValueError(
                f"Pixi environment {env_dir} does not exist on the cluster. "
                "Something may have gone wrong while installing the pixi runtime_env."
            )

        pixi_exe = self._resolve_pixi(pixi_spec, target_dir)
        if pixi_spec.source == "inline":
            manifest_path = os.path.join(target_dir, "pixi.toml")
        else:
            manifest_path = project.main_manifest_path(pixi_spec, target_dir)
        context.py_executable = (
            f"{pixi_exe} run --manifest-path {manifest_path} -e {env_name} python"
        )

    def delete_uri(self, uri: str, logger=default_logger) -> int:
        hash_val = self._hash_of(uri)
        task = self._creating_task.pop(hash_val, None)
        if task is not None:
            task.cancel()
        self._created_hash_bytes.pop(hash_val, None)
        self._create_locks.pop(uri, None)
        target_dir = self._target_dir(uri)
        size = get_directory_size_bytes(target_dir)
        try:
            shutil.rmtree(target_dir)
        except OSError as e:
            logger.warning("Error deleting pixi env %s: %s", target_dir, e)
            return 0
        return size
