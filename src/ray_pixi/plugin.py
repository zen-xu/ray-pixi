"""PixiPlugin: the Ray runtime_env plugin for the 'pixi' field."""

from __future__ import annotations

import asyncio
import contextlib
import glob
import logging
import os
import shlex
import shutil
import sys
import tempfile
from datetime import datetime

from ray._common.utils import try_to_create_directory
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import get_directory_size_bytes

from ray_pixi import binary, manifest, project, spec
from ray_pixi.processor import PixiProcessor

default_logger = logging.getLogger(__name__)

# Project-mode target dirs hold a single pointer file naming the store entry
# (content hash of the env-defining file subset) the environment lives in.
_POINTER_FILE = "STORE"
# Written into a store entry after a successful install + verification.
_OK_MARKER = ".ray-pixi-ok"


def _atomic_write(path: str, content: str) -> None:
    """Write content to path so a crash never leaves a partial file.

    Pointer files are read back by GC to decide whether a store entry is still
    referenced; a truncated pointer would orphan multi-GB store entries.
    """
    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(content)
        os.replace(tmp_path, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp_path)
        raise


def _argv_flag(flag: str) -> str | None:
    """Read a CLI flag value from this process's argv, or None.

    Third-party plugins loaded via RAY_RUNTIME_ENV_PLUGINS are constructed
    with no arguments, so agent settings are not injected; the agent receives
    them as CLI arguments (e.g. ``--runtime-env-dir``, ``--log-dir``).
    """
    argv = sys.argv
    if flag in argv:
        index = argv.index(flag)
        if index + 1 < len(argv):
            return argv[index + 1]
    for arg in argv:
        if arg.startswith(f"{flag}="):
            return arg.split("=", 1)[1]
    return None


def _agent_resources_dir() -> str:
    """Resolve the runtime_env resource dir the agent was started with."""
    return _argv_flag("--runtime-env-dir") or os.path.join(
        tempfile.gettempdir(), "ray_pixi"
    )


class PixiPlugin(RuntimeEnvPlugin):
    name = "pixi"

    def __init__(self, resources_dir: str | None = None, log_dir: str | None = None):
        if resources_dir is None:
            resources_dir = _agent_resources_dir()
        self._resource_dir = os.path.join(resources_dir, "pixi")
        if log_dir is None:
            # The agent's --log-dir is the session logs dir
            # (/tmp/ray/session_*/logs), the only dir the dashboard's Logs tab
            # serves; install logs go there to be viewable from the web UI.
            log_dir = _argv_flag("--log-dir") or os.path.join(
                self._resource_dir, "logs"
            )
        self._log_dir = os.path.join(log_dir, "pixi")
        self._creating_task: dict[str, asyncio.Task] = {}
        # One lock per URI prevents concurrent installs of the same environment.
        self._create_locks: dict[str, asyncio.Lock] = {}
        # One lock per pixi_version: the bootstrap dir is shared across URIs.
        self._bootstrap_locks: dict[str, asyncio.Lock] = {}
        # One lock per store hash: several URIs may share one store entry.
        self._store_locks: dict[str, asyncio.Lock] = {}
        # Maps a created hash to the size in bytes of its installed directory.
        self._created_hash_bytes: dict[str, int] = {}
        try_to_create_directory(self._resource_dir)
        try_to_create_directory(self._log_dir)
        self._sweep_incomplete_stores()

    def _sweep_incomplete_stores(self) -> None:
        """Remove store entries left half-installed by an agent crash.

        Entries without the OK marker can never be reused by create() and no
        GC path covers them (GC is keyed off pointer files). Complete entries
        stay: a future create with the same content hash re-adopts them.
        """
        store_root = os.path.join(self._resource_dir, "store")
        if not os.path.isdir(store_root):
            return
        for entry in os.listdir(store_root):
            entry_dir = os.path.join(store_root, entry)
            if not os.path.isdir(entry_dir) or os.path.exists(
                os.path.join(entry_dir, _OK_MARKER)
            ):
                continue
            default_logger.info(
                "Sweeping incomplete pixi store entry %s (no OK marker).", entry_dir
            )
            shutil.rmtree(entry_dir, ignore_errors=True)
            self._remove_install_logs_for(entry)

    @staticmethod
    def validate(runtime_env_dict: dict) -> None:
        field = runtime_env_dict.get("pixi")
        if field is None:
            return
        pixi_spec = spec.normalize(field)
        # Fail on the driver instead of deep inside the agent's get_uris.
        if pixi_spec.source == "project" and not runtime_env_dict.get("working_dir"):
            raise ValueError(
                "pixi project mode requires runtime_env['working_dir'] so the "
                "manifest and sources reach the workers."
            )

    def _hash_of(self, uri: str) -> str:
        return uri.split("://", 1)[1]

    def _target_dir(self, uri: str) -> str:
        return os.path.join(self._resource_dir, self._hash_of(uri))

    def _store_dir(self, store_hash: str) -> str:
        return os.path.join(self._resource_dir, "store", store_hash)

    def _pointer_of(self, target_dir: str) -> str | None:
        """Read the store hash a project-mode target dir points at, or None."""
        try:
            with open(os.path.join(target_dir, _POINTER_FILE)) as f:
                return f.read().strip() or None
        except OSError:
            return None

    def _env_root(self, target_dir: str) -> str:
        """Dir holding the manifest and .pixi env (the store entry if pointed)."""
        store_hash = self._pointer_of(target_dir)
        return self._store_dir(store_hash) if store_hash else target_dir

    def _new_install_log_path(self, env_hash: str) -> str:
        """Fresh install log in the pixi/ subfolder of the session logs dir.

        Browsable from the dashboard's Logs tab (dirs are listed and files in
        them servable), while staying out of the top-level globs Ray's log
        monitor streams to drivers/clients (worker-*, runtime_env*.log, ...).
        Timestamped per install attempt so a retry never appends to a previous
        attempt's log.
        """
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return os.path.join(self._log_dir, f"install-{stamp}-{env_hash}.log")

    def _remove_install_logs_for(self, env_hash: str) -> None:
        """Remove the logs of all install attempts of the given env."""
        for path in glob.glob(os.path.join(self._log_dir, f"install-*-{env_hash}.log")):
            with contextlib.suppress(OSError):
                os.remove(path)

    def get_uris(self, runtime_env) -> list[str]:
        # Must never raise. Ray's ReferenceTable.uris_parser calls get_uris on
        # every registered plugin for every runtime env, and decrease_reference
        # calls it *after* already decrementing the env-level refcount. An
        # exception here therefore both fails the DeleteRuntimeEnvIfPossible RPC
        # (raylet logs "Delete runtime env failed") and permanently leaks the URI
        # refcount, so delete_uri never runs and envs are never GC'd. Malformed
        # input is rejected by validate() on the driver instead.
        field = runtime_env.get("pixi")
        if not field:
            return []
        try:
            pixi_spec = spec.normalize(field)
            if pixi_spec.source == "inline":
                return [spec.compute_uri(field)]
            # get_uris runs in the agent's increase_reference phase, before Ray
            # has downloaded the working_dir, so its files cannot be read here.
            # Derive the URI from the working_dir URI Ray already computed;
            # create() then dedups actual installs by the env-defining content
            # hash (the store).
            working_dir_uri = runtime_env.get("working_dir", "")
            if not working_dir_uri:
                return []
            return [
                project.compute_project_uri_from_working_dir_uri(
                    pixi_spec, working_dir_uri
                )
            ]
        except Exception:
            return []

    def _resolve_pixi(self, pixi_spec: spec.PixiSpec, target_dir: str) -> str:
        if pixi_spec.pixi_version:
            bootstrap_dir = os.path.join(
                self._resource_dir, "pixi-bin", pixi_spec.pixi_version
            )
            return binary.resolve_pixi(bootstrap_dir, pixi_spec.pixi_version)
        return binary.resolve_pixi(target_dir, None)

    async def _resolve_pixi_async(
        self, pixi_spec: spec.PixiSpec, target_dir: str
    ) -> str:
        """Resolve pixi off the event loop, serializing per-version bootstraps."""
        loop = asyncio.get_running_loop()
        if pixi_spec.pixi_version:
            lock = self._bootstrap_locks.setdefault(
                pixi_spec.pixi_version, asyncio.Lock()
            )
            async with lock:
                return await loop.run_in_executor(
                    None, self._resolve_pixi, pixi_spec, target_dir
                )
        return await loop.run_in_executor(
            None, self._resolve_pixi, pixi_spec, target_dir
        )

    def _cleanup_failed(self, path: str, logger: logging.Logger) -> None:
        logger.exception("Failed to install pixi environment.")
        # Remove the broken dir by default to reclaim space. Set
        # RAY_PIXI_KEEP_ON_FAILURE=1 to keep it so the failed install can be
        # inspected (e.g. a worker pod's pixi build error).
        if os.environ.get("RAY_PIXI_KEEP_ON_FAILURE") == "1":
            logger.warning(
                "Keeping %s for inspection; unset RAY_PIXI_KEEP_ON_FAILURE to "
                "remove it on failure.",
                path,
            )
        else:
            shutil.rmtree(path, ignore_errors=True)

    async def _install_into(
        self,
        pixi_spec: spec.PixiSpec,
        env_root: str,
        materialize_manifest,
        logger: logging.Logger,
    ) -> None:
        """Resolve pixi, materialize the manifest into env_root and install."""
        os.makedirs(env_root, exist_ok=True)
        loop = asyncio.get_running_loop()
        try:
            # Resolving pixi may download and run an installer; keep that and
            # the file IO off the agent's event loop.
            pixi_exe = await self._resolve_pixi_async(pixi_spec, env_root)
            manifest_path = await loop.run_in_executor(None, materialize_manifest)
            await PixiProcessor(
                env_root,
                manifest_path,
                pixi_spec,
                pixi_exe,
                logger,
                log_path=self._new_install_log_path(os.path.basename(env_root)),
            ).run()
        except Exception:
            self._cleanup_failed(env_root, logger)
            raise

    async def _create_inline(
        self, pixi_spec: spec.PixiSpec, target_dir: str, logger: logging.Logger
    ) -> int:
        await self._install_into(
            pixi_spec,
            target_dir,
            lambda: manifest.materialize(pixi_spec, target_dir),
            logger,
        )
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, get_directory_size_bytes, target_dir)

    async def _create_project(
        self, pixi_spec: spec.PixiSpec, target_dir: str, logger: logging.Logger
    ) -> int:
        # create runs inside Ray's with_working_dir_env context, so the
        # working_dir is downloaded and its local path is exposed via the env
        # var (unlike get_uris, which only sees the URI).
        working_dir = project.resolve_working_dir()
        if not working_dir:
            raise ValueError("pixi project mode requires runtime_env['working_dir'].")
        loop = asyncio.get_running_loop()

        # The plugin URI is derived from the whole working_dir (get_uris cannot
        # read files), so it changes whenever any file changes. Environments are
        # stored by the content hash of the env-defining subset instead, and the
        # target dir just points at the store entry: editing e.g. the driver
        # script yields a new URI but reuses the installed environment.
        store_uri = await loop.run_in_executor(
            None, project.compute_project_uri, pixi_spec, working_dir
        )
        store_hash = self._hash_of(store_uri)
        store_dir = self._store_dir(store_hash)

        lock = self._store_locks.setdefault(store_hash, asyncio.Lock())
        async with lock:
            reused = os.path.exists(os.path.join(store_dir, _OK_MARKER))
            if reused:
                logger.info(
                    "Reusing installed pixi environment %s (content hash hit).",
                    store_dir,
                )
            else:
                await self._install_into(
                    pixi_spec,
                    store_dir,
                    lambda: project.materialize_project(
                        pixi_spec, working_dir, store_dir
                    ),
                    logger,
                )
                _atomic_write(os.path.join(store_dir, _OK_MARKER), store_uri)
            os.makedirs(target_dir, exist_ok=True)
            _atomic_write(os.path.join(target_dir, _POINTER_FILE), store_hash)
        # Report the store's size only from the create that installed it, so
        # the URICache books balance against delete_uri (pointer adds/frees on
        # reuse, plus the store once at install and once at final GC).
        size = await loop.run_in_executor(None, get_directory_size_bytes, target_dir)
        if not reused:
            size += await loop.run_in_executor(
                None, get_directory_size_bytes, store_dir
            )
        return size

    async def create(self, uri, runtime_env, context, logger=default_logger) -> int:
        if not runtime_env.get("pixi"):
            return 0
        pixi_spec = spec.normalize(runtime_env["pixi"])
        target_dir = self._target_dir(uri)

        if uri not in self._create_locks:
            self._create_locks[uri] = asyncio.Lock()
        async with self._create_locks[uri]:
            hash_val = self._hash_of(uri)
            if hash_val in self._created_hash_bytes:
                return self._created_hash_bytes[hash_val]
            if pixi_spec.source == "inline":
                coro = self._create_inline(pixi_spec, target_dir, logger)
            else:
                coro = self._create_project(pixi_spec, target_dir, logger)
            self._creating_task[hash_val] = task = asyncio.create_task(coro)
            task.add_done_callback(lambda _: self._creating_task.pop(hash_val, None))
            size = await task
            self._created_hash_bytes[hash_val] = size
            return size

    def _find_pixi_for_run(self, pixi_spec: spec.PixiSpec) -> str:
        """Locate pixi for worker startup. Never downloads: modify_context is a
        synchronous hook on the agent's event loop and create() has already
        bootstrapped the binary."""
        if pixi_spec.pixi_version:
            bootstrap_dir = os.path.join(
                self._resource_dir, "pixi-bin", pixi_spec.pixi_version
            )
            exe = binary.find_bootstrapped_pixi(bootstrap_dir)
            if not exe:
                raise ValueError(
                    f"bootstrapped pixi {pixi_spec.pixi_version} not found under "
                    f"{bootstrap_dir}; the pixi runtime_env may need to be "
                    "recreated."
                )
            return exe
        return binary.resolve_pixi(self._resource_dir, None)

    def modify_context(self, uris, runtime_env, context, logger=default_logger) -> None:
        if not runtime_env.get("pixi"):
            return
        pixi_spec = spec.normalize(runtime_env["pixi"])
        env_root = self._env_root(self._target_dir(uris[0]))
        env_name = pixi_spec.environment

        env_dir = os.path.join(env_root, ".pixi", "envs", env_name)
        if not os.path.exists(env_dir):
            raise ValueError(
                f"Pixi environment {env_dir} does not exist on the cluster. "
                "Something may have gone wrong while installing the pixi runtime_env."
            )

        pixi_exe = self._find_pixi_for_run(pixi_spec)
        if pixi_spec.source == "inline":
            manifest_path = os.path.join(env_root, "pixi.toml")
        else:
            manifest_path = project.main_manifest_path(pixi_spec, env_root)
        # --frozen --no-install: the env was fully installed by create();
        # worker startup must not re-solve or touch the lockfile (slow, and
        # concurrent workers would race on the shared env dir). Quoted because
        # Ray splices py_executable unquoted into the worker's `bash -c` line.
        context.py_executable = (
            f"{shlex.quote(pixi_exe)} run "
            f"--manifest-path {shlex.quote(manifest_path)} "
            f"--frozen --no-install -e {shlex.quote(env_name)} python"
        )

    def _gc_store(self, store_hash: str, logger: logging.Logger) -> int:
        """Delete the store entry if nothing references it; return bytes freed."""
        lock = self._store_locks.get(store_hash)
        if lock is not None and lock.locked():
            return 0  # an in-flight create is using it
        for entry in os.listdir(self._resource_dir):
            if entry in ("store", "pixi-bin"):
                continue
            if self._pointer_of(os.path.join(self._resource_dir, entry)) == store_hash:
                return 0  # still referenced by another URI
        store_dir = self._store_dir(store_hash)
        if not os.path.exists(store_dir):
            return 0
        size = get_directory_size_bytes(store_dir)
        try:
            shutil.rmtree(store_dir)
        except OSError as e:
            logger.warning("Error deleting pixi store %s: %s", store_dir, e)
            return 0
        self._remove_install_logs_for(store_hash)
        self._store_locks.pop(store_hash, None)
        return size

    def delete_uri(self, uri: str, logger=default_logger) -> int:
        hash_val = self._hash_of(uri)
        task = self._creating_task.pop(hash_val, None)
        if task is not None:
            task.cancel()
        self._created_hash_bytes.pop(hash_val, None)
        self._create_locks.pop(uri, None)
        target_dir = self._target_dir(uri)
        if not os.path.exists(target_dir):
            return 0
        store_hash = self._pointer_of(target_dir)
        size = get_directory_size_bytes(target_dir)
        try:
            shutil.rmtree(target_dir)
        except OSError as e:
            logger.warning("Error deleting pixi env %s: %s", target_dir, e)
            return 0
        self._remove_install_logs_for(hash_val)
        if store_hash:
            size += self._gc_store(store_hash, logger)
        return size
