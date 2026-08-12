import logging
import os

import pytest

from ray_pixi.plugin import PixiPlugin


class _Ctx:
    def __init__(self):
        self.py_executable: str = ""
        self.command_prefix: list = []
        self.env_vars: dict = {}


def test_constructible_with_no_args(monkeypatch, tmp_path):
    # Ray loads third-party plugins via `plugin_class()` (no args); the agent
    # receives its resource dir as --runtime-env-dir on the command line.
    monkeypatch.setattr(
        "sys.argv", ["runtime_env_agent", "--runtime-env-dir", str(tmp_path)]
    )
    plugin = PixiPlugin()
    assert plugin._resource_dir == os.path.join(str(tmp_path), "pixi")


def test_get_uris_inline_returns_uri(tmp_path):
    plugin = PixiPlugin(str(tmp_path))
    uris = plugin.get_uris({"pixi": {"channels": ["conda-forge"]}})
    assert len(uris) == 1 and uris[0].startswith("pixi://")


def test_get_uris_project_uses_working_dir_uri_without_files(tmp_path, monkeypatch):
    # In the agent's increase_reference phase, working_dir is not yet downloaded
    # and RAY_RUNTIME_ENV_CREATE_WORKING_DIR is unset. get_uris must still
    # succeed by deriving the URI from the working_dir URI Ray already computed.
    monkeypatch.delenv("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", raising=False)
    plugin = PixiPlugin(str(tmp_path / "res"))
    uris = plugin.get_uris(
        {"pixi": {"manifest": "pixi.toml"}, "working_dir": "gcs://_ray_pkg_abc.zip"}
    )
    assert len(uris) == 1 and uris[0].startswith("pixi://")


def test_get_uris_project_differs_by_working_dir_uri(tmp_path):
    plugin = PixiPlugin(str(tmp_path / "res"))
    a = plugin.get_uris(
        {"pixi": {"manifest": "pixi.toml"}, "working_dir": "gcs://_ray_pkg_aaa.zip"}
    )
    b = plugin.get_uris(
        {"pixi": {"manifest": "pixi.toml"}, "working_dir": "gcs://_ray_pkg_bbb.zip"}
    )
    same = plugin.get_uris(
        {"pixi": {"manifest": "pixi.toml"}, "working_dir": "gcs://_ray_pkg_aaa.zip"}
    )
    assert a[0] != b[0]
    assert a[0] == same[0]


def test_get_uris_never_raises_on_bad_input(tmp_path):
    # Ray's ReferenceTable.uris_parser calls get_uris on every plugin for every
    # runtime env, and decrease_reference calls it after already decrementing the
    # env refcount. A raise there fails DeleteRuntimeEnvIfPossible ("Delete
    # runtime env failed" in the raylet log) and leaks the URI refcount forever,
    # so delete_uri never runs. validate() rejects bad input on the driver.
    plugin = PixiPlugin(str(tmp_path))
    assert plugin.get_uris({"pixi": {"manifest": "pixi.toml"}}) == []
    assert plugin.get_uris({"pixi": {"manifest": "p.toml", "channels": ["x"]}}) == []
    assert plugin.get_uris({"pixi": "not-a-dict"}) == []
    assert plugin.get_uris({"pixi": {"environment": object()}}) == []


def test_get_uris_empty_without_field(tmp_path):
    plugin = PixiPlugin(str(tmp_path))
    assert plugin.get_uris({}) == []


def test_validate_delegates(tmp_path):
    with pytest.raises(ValueError):
        PixiPlugin.validate({"pixi": {"manifest": "p.toml", "channels": ["x"]}})


def test_validate_project_requires_working_dir():
    # Catch the missing working_dir on the driver, not in the agent's get_uris.
    with pytest.raises(ValueError, match="working_dir"):
        PixiPlugin.validate({"pixi": {"manifest": "p.toml"}})


def test_validate_project_with_working_dir_ok():
    PixiPlugin.validate({"pixi": {"manifest": "p.toml"}, "working_dir": "gcs://x.zip"})


def test_validate_inline_without_working_dir_ok():
    PixiPlugin.validate({"pixi": {"channels": ["conda-forge"]}})


def test_modify_context_inline_sets_py_executable(tmp_path, monkeypatch):
    plugin = PixiPlugin(str(tmp_path))
    field = {"channels": ["conda-forge"], "environment": "default"}
    uri = plugin.get_uris({"pixi": field})[0]
    target = plugin._target_dir(uri)
    os.makedirs(os.path.join(target, ".pixi", "envs", "default"), exist_ok=True)

    import ray_pixi.binary as binary_mod

    monkeypatch.setattr(binary_mod, "resolve_pixi", lambda td, v: "/fake/pixi")

    ctx = _Ctx()
    plugin.modify_context([uri], {"pixi": field}, ctx, logging.getLogger("t"))
    assert ctx.py_executable.startswith("/fake/pixi run --manifest-path")
    # The env was fully installed by create(); worker startup must not
    # re-solve or re-install (slow, and concurrent workers would race).
    assert "--frozen" in ctx.py_executable
    assert "--no-install" in ctx.py_executable
    assert ctx.py_executable.endswith("-e default python")


def _project_wd(tmp_path, monkeypatch):
    wd = tmp_path / "wd"
    wd.mkdir()
    (wd / "pixi.toml").write_text("[workspace]\n")
    (wd / "pixi.lock").write_text("version: 6\n")
    monkeypatch.setenv("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", str(wd))
    return wd


def _env_creating_proc(runs):
    class FakeProc:
        def __init__(self, target, manifest_path, pixi_spec, pixi_exe, logger, **kw):
            self._target = target

        async def run(self):
            runs.append(self._target)
            os.makedirs(
                os.path.join(self._target, ".pixi", "envs", "default"), exist_ok=True
            )

    return FakeProc


def test_modify_context_project_resolves_store_manifest(tmp_path, monkeypatch):
    import asyncio

    import ray_pixi.binary as binary_mod
    import ray_pixi.plugin as plugin_mod

    _project_wd(tmp_path, monkeypatch)
    plugin = PixiPlugin(str(tmp_path / "res"))
    field = {"manifest": "pixi.toml", "environment": "default"}
    runtime_env = {"pixi": field, "working_dir": "gcs://_ray_pkg_x.zip"}
    uri = plugin.get_uris(runtime_env)[0]

    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")
    monkeypatch.setattr(plugin_mod, "PixiProcessor", _env_creating_proc([]))
    asyncio.run(plugin.create(uri, runtime_env, None, logging.getLogger("t")))

    monkeypatch.setattr(binary_mod, "resolve_pixi", lambda td, v: "/fake/pixi")
    ctx = _Ctx()
    plugin.modify_context([uri], runtime_env, ctx, logging.getLogger("t"))
    store_root = os.path.join(plugin._resource_dir, "store")
    assert ctx.py_executable.startswith(f"/fake/pixi run --manifest-path {store_root}")
    assert "--frozen" in ctx.py_executable
    assert "--no-install" in ctx.py_executable
    assert ctx.py_executable.endswith("-e default python")


def test_create_project_dedups_by_content(tmp_path, monkeypatch):
    # Same env-defining content reached via two different working_dir uploads
    # (e.g. the driver script changed) must install the environment only once.
    import asyncio

    import ray_pixi.plugin as plugin_mod

    _project_wd(tmp_path, monkeypatch)
    plugin = PixiPlugin(str(tmp_path / "res"))
    field = {"manifest": "pixi.toml"}
    env1 = {"pixi": field, "working_dir": "gcs://_ray_pkg_aaa.zip"}
    env2 = {"pixi": field, "working_dir": "gcs://_ray_pkg_bbb.zip"}
    uri1 = plugin.get_uris(env1)[0]
    uri2 = plugin.get_uris(env2)[0]
    assert uri1 != uri2

    runs = []
    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")
    monkeypatch.setattr(plugin_mod, "PixiProcessor", _env_creating_proc(runs))

    async def main():
        await plugin.create(uri1, env1, None, logging.getLogger("t"))
        await plugin.create(uri2, env2, None, logging.getLogger("t"))

    asyncio.run(main())

    assert len(runs) == 1
    with open(os.path.join(plugin._target_dir(uri1), "STORE")) as f:
        p1 = f.read().strip()
    with open(os.path.join(plugin._target_dir(uri2), "STORE")) as f:
        p2 = f.read().strip()
    assert p1 == p2
    store_dir = os.path.join(plugin._resource_dir, "store", p1)
    assert os.path.exists(os.path.join(store_dir, "pixi.toml"))


def test_delete_uri_keeps_store_until_last_reference(tmp_path, monkeypatch):
    import asyncio

    import ray_pixi.plugin as plugin_mod

    _project_wd(tmp_path, monkeypatch)
    plugin = PixiPlugin(str(tmp_path / "res"))
    field = {"manifest": "pixi.toml"}
    env1 = {"pixi": field, "working_dir": "gcs://_ray_pkg_aaa.zip"}
    env2 = {"pixi": field, "working_dir": "gcs://_ray_pkg_bbb.zip"}
    uri1 = plugin.get_uris(env1)[0]
    uri2 = plugin.get_uris(env2)[0]

    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")
    monkeypatch.setattr(plugin_mod, "PixiProcessor", _env_creating_proc([]))

    async def main():
        await plugin.create(uri1, env1, None, logging.getLogger("t"))
        await plugin.create(uri2, env2, None, logging.getLogger("t"))

    asyncio.run(main())
    with open(os.path.join(plugin._target_dir(uri1), "STORE")) as f:
        store_dir = os.path.join(plugin._resource_dir, "store", f.read().strip())

    plugin.delete_uri(uri1, logging.getLogger("t"))
    assert not os.path.exists(plugin._target_dir(uri1))
    assert os.path.exists(store_dir)  # uri2 still references it

    plugin.delete_uri(uri2, logging.getLogger("t"))
    assert not os.path.exists(plugin._target_dir(uri2))
    assert not os.path.exists(store_dir)


def test_size_accounting_balances_for_shared_store(tmp_path, monkeypatch):
    # Ray's URICache adds create()'s return per URI and subtracts delete_uri()'s
    # return on eviction. A reuse create must not re-report the store's size:
    # the books would gain the store size once per working_dir edit but give it
    # back only once at final GC, drifting upward until everything evicts.
    import asyncio

    import ray_pixi.plugin as plugin_mod

    _project_wd(tmp_path, monkeypatch)
    plugin = PixiPlugin(str(tmp_path / "res"))
    field = {"manifest": "pixi.toml"}
    env1 = {"pixi": field, "working_dir": "gcs://_ray_pkg_aaa.zip"}
    env2 = {"pixi": field, "working_dir": "gcs://_ray_pkg_bbb.zip"}
    uri1 = plugin.get_uris(env1)[0]
    uri2 = plugin.get_uris(env2)[0]

    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")
    monkeypatch.setattr(plugin_mod, "PixiProcessor", _env_creating_proc([]))

    async def main():
        s1 = await plugin.create(uri1, env1, None, logging.getLogger("t"))
        s2 = await plugin.create(uri2, env2, None, logging.getLogger("t"))
        return s1, s2

    size1, size2 = asyncio.run(main())
    assert size2 < size1  # reuse reports the pointer dir, not the store again

    freed1 = plugin.delete_uri(uri1, logging.getLogger("t"))
    freed2 = plugin.delete_uri(uri2, logging.getLogger("t"))
    assert size1 + size2 == freed1 + freed2


def test_atomic_write_never_leaves_partial_file(tmp_path, monkeypatch):
    # An agent kill mid-write must never leave a truncated pointer file: GC is
    # keyed off pointer contents, so a garbage pointer orphans its store entry.
    import ray_pixi.plugin as plugin_mod

    path = tmp_path / "ptr"
    plugin_mod._atomic_write(str(path), "aaa")
    assert path.read_text() == "aaa"

    def boom(src, dst):
        raise OSError("killed before rename")

    monkeypatch.setattr(plugin_mod.os, "replace", boom)
    with pytest.raises(OSError, match="killed before rename"):
        plugin_mod._atomic_write(str(path), "bbb")
    monkeypatch.undo()
    assert path.read_text() == "aaa"  # previous content intact, not truncated
    assert os.listdir(tmp_path) == ["ptr"]  # no temp litter


def test_create_project_writes_pointer_atomically(tmp_path, monkeypatch):
    import asyncio

    import ray_pixi.plugin as plugin_mod

    _project_wd(tmp_path, monkeypatch)
    plugin = PixiPlugin(str(tmp_path / "res"))
    runtime_env = {"pixi": {"manifest": "pixi.toml"}, "working_dir": "gcs://_a.zip"}
    uri = plugin.get_uris(runtime_env)[0]

    written = []
    real = plugin_mod._atomic_write
    monkeypatch.setattr(
        plugin_mod, "_atomic_write", lambda p, c: (written.append(p), real(p, c))[1]
    )
    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")
    monkeypatch.setattr(plugin_mod, "PixiProcessor", _env_creating_proc([]))
    asyncio.run(plugin.create(uri, runtime_env, None, logging.getLogger("t")))

    assert any(p.endswith(plugin_mod._POINTER_FILE) for p in written)


def test_install_log_lands_in_session_log_dir(tmp_path, monkeypatch):
    # The dashboard's Logs tab only serves the session logs dir
    # (/tmp/ray/session_*/logs); the agent receives it as --log-dir. Logs go
    # into a pixi/ subfolder: browsable in the dashboard (dirs are listed and
    # navigable), and out of reach of the top-level globs the log monitor
    # streams to drivers (worker-*, runtime_env*.log, ...).
    import asyncio

    import ray_pixi.manifest as manifest_mod
    import ray_pixi.plugin as plugin_mod

    log_dir = tmp_path / "logs"
    monkeypatch.setattr(
        "sys.argv",
        [
            "runtime_env_agent",
            "--runtime-env-dir",
            str(tmp_path / "res"),
            "--log-dir",
            str(log_dir),
        ],
    )
    plugin = PixiPlugin()
    field = {"channels": ["conda-forge"]}
    uri = plugin.get_uris({"pixi": field})[0]

    seen = {}
    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")
    monkeypatch.setattr(
        manifest_mod, "materialize", lambda s, t: os.path.join(t, "pixi.toml")
    )

    class FakeProc:
        def __init__(self, *a, **k):
            seen["log_path"] = k.get("log_path")

        async def run(self):
            pass

    monkeypatch.setattr(plugin_mod, "PixiProcessor", FakeProc)
    asyncio.run(plugin.create(uri, {"pixi": field}, None, logging.getLogger("t")))

    import re

    # A fresh timestamped file per install attempt: retries do not append to
    # (or overwrite) a previous attempt's log.
    assert os.path.dirname(seen["log_path"]) == os.path.join(str(log_dir), "pixi")
    assert re.fullmatch(
        rf"install-\d{{14}}-{plugin._hash_of(uri)}\.log",
        os.path.basename(seen["log_path"]),
    )


def test_delete_uri_removes_install_logs(tmp_path, monkeypatch):
    import asyncio

    import ray_pixi.plugin as plugin_mod

    _project_wd(tmp_path, monkeypatch)
    plugin = PixiPlugin(str(tmp_path / "res"))
    field = {"manifest": "pixi.toml"}
    runtime_env = {"pixi": field, "working_dir": "gcs://_ray_pkg_x.zip"}
    uri = plugin.get_uris(runtime_env)[0]

    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")
    monkeypatch.setattr(plugin_mod, "PixiProcessor", _env_creating_proc([]))
    asyncio.run(plugin.create(uri, runtime_env, None, logging.getLogger("t")))

    with open(os.path.join(plugin._target_dir(uri), "STORE")) as f:
        store_hash = f.read().strip()
    store_dir = os.path.join(plugin._resource_dir, "store", store_hash)
    # Two timestamped attempts for the same env: both must be cleaned up.
    logs = [
        os.path.join(plugin._log_dir, f"install-2099010100000{i}-{store_hash}.log")
        for i in (1, 2)
    ]
    for log in logs:
        open(log, "w").close()

    plugin.delete_uri(uri, logging.getLogger("t"))
    assert not os.path.exists(store_dir)
    assert not any(os.path.exists(log) for log in logs)


def test_create_project_failure_cleans_store(tmp_path, monkeypatch):
    import asyncio

    import ray_pixi.plugin as plugin_mod

    monkeypatch.delenv("RAY_PIXI_KEEP_ON_FAILURE", raising=False)
    _project_wd(tmp_path, monkeypatch)
    plugin = PixiPlugin(str(tmp_path / "res"))
    field = {"manifest": "pixi.toml"}
    runtime_env = {"pixi": field, "working_dir": "gcs://_ray_pkg_x.zip"}
    uri = plugin.get_uris(runtime_env)[0]

    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")

    class FailingProc:
        def __init__(self, *a, **k):
            pass

        async def run(self):
            raise RuntimeError("install failed")

    monkeypatch.setattr(plugin_mod, "PixiProcessor", FailingProc)
    with pytest.raises(RuntimeError, match="install failed"):
        asyncio.run(plugin.create(uri, runtime_env, None, logging.getLogger("t")))

    store_root = os.path.join(plugin._resource_dir, "store")
    assert not os.path.exists(store_root) or os.listdir(store_root) == []
    assert not os.path.exists(os.path.join(plugin._target_dir(uri), "STORE"))


def test_modify_context_versioned_pixi_missing_bootstrap_raises(tmp_path):
    # modify_context is a synchronous hook: it must never download pixi; a
    # missing bootstrap is an error, not a trigger to install one.
    plugin = PixiPlugin(str(tmp_path))
    field = {"channels": ["conda-forge"], "pixi_version": "0.40.0"}
    uri = plugin.get_uris({"pixi": field})[0]
    target = plugin._target_dir(uri)
    os.makedirs(os.path.join(target, ".pixi", "envs", "default"), exist_ok=True)
    with pytest.raises(ValueError, match="bootstrapped pixi"):
        plugin.modify_context([uri], {"pixi": field}, _Ctx(), logging.getLogger("t"))


def test_modify_context_missing_env_raises(tmp_path, monkeypatch):
    plugin = PixiPlugin(str(tmp_path))
    field = {"channels": ["conda-forge"]}
    uri = plugin.get_uris({"pixi": field})[0]
    import ray_pixi.binary as binary_mod

    monkeypatch.setattr(binary_mod, "resolve_pixi", lambda td, v: "/fake/pixi")
    with pytest.raises(ValueError, match="does not exist"):
        plugin.modify_context([uri], {"pixi": field}, _Ctx(), logging.getLogger("t"))


def test_create_inline_materializes_and_runs(tmp_path, monkeypatch):
    import asyncio

    import ray_pixi.manifest as manifest_mod
    import ray_pixi.plugin as plugin_mod

    plugin = PixiPlugin(str(tmp_path / "res"))
    field = {"channels": ["conda-forge"]}
    uri = plugin.get_uris({"pixi": field})[0]

    seen = {}
    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")
    monkeypatch.setattr(
        manifest_mod, "materialize", lambda s, t: os.path.join(t, "pixi.toml")
    )

    class FakeProc:
        def __init__(self, target, manifest_path, pixi_spec, pixi_exe, logger, **kw):
            seen["manifest_path"] = manifest_path
            seen["pixi_exe"] = pixi_exe

        async def run(self):
            seen["ran"] = True

    monkeypatch.setattr(plugin_mod, "PixiProcessor", FakeProc)
    size = asyncio.run(
        plugin.create(uri, {"pixi": field}, None, logging.getLogger("t"))
    )
    assert seen["ran"] is True
    assert seen["pixi_exe"] == "/fake/pixi"
    assert seen["manifest_path"].endswith("pixi.toml")
    assert isinstance(size, int)


def test_create_project_materializes_from_working_dir(tmp_path, monkeypatch):
    import asyncio

    import ray_pixi.plugin as plugin_mod
    import ray_pixi.project as project_mod

    wd = tmp_path / "wd"
    wd.mkdir()
    (wd / "pixi.toml").write_text("[workspace]\n")
    monkeypatch.setenv("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", str(wd))
    plugin = PixiPlugin(str(tmp_path / "res"))
    field = {"manifest": "pixi.toml"}
    runtime_env = {"pixi": field, "working_dir": "gcs://_ray_pkg_x.zip"}
    uri = plugin.get_uris(runtime_env)[0]

    seen = {}
    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")

    def fake_materialize_project(s, working_dir, target):
        seen["working_dir"] = working_dir
        return os.path.join(target, "pixi.toml")

    monkeypatch.setattr(project_mod, "materialize_project", fake_materialize_project)

    class FakeProc:
        def __init__(self, *a, **k):
            seen["ran_init"] = True

        async def run(self):
            seen["ran"] = True

    monkeypatch.setattr(plugin_mod, "PixiProcessor", FakeProc)
    asyncio.run(plugin.create(uri, runtime_env, None, logging.getLogger("t")))
    assert seen["ran"] is True
    assert seen["working_dir"] == str(wd)


def test_create_does_not_block_event_loop(tmp_path, monkeypatch):
    # Resolving pixi may download and run an installer (minutes). The agent's
    # event loop must stay responsive while that happens.
    import asyncio
    import time

    import ray_pixi.binary as binary_mod
    import ray_pixi.manifest as manifest_mod
    import ray_pixi.plugin as plugin_mod

    plugin = PixiPlugin(str(tmp_path / "res"))
    field = {"channels": ["conda-forge"]}
    uri = plugin.get_uris({"pixi": field})[0]

    def slow_resolve(target_dir, version):
        time.sleep(0.3)
        return "/fake/pixi"

    monkeypatch.setattr(binary_mod, "resolve_pixi", slow_resolve)
    monkeypatch.setattr(
        manifest_mod, "materialize", lambda s, t: os.path.join(t, "pixi.toml")
    )

    class FakeProc:
        def __init__(self, *a, **k):
            pass

        async def run(self):
            pass

    monkeypatch.setattr(plugin_mod, "PixiProcessor", FakeProc)

    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    async def main():
        tick_task = asyncio.create_task(ticker())
        await plugin.create(uri, {"pixi": field}, None, logging.getLogger("t"))
        tick_task.cancel()

    asyncio.run(main())
    assert ticks >= 10


def test_concurrent_bootstrap_same_version_serialized(tmp_path, monkeypatch):
    # Two different envs pinning the same pixi_version share one bootstrap dir;
    # concurrent creates must not run the installer into it at the same time.
    import asyncio
    import time

    import ray_pixi.binary as binary_mod
    import ray_pixi.manifest as manifest_mod
    import ray_pixi.plugin as plugin_mod

    plugin = PixiPlugin(str(tmp_path / "res"))
    f1 = {"channels": ["a"], "pixi_version": "0.40.0"}
    f2 = {"channels": ["b"], "pixi_version": "0.40.0"}
    uri1 = plugin.get_uris({"pixi": f1})[0]
    uri2 = plugin.get_uris({"pixi": f2})[0]

    active = 0
    max_active = 0

    def fake_resolve(target_dir, version):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        time.sleep(0.05)
        active -= 1
        return "/fake/pixi"

    monkeypatch.setattr(binary_mod, "resolve_pixi", fake_resolve)
    monkeypatch.setattr(
        manifest_mod, "materialize", lambda s, t: os.path.join(t, "pixi.toml")
    )

    class FakeProc:
        def __init__(self, *a, **k):
            pass

        async def run(self):
            pass

    monkeypatch.setattr(plugin_mod, "PixiProcessor", FakeProc)

    async def main():
        await asyncio.gather(
            plugin.create(uri1, {"pixi": f1}, None, logging.getLogger("t")),
            plugin.create(uri2, {"pixi": f2}, None, logging.getLogger("t")),
        )

    asyncio.run(main())
    assert max_active == 1


def _run_failing_create(plugin, monkeypatch):
    import asyncio

    import ray_pixi.manifest as manifest_mod
    import ray_pixi.plugin as plugin_mod

    field = {"channels": ["conda-forge"]}
    uri = plugin.get_uris({"pixi": field})[0]
    target = plugin._target_dir(uri)

    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")
    monkeypatch.setattr(
        manifest_mod, "materialize", lambda s, t: os.path.join(t, "pixi.toml")
    )

    class FakeProc:
        def __init__(self, *a, **k):
            pass

        async def run(self):
            raise RuntimeError("install failed")

    monkeypatch.setattr(plugin_mod, "PixiProcessor", FakeProc)
    with pytest.raises(RuntimeError, match="install failed"):
        asyncio.run(plugin.create(uri, {"pixi": field}, None, logging.getLogger("t")))
    return target


def test_create_cleans_up_target_on_failure_by_default(tmp_path, monkeypatch):
    # Failures remove the target dir by default to reclaim space.
    monkeypatch.delenv("RAY_PIXI_KEEP_ON_FAILURE", raising=False)
    plugin = PixiPlugin(str(tmp_path / "res"))
    target = _run_failing_create(plugin, monkeypatch)
    assert not os.path.exists(target)


def test_create_keeps_target_on_failure_when_enabled(tmp_path, monkeypatch):
    # Set RAY_PIXI_KEEP_ON_FAILURE=1 to keep the broken install for inspection
    # (e.g. a worker pod's pixi build that failed).
    monkeypatch.setenv("RAY_PIXI_KEEP_ON_FAILURE", "1")
    plugin = PixiPlugin(str(tmp_path / "res"))
    target = _run_failing_create(plugin, monkeypatch)
    assert os.path.exists(target)


def test_modify_context_quotes_paths_with_spaces(tmp_path, monkeypatch):
    # Ray splices py_executable unquoted into `bash -c`; a space in the
    # resource dir (or session path) must not split the manifest path.
    import asyncio
    import shlex

    import ray_pixi.binary as binary_mod
    import ray_pixi.plugin as plugin_mod

    _project_wd(tmp_path, monkeypatch)
    plugin = PixiPlugin(str(tmp_path / "re s"))  # resource dir with a space
    runtime_env = {"pixi": {"manifest": "pixi.toml"}, "working_dir": "gcs://_a.zip"}
    uri = plugin.get_uris(runtime_env)[0]
    monkeypatch.setattr(plugin, "_resolve_pixi", lambda s, t: "/fake/pixi")
    monkeypatch.setattr(plugin_mod, "PixiProcessor", _env_creating_proc([]))
    asyncio.run(plugin.create(uri, runtime_env, None, logging.getLogger("t")))

    monkeypatch.setattr(binary_mod, "resolve_pixi", lambda td, v: "/fake/pixi")
    ctx = _Ctx()
    plugin.modify_context([uri], runtime_env, ctx, logging.getLogger("t"))

    tokens = shlex.split(ctx.py_executable)
    manifest_token = tokens[tokens.index("--manifest-path") + 1]
    assert " " in manifest_token  # spaced path survived shell splitting whole
    assert os.path.exists(manifest_token)


def test_init_sweeps_incomplete_store_entries(tmp_path):
    # An agent crash mid-install leaves a store entry without the OK marker;
    # nothing references it and create() would never reuse it, so it is dead
    # weight that no GC path covers. Complete entries stay: a future create
    # with the same content hash re-adopts them.
    res = str(tmp_path / "res")
    store = tmp_path / "res" / "pixi" / "store"
    complete = store / "aaa"
    partial = store / "bbb"
    os.makedirs(complete)
    os.makedirs(partial)
    (complete / ".ray-pixi-ok").write_text("pixi://aaa")
    (partial / "pixi.toml").write_text("[workspace]\n")
    log_pixi_dir = tmp_path / "logs" / "pixi"
    os.makedirs(log_pixi_dir)
    stale_log = log_pixi_dir / "install-20990101000000-bbb.log"
    stale_log.write_text("partial install output\n")

    PixiPlugin(res, str(tmp_path / "logs"))

    assert os.path.isdir(complete)
    assert not os.path.exists(partial)
    assert not stale_log.exists()


def test_reference_table_decrease_balances_with_ray(tmp_path):
    # End-to-end against Ray's real ReferenceTable: an exception from get_uris
    # during decrease_reference is what surfaced as the raylet's "Delete runtime
    # env failed". Project mode without working_dir is the shape Ray Client sent.
    from ray._private.runtime_env.agent.runtime_env_agent import ReferenceTable
    from ray.runtime_env import RuntimeEnv

    plugin = PixiPlugin(str(tmp_path))
    env = RuntimeEnv(pixi={"manifest": "pixi.toml"})
    serialized = env.serialize()
    # Mirrors the agent's own uris_parser, whose annotation says Tuple but which
    # actually returns a list of (uri, type) pairs for every plugin.
    table = ReferenceTable(
        uris_parser=lambda re: [(u, "pixi") for u in plugin.get_uris(re)],  # ty: ignore[invalid-argument-type]
        unused_uris_callback=lambda uris: None,
        unused_runtime_env_callback=lambda e: None,
    )
    table.increase_reference(env, serialized, "raylet")
    table.decrease_reference(env, serialized, "raylet")  # must not raise
    assert table.runtime_env_refs == {}
    assert table._uri_reference == {}
