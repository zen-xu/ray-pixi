import logging
import os

import pytest

from ray_pixi.plugin import PixiPlugin


class _Ctx:
    def __init__(self):
        self.py_executable: str | None = None
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


def test_get_uris_project_without_working_dir_raises(tmp_path):
    plugin = PixiPlugin(str(tmp_path))
    with pytest.raises(ValueError, match="working_dir"):
        plugin.get_uris({"pixi": {"manifest": "pixi.toml"}})


def test_get_uris_empty_without_field(tmp_path):
    plugin = PixiPlugin(str(tmp_path))
    assert plugin.get_uris({}) == []


def test_validate_delegates(tmp_path):
    with pytest.raises(ValueError):
        PixiPlugin.validate({"pixi": {"manifest": "p.toml", "channels": ["x"]}})


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
    assert ctx.py_executable.endswith("-e default python")


def test_modify_context_project_uses_target_manifest(tmp_path, monkeypatch):
    wd = tmp_path / "wd"
    wd.mkdir()
    (wd / "pixi.toml").write_text("[workspace]\n")
    monkeypatch.setenv("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", str(wd))
    plugin = PixiPlugin(str(tmp_path / "res"))
    field = {"manifest": "pixi.toml", "environment": "default"}
    uri = plugin.get_uris({"pixi": field, "working_dir": "gcs://_ray_pkg_x.zip"})[0]
    target = plugin._target_dir(uri)
    os.makedirs(os.path.join(target, ".pixi", "envs", "default"), exist_ok=True)
    open(os.path.join(target, "pixi.toml"), "w").close()

    import ray_pixi.binary as binary_mod

    monkeypatch.setattr(binary_mod, "resolve_pixi", lambda td, v: "/fake/pixi")

    ctx = _Ctx()
    plugin.modify_context([uri], {"pixi": field}, ctx, logging.getLogger("t"))
    assert ctx.py_executable.startswith("/fake/pixi run --manifest-path")
    assert os.path.join(target, "pixi.toml") in ctx.py_executable
    assert ctx.py_executable.endswith("-e default python")


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
        def __init__(self, target, manifest_path, pixi_spec, pixi_exe, logger):
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
