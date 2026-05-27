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


def test_get_uris_returns_pixi_uri(tmp_path):
    plugin = PixiPlugin(str(tmp_path))
    uris = plugin.get_uris({"pixi": {"manifest_content": "[project]\n"}})
    assert len(uris) == 1 and uris[0].startswith("pixi://")


def test_get_uris_empty_without_field(tmp_path):
    plugin = PixiPlugin(str(tmp_path))
    assert plugin.get_uris({}) == []


def test_validate_delegates(tmp_path):
    with pytest.raises(ValueError):
        PixiPlugin.validate({"pixi": {"manifest": "p.toml", "channels": ["x"]}})


def test_modify_context_sets_py_executable(tmp_path, monkeypatch):
    plugin = PixiPlugin(str(tmp_path))
    field = {"manifest_content": "[project]\n", "environment": "default"}
    uri = plugin.get_uris({"pixi": field})[0]
    target = plugin._target_dir(uri)
    os.makedirs(os.path.join(target, ".pixi", "envs", "default"), exist_ok=True)
    open(os.path.join(target, "pixi.toml"), "w").close()

    import ray_pixi.binary as binary_mod

    monkeypatch.setattr(binary_mod, "resolve_pixi", lambda td, v: "/fake/pixi")

    ctx = _Ctx()
    plugin.modify_context([uri], {"pixi": field}, ctx, logging.getLogger("t"))
    assert ctx.py_executable is not None
    assert ctx.py_executable.startswith("/fake/pixi run --manifest-path")
    assert ctx.py_executable.endswith("-e default python")


def test_modify_context_missing_env_raises(tmp_path):
    plugin = PixiPlugin(str(tmp_path))
    field = {"manifest_content": "[project]\n"}
    uri = plugin.get_uris({"pixi": field})[0]
    with pytest.raises(ValueError, match="does not exist"):
        plugin.modify_context([uri], {"pixi": field}, _Ctx(), logging.getLogger("t"))
