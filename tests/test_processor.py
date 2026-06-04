import asyncio
import logging
import os

import pytest

from ray_pixi import manifest, processor, spec


def _populate_env(target, *, python_minor, ray_version, environment="default"):
    site = os.path.join(
        target,
        ".pixi",
        "envs",
        environment,
        "lib",
        f"python{python_minor}",
        "site-packages",
    )
    os.makedirs(os.path.join(site, f"ray-{ray_version}.dist-info"))


def _proc(target, manifest_path, field, runner):
    return processor.PixiProcessor(
        target,
        manifest_path,
        spec.normalize(field),
        "/fake/pixi",
        logging.getLogger("test"),
        runner=runner,
    )


def _default_field():
    return {"manifest": "pixi.toml"}


def _default_proc(target, runner):
    manifest_path = os.path.join(target, "pixi.toml")
    return _proc(target, manifest_path, _default_field(), runner)


def test_processor_runs_pixi_install(tmp_path):
    target = str(tmp_path)
    captured = {}

    async def fake_runner(cmd, *, cwd):
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        _populate_env(
            target,
            python_minor=manifest.current_python_minor(),
            ray_version=manifest.current_ray_version(),
        )

    proc = _default_proc(target, fake_runner)
    asyncio.run(proc.run())

    assert captured["cmd"][0] == "/fake/pixi"
    assert "install" in captured["cmd"]
    assert "--manifest-path" in captured["cmd"]
    mp = captured["cmd"].index("--manifest-path")
    assert captured["cmd"][mp + 1] == os.path.join(target, "pixi.toml")
    assert captured["cmd"][captured["cmd"].index("-e") + 1] == "default"
    assert captured["cwd"] == target


def test_processor_adds_locked_and_options(tmp_path):
    target = str(tmp_path)
    captured = {}

    async def fake_runner(cmd, *, cwd):
        captured["cmd"] = cmd
        _populate_env(
            target,
            python_minor=manifest.current_python_minor(),
            ray_version=manifest.current_ray_version(),
        )

    field = {
        "manifest": "pixi.toml",
        "locked": True,
        "pixi_install_options": ["--no-progress"],
    }
    proc = _proc(target, os.path.join(target, "pixi.toml"), field, fake_runner)
    asyncio.run(proc.run())

    assert "--locked" in captured["cmd"]
    assert "--no-progress" in captured["cmd"]


def test_processor_rejects_missing_python(tmp_path):
    target = str(tmp_path)

    async def fake_runner(cmd, *, cwd):
        os.makedirs(os.path.join(target, ".pixi", "envs", "default"))

    proc = _default_proc(target, fake_runner)
    with pytest.raises(RuntimeError, match="does not provide python"):
        asyncio.run(proc.run())


def test_processor_rejects_missing_ray(tmp_path):
    target = str(tmp_path)

    async def fake_runner(cmd, *, cwd):
        os.makedirs(
            os.path.join(
                target,
                ".pixi",
                "envs",
                "default",
                "lib",
                f"python{manifest.current_python_minor()}",
            )
        )

    proc = _default_proc(target, fake_runner)
    with pytest.raises(RuntimeError, match="does not provide ray"):
        asyncio.run(proc.run())


def test_processor_rejects_python_minor_mismatch(tmp_path):
    target = str(tmp_path)

    async def fake_runner(cmd, *, cwd):
        _populate_env(
            target,
            python_minor="3.0",
            ray_version=manifest.current_ray_version(),
        )

    proc = _default_proc(target, fake_runner)
    with pytest.raises(RuntimeError, match=r"python.*does not match"):
        asyncio.run(proc.run())


def test_processor_rejects_ray_mismatch(tmp_path):
    target = str(tmp_path)

    async def fake_runner(cmd, *, cwd):
        _populate_env(
            target,
            python_minor=manifest.current_python_minor(),
            ray_version="0.0.1",
        )

    proc = _default_proc(target, fake_runner)
    with pytest.raises(RuntimeError, match=r"ray.*does not match"):
        asyncio.run(proc.run())
