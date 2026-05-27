import asyncio
import logging
import os

from ray_pixi import manifest, processor


def _populate_env(target, *, python_minor, ray_version, environment="default"):
    """Create a fake installed pixi env with the given python and ray versions."""
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


def test_processor_runs_pixi_install(tmp_path, monkeypatch):
    target = str(tmp_path / "t")
    captured = {}

    async def fake_runner(cmd, *, cwd):
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        _populate_env(
            target,
            python_minor=manifest.current_python_minor(),
            ray_version=manifest.current_ray_version(),
        )

    monkeypatch.setattr(processor.binary, "resolve_pixi", lambda td, v: "/fake/pixi")

    runtime_env = {
        "pixi": {"manifest_content": "[workspace]\n", "environment": "default"}
    }
    proc = processor.PixiProcessor(
        target, runtime_env, logging.getLogger("test"), runner=fake_runner
    )
    asyncio.run(proc.run())

    assert captured["cmd"][0] == "/fake/pixi"
    assert "install" in captured["cmd"]
    assert "--manifest-path" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("-e") + 1] == "default"
    assert os.path.exists(os.path.join(target, "pixi.toml"))


def test_processor_adds_locked_and_options(tmp_path, monkeypatch):
    target = str(tmp_path / "t")
    captured = {}

    async def fake_runner(cmd, *, cwd):
        captured["cmd"] = cmd
        _populate_env(
            target,
            python_minor=manifest.current_python_minor(),
            ray_version=manifest.current_ray_version(),
        )

    monkeypatch.setattr(processor.binary, "resolve_pixi", lambda td, v: "/fake/pixi")
    runtime_env = {
        "pixi": {
            "manifest_content": "[workspace]\n",
            "locked": True,
            "pixi_install_options": ["--no-progress"],
        }
    }
    proc = processor.PixiProcessor(
        target, runtime_env, logging.getLogger("test"), runner=fake_runner
    )
    asyncio.run(proc.run())

    assert "--locked" in captured["cmd"]
    assert "--no-progress" in captured["cmd"]


def test_processor_rejects_missing_python_in_manifest(tmp_path, monkeypatch):
    target = str(tmp_path / "t")

    async def fake_runner(cmd, *, cwd):
        os.makedirs(os.path.join(target, ".pixi", "envs", "default"))

    monkeypatch.setattr(processor.binary, "resolve_pixi", lambda td, v: "/fake/pixi")
    runtime_env = {"pixi": {"manifest_content": "[workspace]\n"}}
    proc = processor.PixiProcessor(
        target, runtime_env, logging.getLogger("test"), runner=fake_runner
    )
    try:
        asyncio.run(proc.run())
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "does not provide python" in str(e)
    assert not os.path.exists(target)


def test_processor_rejects_missing_ray_in_manifest(tmp_path, monkeypatch):
    target = str(tmp_path / "t")

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

    monkeypatch.setattr(processor.binary, "resolve_pixi", lambda td, v: "/fake/pixi")
    runtime_env = {"pixi": {"manifest_content": "[workspace]\n"}}
    proc = processor.PixiProcessor(
        target, runtime_env, logging.getLogger("test"), runner=fake_runner
    )
    try:
        asyncio.run(proc.run())
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "does not provide ray" in str(e)
    assert not os.path.exists(target)


def test_processor_rejects_python_minor_mismatch(tmp_path, monkeypatch):
    target = str(tmp_path / "t")

    async def fake_runner(cmd, *, cwd):
        # python 3.0 cannot match the cluster; ray is present and matching.
        _populate_env(
            target, python_minor="3.0", ray_version=manifest.current_ray_version()
        )

    monkeypatch.setattr(processor.binary, "resolve_pixi", lambda td, v: "/fake/pixi")
    runtime_env = {"pixi": {"manifest_content": "[workspace]\n"}}
    proc = processor.PixiProcessor(
        target, runtime_env, logging.getLogger("test"), runner=fake_runner
    )
    try:
        asyncio.run(proc.run())
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "python" in str(e) and "does not match" in str(e)
    assert not os.path.exists(target)


def test_processor_rejects_ray_mismatch(tmp_path, monkeypatch):
    target = str(tmp_path / "t")

    async def fake_runner(cmd, *, cwd):
        # python matches the cluster minor, but ray does not match.
        _populate_env(
            target, python_minor=manifest.current_python_minor(), ray_version="0.0.1"
        )

    monkeypatch.setattr(processor.binary, "resolve_pixi", lambda td, v: "/fake/pixi")
    runtime_env = {"pixi": {"manifest_content": "[workspace]\n"}}
    proc = processor.PixiProcessor(
        target, runtime_env, logging.getLogger("test"), runner=fake_runner
    )
    try:
        asyncio.run(proc.run())
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "ray" in str(e) and "does not match" in str(e)
    assert not os.path.exists(target)


def test_processor_cleans_up_on_failure(tmp_path, monkeypatch):
    target = str(tmp_path / "t")

    async def boom_runner(cmd, *, cwd):
        raise RuntimeError("pixi failed")

    monkeypatch.setattr(processor.binary, "resolve_pixi", lambda td, v: "/fake/pixi")
    runtime_env = {"pixi": {"manifest_content": "[workspace]\n"}}
    proc = processor.PixiProcessor(
        target, runtime_env, logging.getLogger("test"), runner=boom_runner
    )
    try:
        asyncio.run(proc.run())
        raise AssertionError("expected RuntimeError")
    except RuntimeError:
        pass
    assert not os.path.exists(target)
