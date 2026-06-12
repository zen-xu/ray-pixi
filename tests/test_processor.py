import asyncio
import logging
import os
import sys

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


def test_default_runner_streams_output_to_log_file(tmp_path):
    # pixi's verbose output must go to a dedicated file, NOT to a ray logger:
    # runtime_env_setup-*.log can be streamed to drivers/clients.
    log_path = str(tmp_path / "x.install.log")
    asyncio.run(
        processor._stream_to_log_runner(
            ["sh", "-c", "echo out; echo err 1>&2"],
            cwd=str(tmp_path),
            log_path=log_path,
        )
    )
    with open(log_path) as f:
        content = f.read()
    assert "out" in content
    assert "err" in content  # stderr is merged into the same file


def test_default_runner_failure_raises_with_tail_and_path(tmp_path):
    log_path = str(tmp_path / "x.install.log")
    with pytest.raises(RuntimeError, match="boom") as excinfo:
        asyncio.run(
            processor._stream_to_log_runner(
                ["sh", "-c", "echo boom; exit 3"],
                cwd=str(tmp_path),
                log_path=log_path,
            )
        )
    assert "exit code 3" in str(excinfo.value)
    assert log_path in str(excinfo.value)


def test_default_runner_kills_subprocess_on_cancel(tmp_path):
    # delete_uri cancels in-flight creates; the install subprocess must die
    # with the task, not keep writing into the store dir as an orphan.
    pid_file = tmp_path / "pid"
    log_path = str(tmp_path / "x.install.log")
    cmd = [
        sys.executable,
        "-c",
        "import os, time; open('pid', 'w').write(str(os.getpid())); time.sleep(30)",
    ]

    async def main():
        task = asyncio.create_task(
            processor._stream_to_log_runner(cmd, cwd=str(tmp_path), log_path=log_path)
        )
        while not (pid_file.exists() and pid_file.read_text()):
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_file.read_text()), 0)


def test_default_runner_disables_color(tmp_path):
    log_path = str(tmp_path / "x.install.log")
    asyncio.run(
        processor._stream_to_log_runner(
            ["sh", "-c", 'printf "NO_COLOR=%s" "$NO_COLOR"'],
            cwd=str(tmp_path),
            log_path=log_path,
        )
    )
    with open(log_path) as f:
        assert "NO_COLOR=1" in f.read()


def test_processor_run_writes_sibling_install_log(tmp_path):
    # Default log location is a sibling of the env dir, so a failed install's
    # cleanup (rmtree of the env dir) keeps the log for inspection.
    target = str(tmp_path / "envdir")
    os.makedirs(target)
    _populate_env(
        target,
        python_minor=manifest.current_python_minor(),
        ray_version=manifest.current_ray_version(),
    )
    manifest_path = os.path.join(target, "pixi.toml")
    proc = processor.PixiProcessor(
        target,
        manifest_path,
        spec.normalize(_default_field()),
        "/bin/echo",  # stands in for pixi: prints the args it gets
        logging.getLogger("test"),
    )
    asyncio.run(proc.run())

    log_path = f"{target}.install.log"
    assert os.path.exists(log_path)
    with open(log_path) as f:
        content = f.read()
    assert "--manifest-path" in content


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


def test_lock_mismatch_failure_carries_fix_hint(tmp_path):
    # `pixi install --locked` fails in seconds (pre-download) when pixi.lock is
    # out of sync with the manifest; surface how to fix it instead of leaving
    # users to decode pixi's error from a node log.
    async def fake_runner(cmd, *, cwd):
        raise RuntimeError(
            "`/fake/pixi install` failed with exit code 1. Last output:\n"
            "ERROR: lock-file not up-to-date with the project\n"
        )

    proc = _default_proc(str(tmp_path), fake_runner)
    with pytest.raises(RuntimeError, match=r"pixi lock"):
        asyncio.run(proc.run())


def test_unrelated_failure_keeps_original_error(tmp_path):
    async def fake_runner(cmd, *, cwd):
        raise RuntimeError("`/fake/pixi install` failed with exit code 1. boom")

    proc = _default_proc(str(tmp_path), fake_runner)
    with pytest.raises(RuntimeError, match="boom") as excinfo:
        asyncio.run(proc.run())
    assert "pixi lock" not in str(excinfo.value)
