import os
import shutil

import pytest

# Opt-in: this test starts a real Ray cluster and builds a real pixi environment
# (network + minutes). It is skipped unless RAY_PIXI_RUN_INTEGRATION=1 and a pixi
# executable is available.
pytestmark = pytest.mark.skipif(
    os.environ.get("RAY_PIXI_RUN_INTEGRATION") != "1" or shutil.which("pixi") is None,
    reason="set RAY_PIXI_RUN_INTEGRATION=1 with pixi installed to run",
)


def test_pixi_runtime_env_end_to_end():
    import ray

    from ray_pixi import pixi

    # Register the plugin before ray.init() starts the local runtime_env agent.
    os.environ["RAY_RUNTIME_ENV_PLUGINS"] = '[{"class": "ray_pixi.PixiPlugin"}]'

    # Inline spec: python and ray are auto-filled to this process's versions.
    ray.init(runtime_env={"pixi": pixi(channels=["conda-forge"])})
    try:

        @ray.remote
        def where():
            import sys

            return sys.executable

        exe = ray.get(where.remote())
        assert os.path.join(".pixi", "envs", "default") in exe
    finally:
        ray.shutdown()


def test_pixi_project_mode_end_to_end(tmp_path):
    import ray

    from ray_pixi import pixi

    os.environ["RAY_RUNTIME_ENV_PLUGINS"] = '[{"class": "ray_pixi.PixiPlugin"}]'

    from ray_pixi import manifest as _manifest

    py = _manifest.current_python_version()
    ray_ver = _manifest.current_ray_version()
    (tmp_path / "pixi.toml").write_text(
        "[workspace]\n"
        'channels = ["conda-forge"]\n'
        'platforms = ["linux-64"]\n'
        "\n"
        "[dependencies]\n"
        f'python = "=={py}"\n'
        "\n"
        "[pypi-dependencies]\n"
        f'ray = {{ version = "=={ray_ver}", extras = ["default"] }}\n'
    )

    ray.init(
        runtime_env={
            "pixi": pixi(manifest="pixi.toml"),
            "working_dir": str(tmp_path),
        }
    )
    try:

        @ray.remote
        def where():
            import sys

            return sys.executable

        exe = ray.get(where.remote())
        assert os.path.join(".pixi", "envs", "default") in exe
    finally:
        ray.shutdown()
