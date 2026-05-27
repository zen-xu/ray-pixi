import os
import tomllib

import pytest

from ray_pixi import manifest, spec


def test_synthesize_pixi_toml_from_inline():
    normalized = spec.normalize(
        {
            "channels": ["conda-forge"],
            "dependencies": {"python": "3.13.*"},
            "pypi_dependencies": {"ray": ">=2.50"},
            "platforms": ["linux-64"],
        }
    )
    text = manifest.synthesize_pixi_toml(normalized)
    parsed = tomllib.loads(text)
    assert parsed["workspace"]["channels"] == ["conda-forge"]
    assert parsed["workspace"]["platforms"] == ["linux-64"]
    assert parsed["dependencies"] == {"python": "3.13.*"}
    assert parsed["pypi-dependencies"] == {"ray": ">=2.50"}


def test_synthesize_table_valued_dependencies():
    normalized = spec.normalize(
        {
            "channels": ["conda-forge"],
            "dependencies": {"python": {"version": "3.13.*", "channel": "conda-forge"}},
            "pypi_dependencies": {"black": {"version": ">=22", "extras": ["d"]}},
        }
    )
    parsed = tomllib.loads(manifest.synthesize_pixi_toml(normalized))
    assert parsed["dependencies"]["python"] == {
        "version": "3.13.*",
        "channel": "conda-forge",
    }
    assert parsed["pypi-dependencies"]["black"] == {"version": ">=22", "extras": ["d"]}


def test_synthesize_autofills_python_when_absent():
    normalized = spec.normalize(
        {"channels": ["conda-forge"], "dependencies": {"numpy": "*"}}
    )
    parsed = tomllib.loads(manifest.synthesize_pixi_toml(normalized))
    assert parsed["dependencies"]["python"] == f"=={manifest.current_python_version()}"
    assert parsed["dependencies"]["numpy"] == "*"


def test_synthesize_keeps_explicit_python():
    normalized = spec.normalize({"dependencies": {"python": "==3.12.0"}})
    parsed = tomllib.loads(manifest.synthesize_pixi_toml(normalized))
    assert parsed["dependencies"]["python"] == "==3.12.0"


def test_synthesize_autofills_ray_when_absent():
    normalized = spec.normalize({"channels": ["conda-forge"]})
    parsed = tomllib.loads(manifest.synthesize_pixi_toml(normalized))
    assert parsed["pypi-dependencies"]["ray"] == {
        "version": f"=={manifest.current_ray_version()}",
        "extras": ["default"],
    }


def test_synthesize_keeps_explicit_ray():
    normalized = spec.normalize({"pypi_dependencies": {"ray": "==1.0.0"}})
    parsed = tomllib.loads(manifest.synthesize_pixi_toml(normalized))
    assert parsed["pypi-dependencies"]["ray"] == "==1.0.0"


def test_installed_ray_version_reads_distinfo(tmp_path):
    dist = (
        tmp_path
        / ".pixi"
        / "envs"
        / "default"
        / "lib"
        / "python3.13"
        / "site-packages"
        / "ray-2.55.1.dist-info"
    )
    dist.mkdir(parents=True)
    assert manifest.installed_ray_version(str(tmp_path), "default") == "2.55.1"


def test_installed_python_minor_reads_env(tmp_path):
    env_lib = tmp_path / ".pixi" / "envs" / "default" / "lib" / "python3.13"
    env_lib.mkdir(parents=True)
    assert manifest.installed_python_minor(str(tmp_path), "default") == "3.13"


def test_installed_python_minor_none_when_absent(tmp_path):
    assert manifest.installed_python_minor(str(tmp_path), "default") is None


def test_synthesize_defaults_channels_when_empty():
    normalized = spec.normalize({"pypi_dependencies": {"sh": "*"}})
    parsed = tomllib.loads(manifest.synthesize_pixi_toml(normalized))
    assert parsed["workspace"]["channels"] == ["conda-forge"]


def test_synthesize_defaults_platform_when_unset():
    normalized = spec.normalize({"channels": ["conda-forge"]})
    parsed = tomllib.loads(manifest.synthesize_pixi_toml(normalized))
    assert parsed["workspace"]["platforms"] == [manifest.current_pixi_platform()]
    assert manifest.current_pixi_platform()  # non-empty, e.g. "linux-64"


def test_materialize_inlined_content_writes_files(tmp_path):
    normalized = spec.normalize(
        {"manifest_content": "[project]\nname='x'\n", "lock_content": "version: 6\n"}
    )
    path = manifest.materialize(normalized, str(tmp_path))
    assert path == os.path.join(str(tmp_path), "pixi.toml")
    with open(path) as f:
        assert f.read() == "[project]\nname='x'\n"
    with open(os.path.join(str(tmp_path), "pixi.lock")) as f:
        assert f.read() == "version: 6\n"


def test_materialize_inline_spec_synthesizes(tmp_path):
    normalized = spec.normalize(
        {"channels": ["conda-forge"], "dependencies": {"python": "*"}}
    )
    path = manifest.materialize(normalized, str(tmp_path))
    assert os.path.exists(path)
    with open(path) as f:
        assert "conda-forge" in f.read()


def test_materialize_path_mode_copies_from_agent_fs(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "pixi.toml").write_text("[project]\nname='y'\n")
    (src / "pixi.lock").write_text("version: 6\n")
    normalized = spec.normalize({"manifest": str(src / "pixi.toml")})

    target = tmp_path / "target"
    target.mkdir()
    path = manifest.materialize(normalized, str(target))
    with open(path) as f:
        assert f.read() == "[project]\nname='y'\n"
    with open(os.path.join(str(target), "pixi.lock")) as f:
        assert f.read() == "version: 6\n"


def test_materialize_path_mode_missing_raises(tmp_path):
    normalized = spec.normalize({"manifest": str(tmp_path / "nope.toml")})
    with pytest.raises(ValueError, match="could not locate"):
        manifest.materialize(normalized, str(tmp_path))
