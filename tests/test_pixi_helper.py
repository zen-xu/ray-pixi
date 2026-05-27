import pytest

from ray_pixi import spec


def test_pixi_inlines_manifest_and_lock(tmp_path):
    manifest = tmp_path / "pixi.toml"
    manifest.write_text("[project]\nname='x'\n")
    (tmp_path / "pixi.lock").write_text("version: 6\n")

    out = spec.pixi(str(manifest), environment="gpu", locked=True)

    assert out["manifest_content"] == "[project]\nname='x'\n"
    assert out["lock_content"] == "version: 6\n"
    assert out["manifest_format"] == "pixi.toml"
    assert out["environment"] == "gpu"
    assert out["locked"] is True


def test_pixi_pyproject_format(tmp_path):
    manifest = tmp_path / "pyproject.toml"
    manifest.write_text("[tool.pixi]\n")
    out = spec.pixi(str(manifest))
    assert out["manifest_format"] == "pyproject.toml"
    assert out["lock_content"] is None


def test_pixi_inline_spec_passthrough():
    out = spec.pixi(channels=["conda-forge"], dependencies={"python": "3.13.*"})
    assert out["channels"] == ["conda-forge"]
    assert out["dependencies"] == {"python": "3.13.*"}
    assert "manifest_content" not in out


def test_pixi_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        spec.pixi(str(tmp_path / "nope.toml"))


def test_pixi_rejects_manifest_and_inline():
    with pytest.raises(ValueError):
        # Intentional misuse: overloads reject this statically; verify runtime guard.
        spec.pixi("pixi.toml", channels=["conda-forge"])  # ty: ignore[no-matching-overload]
