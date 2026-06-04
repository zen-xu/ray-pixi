import pytest

from ray_pixi import spec


def test_pixi_inline_spec_passthrough():
    out = spec.pixi(channels=["conda-forge"], dependencies={"python": "3.13.*"})
    assert out["channels"] == ["conda-forge"]
    assert out["dependencies"] == {"python": "3.13.*"}
    assert "manifest" not in out
    assert "include" not in out


def test_pixi_project_assembles_without_reading_files():
    out = spec.pixi(manifest="pixi.toml", include=["pyproject.toml", "pkg/**/*.py"])
    assert out["manifest"] == "pixi.toml"
    assert out["include"] == ["pyproject.toml", "pkg/**/*.py"]
    assert "channels" not in out


def test_pixi_project_defaults_when_no_args():
    out = spec.pixi()
    assert out["manifest"] is None
    assert out["include"] == []
    assert not any(
        k in out for k in ("channels", "dependencies", "pypi_dependencies", "platforms")
    )


def test_pixi_common_keys():
    out = spec.pixi(manifest="pixi.toml", environment="gpu", locked=True)
    assert out["environment"] == "gpu"
    assert out["locked"] is True


def test_pixi_rejects_manifest_and_inline():
    with pytest.raises(ValueError):
        spec.pixi(manifest="pixi.toml", channels=["conda-forge"])  # ty: ignore[no-matching-overload]
