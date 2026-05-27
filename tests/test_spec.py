import pytest

from ray_pixi import spec


def test_str_field_becomes_manifest_path():
    out = spec.normalize("pixi.toml")
    assert out.source == "manifest"
    assert out.manifest_path == "pixi.toml"
    assert out.environment == "default"
    assert out.locked is False
    assert out.pixi_version is None
    assert out.pixi_install_options == []


def test_inline_spec_source():
    out = spec.normalize(
        {"channels": ["conda-forge"], "dependencies": {"python": "3.13.*"}}
    )
    assert out.source == "inline"
    assert out.channels == ["conda-forge"]
    assert out.dependencies == {"python": "3.13.*"}
    assert out.pypi_dependencies == {}
    assert out.platforms == []


def test_manifest_content_inlined():
    out = spec.normalize(
        {"manifest_content": "[project]\n", "manifest_format": "pixi.toml"}
    )
    assert out.source == "manifest"
    assert out.manifest_content == "[project]\n"
    assert out.manifest_format == "pixi.toml"


def test_common_keys_passthrough():
    out = spec.normalize(
        {
            "manifest": "p.toml",
            "environment": "gpu",
            "locked": True,
            "pixi_version": "0.40.0",
            "pixi_install_options": ["--no-progress"],
        }
    )
    assert out.environment == "gpu"
    assert out.locked is True
    assert out.pixi_version == "0.40.0"
    assert out.pixi_install_options == ["--no-progress"]


def test_validate_rejects_both_sources():
    with pytest.raises(ValueError, match="cannot specify both"):
        spec.validate({"manifest": "p.toml", "channels": ["conda-forge"]})


def test_validate_rejects_neither_source():
    with pytest.raises(ValueError, match="must specify"):
        spec.validate({"environment": "default"})


def test_validate_rejects_wrong_type():
    with pytest.raises(TypeError):
        spec.validate(123)


def test_compute_uri_prefix_and_determinism():
    a = spec.compute_uri({"manifest_content": "x", "manifest_format": "pixi.toml"})
    b = spec.compute_uri({"manifest_content": "x", "manifest_format": "pixi.toml"})
    assert a.startswith("pixi://")
    assert a == b


def test_compute_uri_changes_with_content():
    a = spec.compute_uri({"manifest_content": "x"})
    b = spec.compute_uri({"manifest_content": "y"})
    assert a != b


def test_compute_uri_changes_with_environment():
    a = spec.compute_uri({"manifest": "p.toml", "environment": "default"})
    b = spec.compute_uri({"manifest": "p.toml", "environment": "gpu"})
    assert a != b
