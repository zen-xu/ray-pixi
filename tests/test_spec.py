import pytest

from ray_pixi import spec


def test_str_field_becomes_manifest_path():
    out = spec.normalize("pixi.toml")
    assert out.source == "project"
    assert out.manifest == "pixi.toml"
    assert out.include == []
    assert out.environment == "default"
    assert out.locked is True  # project mode locks by default


def test_inline_spec_source():
    out = spec.normalize(
        {"channels": ["conda-forge"], "dependencies": {"python": "3.13.*"}}
    )
    assert out.source == "inline"
    assert out.channels == ["conda-forge"]
    assert out.dependencies == {"python": "3.13.*"}


def test_project_with_include():
    out = spec.normalize(
        {"manifest": "pixi.toml", "include": ["pyproject.toml", "pkg/**/*.py"]}
    )
    assert out.source == "project"
    assert out.manifest == "pixi.toml"
    assert out.include == ["pyproject.toml", "pkg/**/*.py"]


def test_empty_dict_is_project_auto_discover():
    out = spec.normalize({})
    assert out.source == "project"
    assert out.manifest is None
    assert out.include == []


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


def test_validate_rejects_inline_with_project():
    with pytest.raises(ValueError, match="cannot specify both"):
        spec.validate({"manifest": "p.toml", "channels": ["conda-forge"]})


def test_validate_rejects_inline_with_include():
    with pytest.raises(ValueError, match="cannot specify both"):
        spec.validate({"include": ["pkg/**"], "dependencies": {"python": "*"}})


def test_validate_rejects_wrong_type():
    with pytest.raises(TypeError):
        spec.validate(123)


def test_compute_uri_prefix_and_determinism():
    a = spec.compute_uri({"channels": ["conda-forge"]})
    b = spec.compute_uri({"channels": ["conda-forge"]})
    assert a.startswith("pixi://")
    assert a == b


def test_compute_uri_changes_with_content():
    a = spec.compute_uri({"dependencies": {"numpy": "1"}})
    b = spec.compute_uri({"dependencies": {"numpy": "2"}})
    assert a != b


def test_compute_uri_insensitive_to_dict_key_order():
    # The same spec written in a different key order must hit the same cache.
    a = spec.compute_uri({"dependencies": {"numpy": "1", "pandas": "2"}})
    b = spec.compute_uri({"dependencies": {"pandas": "2", "numpy": "1"}})
    assert a == b


def test_exclude_is_project_key():
    s = spec.normalize({"manifest": "pixi.toml", "exclude": ["pkg/tests"]})
    assert s.exclude == ["pkg/tests"]
    assert spec.normalize({"manifest": "pixi.toml"}).exclude == []


def test_validate_rejects_inline_with_exclude():
    with pytest.raises(ValueError, match=r"exclusive|cannot"):
        spec.normalize({"dependencies": {"python": "*"}, "exclude": ["x"]})


def test_pixi_helper_accepts_exclude():
    field = spec.pixi("pixi.toml", exclude=["pkg/tests"])
    assert field["exclude"] == ["pkg/tests"]
    with pytest.raises(ValueError, match="cannot"):
        spec.pixi(dependencies={"python": "*"}, exclude=["x"])  # ty: ignore[no-matching-overload]


def test_locked_defaults_true_in_project_mode():
    # Without --locked, an out-of-date pixi.lock makes each node re-solve at
    # install time, so nodes could build DIFFERENT environments under the SAME
    # store hash. Project mode therefore locks by default.
    assert spec.normalize({"manifest": "pixi.toml"}).locked is True
    assert spec.normalize({}).locked is True  # auto-discovered project mode
    assert spec.normalize({"manifest": "pixi.toml", "locked": False}).locked is False


def test_locked_defaults_false_in_inline_mode():
    # Inline mode synthesizes a manifest with no lockfile; nothing to lock to.
    assert spec.normalize({"dependencies": {"python": "*"}}).locked is False
    assert (
        spec.normalize({"dependencies": {"python": "*"}, "locked": True}).locked is True
    )


def test_pixi_helper_locked_follows_mode_default():
    assert spec.normalize(spec.pixi("pixi.toml")).locked is True
    assert spec.normalize(spec.pixi("pixi.toml", locked=False)).locked is False
    assert spec.normalize(spec.pixi(dependencies={"python": "*"})).locked is False
