import os

import pytest

from ray_pixi import project, spec


def _wd(tmp_path):
    (tmp_path / "pixi.toml").write_text("[workspace]\n")
    (tmp_path / "pixi.lock").write_text("version: 6\n")
    (tmp_path / "pyproject.toml").write_text("[project]\nname='pkg'\n")
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("x = 1\n")
    (tmp_path / "driver.py").write_text("print('driver')\n")
    return str(tmp_path)


def test_resolve_working_dir_reads_env(monkeypatch):
    monkeypatch.setenv("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", "/wd")
    assert project.resolve_working_dir() == "/wd"


def test_resolve_working_dir_none_when_absent(monkeypatch):
    monkeypatch.delenv("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", raising=False)
    assert project.resolve_working_dir() is None


def test_main_manifest_path_explicit(tmp_path):
    wd = _wd(tmp_path)
    s = spec.normalize({"manifest": "pixi.toml"})
    assert project.main_manifest_path(s, wd) == os.path.join(wd, "pixi.toml")


def test_main_manifest_path_autodiscovers_pixi_toml(tmp_path):
    wd = _wd(tmp_path)
    s = spec.normalize({})
    assert project.main_manifest_path(s, wd) == os.path.join(wd, "pixi.toml")


def test_main_manifest_path_missing_raises(tmp_path):
    s = spec.normalize({})
    with pytest.raises(ValueError, match=r"no pixi\.toml"):
        project.main_manifest_path(s, str(tmp_path))


def test_collect_files_includes_manifest_lock_and_globs(tmp_path):
    wd = _wd(tmp_path)
    s = spec.normalize({"manifest": "pixi.toml", "include": ["pkg/**/*.py"]})
    files = project.collect_files(s, wd)
    assert files["pixi.toml"] == "[workspace]\n"
    assert files["pixi.lock"] == "version: 6\n"
    assert files[os.path.join("pkg", "__init__.py")] == "x = 1\n"
    # driver.py and pyproject.toml are NOT in include -> excluded
    assert "driver.py" not in files
    assert "pyproject.toml" not in files


def test_collect_files_rejects_non_text(tmp_path):
    wd = _wd(tmp_path)
    (tmp_path / "blob.bin").write_bytes(b"\xff\xfe\x00\x01")
    s = spec.normalize({"manifest": "pixi.toml", "include": ["blob.bin"]})
    with pytest.raises(ValueError, match="non-text"):
        project.collect_files(s, wd)


def test_compute_project_uri_stable_and_subset_sensitive(tmp_path):
    wd = _wd(tmp_path)
    s = spec.normalize({"manifest": "pixi.toml", "include": ["pkg/**/*.py"]})
    a = project.compute_project_uri(s, wd)
    assert a.startswith("pixi://")
    # changing a file OUTSIDE include leaves the hash unchanged
    (tmp_path / "driver.py").write_text("print('changed')\n")
    assert project.compute_project_uri(s, wd) == a
    # changing a file INSIDE include changes the hash
    (tmp_path / "pkg" / "__init__.py").write_text("x = 2\n")
    assert project.compute_project_uri(s, wd) != a


def test_materialize_project_writes_subset_and_returns_manifest(tmp_path):
    wd = _wd(tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    s = spec.normalize({"manifest": "pixi.toml", "include": ["pkg/**/*.py"]})
    manifest_path = project.materialize_project(s, wd, str(target))
    assert manifest_path == os.path.join(str(target), "pixi.toml")
    assert (target / "pixi.toml").read_text() == "[workspace]\n"
    assert (target / "pixi.lock").read_text() == "version: 6\n"
    assert (target / "pkg" / "__init__.py").read_text() == "x = 1\n"
    assert not (target / "driver.py").exists()


def test_materialize_project_requires_pixi_lock(tmp_path):
    # Project mode must ship a pixi.lock so workers install the exact same
    # environment; without it pixi would re-solve on every worker.
    wd = _wd(tmp_path)
    os.remove(os.path.join(wd, "pixi.lock"))
    target = tmp_path / "target"
    target.mkdir()
    s = spec.normalize({"manifest": "pixi.toml"})
    with pytest.raises(ValueError, match=r"pixi\.lock"):
        project.materialize_project(s, wd, str(target))


def test_main_manifest_path_explicit_missing_raises(tmp_path):
    s = spec.normalize({"manifest": "nope.toml"})
    with pytest.raises(ValueError, match="not found"):
        project.main_manifest_path(s, str(tmp_path))


def test_collect_files_rejects_parent_traversal(tmp_path):
    wd = tmp_path / "wd"
    wd.mkdir()
    (wd / "pixi.toml").write_text("[workspace]\n")
    (tmp_path / "outside.py").write_text("evil\n")
    s = spec.normalize({"manifest": "pixi.toml", "include": ["../outside.py"]})
    with pytest.raises(ValueError, match="outside"):
        project.collect_files(s, str(wd))
