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
    assert files["pixi.toml"] == b"[workspace]\n"
    assert files["pixi.lock"] == b"version: 6\n"
    assert files[os.path.join("pkg", "__init__.py")] == b"x = 1\n"
    # driver.py and pyproject.toml are NOT in include -> excluded
    assert "driver.py" not in files
    assert "pyproject.toml" not in files


def test_collect_files_allows_binary(tmp_path):
    wd = _wd(tmp_path)
    payload = b"\xff\xfe\x00\x01"
    (tmp_path / "blob.bin").write_bytes(payload)
    s = spec.normalize({"manifest": "pixi.toml", "include": ["blob.bin"]})
    assert project.collect_files(s, wd)["blob.bin"] == payload


def test_materialize_project_preserves_bytes_exactly(tmp_path):
    # CRLF must survive the copy; text-mode IO would rewrite it to LF and the
    # materialized project would no longer match the working_dir content.
    wd = _wd(tmp_path)
    (tmp_path / "win.cfg").write_bytes(b"a = 1\r\nb = 2\r\n")
    target = tmp_path / "target"
    target.mkdir()
    s = spec.normalize({"manifest": "pixi.toml", "include": ["win.cfg"]})
    project.materialize_project(s, wd, str(target))
    assert (target / "win.cfg").read_bytes() == b"a = 1\r\nb = 2\r\n"


def test_collect_files_rejects_symlink_escape(tmp_path):
    wd = tmp_path / "wd"
    wd.mkdir()
    (wd / "pixi.toml").write_text("[workspace]\n")
    (tmp_path / "outside.py").write_text("evil\n")
    os.symlink(tmp_path / "outside.py", wd / "sneaky.py")
    s = spec.normalize({"manifest": "pixi.toml", "include": ["sneaky.py"]})
    with pytest.raises(ValueError, match="outside"):
        project.collect_files(s, str(wd))


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


def test_collect_files_include_directory_recursively(tmp_path):
    # A bare directory entry means "everything under it", dotfiles included --
    # no fold/**/*.py spelling needed.
    wd = _wd(tmp_path)
    nested = tmp_path / "pkg" / "sub"
    nested.mkdir()
    (nested / "mod.py").write_text("y = 2\n")
    (tmp_path / "pkg" / ".env.template").write_text("KEY=\n")
    s = spec.normalize({"manifest": "pixi.toml", "include": ["pkg"]})
    files = project.collect_files(s, wd)
    assert files[os.path.join("pkg", "__init__.py")] == b"x = 1\n"
    assert files[os.path.join("pkg", "sub", "mod.py")] == b"y = 2\n"
    assert files[os.path.join("pkg", ".env.template")] == b"KEY=\n"


def test_collect_files_include_directory_rejects_escape(tmp_path):
    wd_root = tmp_path / "wd"
    wd_root.mkdir()
    (wd_root / "pixi.toml").write_text("[workspace]\n")
    (wd_root / "pixi.lock").write_text("version: 6\n")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("s\n")
    s = spec.normalize({"manifest": "pixi.toml", "include": ["../outside"]})
    with pytest.raises(ValueError, match="outside"):
        project.collect_files(s, str(wd_root))


def test_collect_files_exclude_removes_matched_files(tmp_path):
    wd = _wd(tmp_path)
    (tmp_path / "pkg" / "big.bin").write_bytes(b"\x00")
    s = spec.normalize(
        {"manifest": "pixi.toml", "include": ["pkg"], "exclude": ["pkg/*.bin"]}
    )
    files = project.collect_files(s, wd)
    assert os.path.join("pkg", "__init__.py") in files
    assert os.path.join("pkg", "big.bin") not in files


def test_collect_files_exclude_directory_prunes_subtree(tmp_path):
    # Excluding a directory needs no trailing /**: the whole subtree goes.
    wd = _wd(tmp_path)
    tests_dir = tmp_path / "pkg" / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_mod.py").write_text("t = 1\n")
    s = spec.normalize(
        {"manifest": "pixi.toml", "include": ["pkg"], "exclude": ["pkg/tests"]}
    )
    files = project.collect_files(s, wd)
    assert os.path.join("pkg", "__init__.py") in files
    assert os.path.join("pkg", "tests", "test_mod.py") not in files


def test_collect_files_exclude_cannot_remove_manifest_or_lock(tmp_path):
    wd = _wd(tmp_path)
    s = spec.normalize({"manifest": "pixi.toml", "exclude": ["pixi.toml", "pixi.lock"]})
    files = project.collect_files(s, wd)
    assert "pixi.toml" in files
    assert "pixi.lock" in files


def test_uri_from_working_dir_uri_distinguishes_exclude():
    wd_uri = "gcs://_ray_pkg_0123456789abcdef.zip"
    base = spec.normalize({"manifest": "pixi.toml"})
    with_exclude = spec.normalize({"manifest": "pixi.toml", "exclude": ["pkg/tests"]})
    assert project.compute_project_uri_from_working_dir_uri(
        base, wd_uri
    ) != project.compute_project_uri_from_working_dir_uri(with_exclude, wd_uri)


def test_uri_from_working_dir_uri_distinguishes_manifest_and_include():
    wd_uri = "gcs://_ray_pkg_0123456789abcdef.zip"
    base = spec.normalize({"manifest": "a/pixi.toml"})
    other_manifest = spec.normalize({"manifest": "b/pixi.toml"})
    with_include = spec.normalize({"manifest": "a/pixi.toml", "include": ["pkg/**"]})

    base_uri = project.compute_project_uri_from_working_dir_uri(base, wd_uri)
    # same working_dir, different manifest -> a different environment
    assert (
        project.compute_project_uri_from_working_dir_uri(other_manifest, wd_uri)
        != base_uri
    )
    # same working_dir, different include -> a different environment
    assert (
        project.compute_project_uri_from_working_dir_uri(with_include, wd_uri)
        != base_uri
    )
    # identical spec stays stable
    assert project.compute_project_uri_from_working_dir_uri(base, wd_uri) == base_uri


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
