# ray-pixi

A [pixi](https://pixi.sh) `runtime_env` for [Ray](https://ray.io): declare a pixi
environment in your `runtime_env`, and Ray installs it on each node and launches
the workers inside it via `pixi run`.

## Install

```bash
pip install ray-pixi      # or: pixi add --pypi ray-pixi
```

## Plugin registration

Ray discovers runtime_env plugins through the `RAY_RUNTIME_ENV_PLUGINS`
environment variable, which **must be set before the runtime_env agent starts** —
that is, before `ray start` on every node (and before `ray.init()` for a local
cluster):

```bash
export RAY_RUNTIME_ENV_PLUGINS='[{"class":"ray_pixi.PixiPlugin"}]'
ray start --head          # or: ray start --address=...
```

On KubeRay / a cluster YAML, set the same value in each node's container env.
ray-pixi must also be installed in the agent's Python environment on every node
(so it can import `ray_pixi.PixiPlugin`).

## Usage

Once the variable is set, declare the environment with `pixi()`:

```python
import ray

from ray_pixi import pixi

ray.init(runtime_env={"pixi": pixi("pixi.toml", environment="default", locked=True)})


@ray.remote
def task():
    import numpy

    return numpy.__version__


print(ray.get(task.remote()))
```

You can also declare the environment inline, without a manifest file:

```python
ray.init(
    runtime_env={
        "pixi": pixi(
            channels=["conda-forge"],
            dependencies={"python": "3.13.*", "numpy": "*"},
        )
    }
)
```

## The `pixi` field

`runtime_env["pixi"]` accepts a `str` (a manifest path) or a `dict`:

| key | description |
| --- | --- |
| `manifest` | path to a `pixi.toml` / `pyproject.toml` (mutually exclusive with the inline keys) |
| `channels` / `dependencies` / `pypi_dependencies` / `platforms` | inline spec |
| `environment` | environment to select, defaults to `default` |
| `locked` | reproduce strictly from `pixi.lock`, defaults to `False` |
| `pixi_version` | if set, bootstrap this pixi version on the node |
| `pixi_install_options` | extra flags passed through to `pixi install` |

Prefer building the field with `ray_pixi.pixi()`: given a manifest path it reads
the file (and a sibling `pixi.lock`) on the driver and inlines the content, so the
configuration is self-contained across nodes.

> **Requirements:** every node needs a `pixi` executable (on `PATH`, or bootstrapped
> via `pixi_version`), and the pixi environment must provide a `python` and `ray`
> that match the cluster (see *Version matching* below).

## Version matching

Ray refuses to connect a worker whose `ray` or `python` version differs from the
cluster. Concretely:

- **Ray version** is always compared exactly — declare the cluster's exact version.
- **Python version** is compared at the level set by the cluster's
  `RAY_DEFAULT_PYTHON_VERSION_MATCH_LEVEL` environment variable:
  - `patch` (Ray's default) — must match down to the micro version
    (`3.13.12` ≠ `3.13.13`), or Ray errors.
  - `minor` — only `major.minor` must match; a micro difference just warns. Set
    `RAY_DEFAULT_PYTHON_VERSION_MATCH_LEVEL=minor` on the cluster to allow this.

How ray-pixi helps:

- **Inline mode:** if you omit `python` from `dependencies` it is pinned to the
  node's exact running version, and if you omit `ray` from `pypi_dependencies` it is
  added as `ray[default]==<cluster ray version>`, so workers match by default.
- **Manifest mode:** ray-pixi does **not** edit your manifest, so it must declare
  both `python` and `ray` itself (ray as a pypi dependency). A manifest that installs
  neither is rejected after install with a clear error.
- **Both modes:** after installing, ray-pixi verifies the env's python *minor* and
  *ray* version against the cluster and fails fast (with a clear error) on a
  mismatch. The python check is minor-level only, so under the default `patch` level
  still pin `python` to the cluster's exact version (e.g. `python = "==3.13.12"` in
  your manifest), or relax the cluster to `minor`.

## Known interactions

- **Launching the driver with `uv run`:** Ray's built-in uv integration
  (`RAY_ENABLE_UV_RUN_RUNTIME_ENV`, on by default) detects the `uv run` ancestor and
  tries to rewrite `py_executable` to replicate the uv environment, which conflicts
  with the pixi plugin. Disable it when using pixi:
  `RAY_ENABLE_UV_RUN_RUNTIME_ENV=0`.
- **`platforms`:** when you declare an inline spec without `platforms`, ray-pixi
  defaults it to the building node's platform (e.g. `linux-64`).
