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

ray.init(
    runtime_env={
        # project mode: the manifest (and its pixi.lock) travel via working_dir
        "pixi": pixi("pixi.toml", environment="default", locked=True),
        "working_dir": ".",
    }
)


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
| `manifest` | working_dir-relative path to a `pixi.toml` / `pyproject.toml` (mutually exclusive with the inline keys) |
| `include` | extra globs or directories (relative to working_dir) selecting env-defining files, e.g. local package sources for an editable install; a directory entry takes its whole subtree, dotfiles included (glob entries skip dotfiles) |
| `exclude` | globs or directories removed from the `include` selection (a directory prunes its subtree); the manifest and `pixi.lock` are always kept |
| `channels` / `dependencies` / `pypi_dependencies` / `platforms` | inline spec |
| `environment` | environment to select, defaults to `default` |
| `locked` | reproduce strictly from `pixi.lock`, defaults to `False` |
| `pixi_version` | if set, bootstrap this pixi version on the node |
| `pixi_install_options` | extra flags passed through to `pixi install` |

In project mode (`manifest`/`include`) the files are **not** read on the driver:
they travel to the nodes via `runtime_env["working_dir"]`, which is therefore
required, and a `pixi.lock` must sit next to the manifest so every node installs
the exact same environment. Installed environments are cached by the content
hash of the env-defining subset (manifest + `pixi.lock` + `include` matches), so
editing other files in the working_dir — e.g. the driver script — re-uploads the
working_dir but does **not** rebuild the pixi environment.

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

## Install logs

Each install attempt writes pixi's output to its own timestamped file in the
node's session logs dir — `/tmp/ray/session_*/logs/pixi/install-<timestamp>-<hash>.log`
— so it is browsable in the Ray dashboard's **Logs** tab (and via
`ray logs "pixi/*"`), while staying out of the top-level patterns Ray streams
to drivers/clients (`runtime_env_setup-*.log`, `worker-*`). The setup log only
records a one-line pointer to it, and a failed install raises an error
carrying the log tail. The logs survive the cleanup of a failed env dir and
are removed together with their environment.

## Caching and garbage collection

Installed environments are **not** deleted when the last job using them exits.
Ray's runtime_env agent reference-counts each environment per node; when the
count drops to zero the environment is only marked *unused* and kept on disk,
so a later submission with the same spec reuses it instantly. Unused
environments are evicted (deleted) only when the plugin's total cache size on
a node exceeds its cap.

Two knobs matter in practice — set both in the node environment **before
`ray start`** (same place as `RAY_RUNTIME_ENV_PLUGINS`):

- **`RAY_RUNTIME_ENV_PIXI_CACHE_SIZE_GB`** (Ray's per-plugin cap, default
  `10`). Pixi environments easily reach several GB each, so the default fits
  only a couple of them before they start evicting one another and every
  submission reinstalls. Size it to the disk you can spare, e.g. `100`. Note
  that Ray picks the eviction victim arbitrarily among unused environments
  (not LRU), so a generous cap is the only reliable way to keep hot
  environments around.
- **`PIXI_CACHE_DIR`** (pixi's global package cache, defaults to
  `~/.cache/rattler`). Point it at a persistent volume in containerized
  deployments. Even when an environment is evicted, reinstalling from a warm
  package cache (plus `locked: true`, which skips the solve) is mostly a
  re-link — seconds instead of the minutes a cold first install takes.

In project mode, eviction usually costs nothing anyway: environments are
stored content-addressed by the env-defining subset, so URIs that differ only
in unrelated working_dir files share one store entry, and the entry is removed
only when its last referencing URI is evicted.

If a specific environment must never be evicted, a long-lived detached actor
declared with that runtime_env keeps its reference count above zero.

## Known interactions

- **Launching the driver with `uv run`:** Ray's built-in uv integration
  (`RAY_ENABLE_UV_RUN_RUNTIME_ENV`, on by default) detects the `uv run` ancestor and
  tries to rewrite `py_executable` to replicate the uv environment, which conflicts
  with the pixi plugin. Disable it when using pixi:
  `RAY_ENABLE_UV_RUN_RUNTIME_ENV=0`.
- **`platforms`:** when you declare an inline spec without `platforms`, ray-pixi
  defaults it to the building node's platform (e.g. `linux-64`).
- **Ray Client (`ray://`) and long installs:** grpcio's Linux wheels enable
  gRPC's fork handlers by default, and Ray Client's proxy server forks a
  per-client subprocess right after the runtime_env is created. A long first
  install keeps the proxy's gRPC threads busy, which can make that fork hit a
  known gRPC crash (`ev_epoll1_linux.cc ... Check failed: next_worker->state
  == KICKED`) — the client then fails to connect even though the environment
  was built (reconnecting usually works, since the env is now cached). Set
  `GRPC_ENABLE_FORK_SUPPORT=false` in the head node's environment, or prefer
  the Ray Jobs API over Ray Client.
