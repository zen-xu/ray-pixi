"""ray-pixi: a pixi runtime_env plugin for Ray.

Register the plugin by setting, on every node before it starts::

    RAY_RUNTIME_ENV_PLUGINS = '[{"class": "ray_pixi.PixiPlugin"}]'

The Ray runtime_env agent reads that variable at startup. The ``runtime_env["pixi"]``
field is a plain dict (no local ray_pixi needed on the driver):

* inline mode -- ``{"channels": [...], "dependencies": {...}}``
* project mode -- ``{"manifest": "pixi.toml", "include": ["pyproject.toml", "pkg/**"]}``
  together with ``runtime_env["working_dir"]``.

:func:`pixi` is an optional convenience for assembling that dict.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from ray_pixi.plugin import PixiPlugin
from ray_pixi.spec import pixi

try:
    __version__ = version("ray-pixi")
except PackageNotFoundError:  # package is not installed
    __version__ = "0.0.0+unknown"

__all__ = ["PixiPlugin", "__version__", "pixi"]
