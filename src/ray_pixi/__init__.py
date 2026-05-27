"""ray-pixi: a pixi runtime_env plugin for Ray.

Register the plugin by setting, on every node before it starts::

    RAY_RUNTIME_ENV_PLUGINS = '[{"class": "ray_pixi.PixiPlugin"}]'

The Ray runtime_env agent reads that variable at startup. Build the
``runtime_env["pixi"]`` field with :func:`pixi`.
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
