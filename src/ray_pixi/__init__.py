from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ray-pixi")
except PackageNotFoundError:  # package is not installed
    __version__ = "0.0.0+unknown"

__all__ = ["__version__", "hello"]


def hello() -> str:
    return "Hello from ray-pixi!"
