from importlib.metadata import version, PackageNotFoundError
from .routes import async_file_routes

try:
    __version__ = version("girder-async-routes")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = ["async_file_routes", "__version__"]
