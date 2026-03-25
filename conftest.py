"""
conftest.py – makes pytest-girder fixtures available and adds the
girder_async_routes directory to sys.path so that async_file_routes can be
imported directly.
"""

import sys
import os

# Make async_file_routes importable
sys.path.insert(0, os.path.dirname(__file__))

# Register pytest-girder as a plugin so its fixtures (db, admin, user,
# fsAssetstore, server, …) are available without any extra marker.
pytest_plugins = ["pytest_girder"]
