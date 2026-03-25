"""Aggregator – assembles all async download routes from their submodules.

Registered in asgi.py **before** the WSGIMiddleware Mount so these routes
take precedence over the WSGI catch-all.
"""

from .file import file_routes
from .folder import folder_routes
from .item import item_routes
from .resource import resource_routes

async_file_routes = [
    *file_routes,
    *item_routes,
    *folder_routes,
    *resource_routes,
]
