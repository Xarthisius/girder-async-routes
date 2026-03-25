"""
Shared fixtures and helpers for async-route integration tests.

Real MongoDB + filesystem assetstore are used (no mocking).
pytest-girder provides the ``db``, ``admin``, ``user``, ``fsAssetstore``,
and ``server`` fixtures.

A minimal Starlette application wrapping only the async routes is built per
test session so that Starlette's TestClient can exercise the handlers without
needing to start a full Uvicorn/Gunicorn process.

Token-based auth is used: a Girder token is created for the requesting user
and passed via the ``Girder-Token`` header (or the ``token`` query-parameter
for the ``?token=`` variant tests).
"""

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from girder_async_routes.routes import async_file_routes

from girder.constants import TokenScope
from girder.models.folder import Folder
from girder.models.token import Token

pytest_plugins = ["pytest_girder"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_private_folder(admin):
    """Return admin's Private folder (ACL-restricted; requires authentication)."""
    folders = list(Folder().childFolders(admin, parentType="user", user=admin))
    return next(f for f in folders if f["name"] == "Private")


def auth_headers(token):
    return {"Girder-Token": str(token["_id"])}


def upload_file(server, name, content, user, folder):
    """Upload bytes to the filesystem assetstore via the CherryPy server fixture."""
    from pytest_girder.utils import uploadFile

    return uploadFile(name, content, user, folder)


# ---------------------------------------------------------------------------
# Session-scoped app / client
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def asgi_app():
    """A minimal Starlette app containing only the async download routes."""
    return Starlette(routes=async_file_routes)


@pytest.fixture(scope="module")
def http(asgi_app):
    """A Starlette TestClient for the async-routes-only app."""
    with TestClient(asgi_app, raise_server_exceptions=True) as client:
        yield client


# ---------------------------------------------------------------------------
# Token fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def token(admin):
    """Create and return a Girder token for the admin user."""
    return Token().createToken(admin, scope=[TokenScope.DATA_READ])


@pytest.fixture
def user_token(user):
    """Create and return a Girder token for the regular user."""
    return Token().createToken(user, scope=[TokenScope.DATA_READ])


# ---------------------------------------------------------------------------
# Folder fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def public_folder(admin, db):
    """Return the first public folder in admin's Personal Space."""
    folders = list(Folder().childFolders(admin, parentType="user", user=admin))
    return folders[0]
