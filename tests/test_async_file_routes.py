"""
Integration tests for the ASGI async download routes.

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

import io
import zipfile

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from girder_async_routes import async_file_routes

from girder.models.folder import Folder
from girder.models.item import Item
from girder.models.token import Token
from girder.models.upload import Upload


def _get_private_folder(admin):
    """Return admin's Private folder (ACL-restricted; requires authentication)."""
    folders = list(Folder().childFolders(admin, parentType="user", user=admin))
    return next(f for f in folders if f["name"] == "Private")


# ---------------------------------------------------------------------------
# Fixtures
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


@pytest.fixture
def token(admin):
    """Create and return a Girder token for the admin user."""
    return Token().createToken(admin)


@pytest.fixture
def user_token(user):
    """Create and return a Girder token for the regular user."""
    return Token().createToken(user)


@pytest.fixture
def public_folder(admin, db):
    """Return the first public folder in admin's Personal Space."""
    folders = list(Folder().childFolders(admin, parentType="user", user=admin))
    return folders[0]


def _upload_file(server, name, content, user, folder):
    """Upload bytes to the filesystem assetstore via the CherryPy server fixture."""
    from pytest_girder.utils import uploadFile

    return uploadFile(name, content, user, folder)


def _auth_headers(token):
    return {"Girder-Token": str(token["_id"])}


# ---------------------------------------------------------------------------
# /api/v1/file/{file_id}/download  (filesystem assetstore)
# ---------------------------------------------------------------------------


class TestFileDownload:
    def test_full_download(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"Hello, async world!" * 100
        file = _upload_file(server, "hello.txt", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download", headers=_auth_headers(token)
        )

        assert resp.status_code == 200
        assert resp.content == content

    def test_full_download_token_query_param(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"token-in-query-param"
        file = _upload_file(server, "qp.txt", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(f"/api/v1/file/{file_id}/download?token={token['_id']}")
        assert resp.status_code == 200
        assert resp.content == content

    def test_full_download_with_name_segment(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        """The /download/{name} variant is cosmetic; content must be equal."""
        content = b"named download"
        file = _upload_file(server, "named.bin", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download/named.bin",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.content == content

    def test_range_request_partial(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"0123456789" * 10  # 100 bytes
        file = _upload_file(server, "range.bin", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download",
            headers={**_auth_headers(token), "Range": "bytes=0-9"},
        )
        assert resp.status_code == 206
        assert resp.content == b"0123456789"
        assert resp.headers["Content-Range"] == "bytes 0-9/100"
        assert resp.headers["Content-Length"] == "10"

    def test_range_request_mid_file(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"abcdefghij" * 10  # 100 bytes
        file = _upload_file(server, "midrange.bin", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download",
            headers={**_auth_headers(token), "Range": "bytes=10-19"},
        )
        assert resp.status_code == 206
        assert resp.content == b"abcdefghij"
        assert resp.headers["Content-Range"] == "bytes 10-19/100"

    def test_open_ended_range(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"xyz" * 10  # 30 bytes
        file = _upload_file(server, "openrange.bin", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download",
            headers={**_auth_headers(token), "Range": "bytes=0-"},
        )
        assert resp.status_code == 206
        assert resp.content == content

    def test_content_disposition_attachment(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"disp"
        file = _upload_file(server, "disp.txt", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download", headers=_auth_headers(token)
        )
        assert "attachment" in resp.headers.get("Content-Disposition", "")
        assert "disp.txt" in resp.headers.get("Content-Disposition", "")

    def test_content_disposition_inline(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"inline-disp"
        file = _upload_file(server, "inline.txt", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download?contentDisposition=inline",
            headers=_auth_headers(token),
        )
        assert resp.headers.get("Content-Disposition", "").startswith("inline")

    def test_offset_query_param(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"ABCDEFGHIJ"  # 10 bytes
        file = _upload_file(server, "offset.bin", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download?offset=5",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.content == b"FGHIJ"


# ---------------------------------------------------------------------------
# /api/v1/file/{file_id}/download – error cases
# ---------------------------------------------------------------------------


class TestFileDownloadErrors:
    def test_unauthenticated_returns_403(
        self, server, http, admin, fsAssetstore, token
    ):
        private_folder = _get_private_folder(admin)
        content = b"secret"
        file = _upload_file(server, "secret.txt", content, admin, private_folder)
        file_id = str(file["_id"])

        resp = http.get(f"/api/v1/file/{file_id}/download")
        assert resp.status_code == 403

    def test_invalid_token_returns_403(self, server, http, admin, fsAssetstore):
        # An invalid/expired token is treated as anonymous access.
        # A file in admin's Private folder must therefore be 403.
        private_folder = _get_private_folder(admin)
        file = _upload_file(server, "priv_token.bin", b"x", admin, private_folder)
        resp = http.get(
            f"/api/v1/file/{file['_id']}/download",
            headers={"Girder-Token": "not-a-valid-token"},
        )
        assert resp.status_code == 403

    def test_nonexistent_file_returns_404(self, http, token):
        resp = http.get(
            "/api/v1/file/000000000000000000000000/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 404

    def test_user_cannot_read_private_file(
        self, server, http, admin, user, fsAssetstore, user_token
    ):
        """Files in admin's private folder must not be accessible with another user's token."""
        folders = list(Folder().childFolders(admin, parentType="user", user=admin))
        private_folder = next(f for f in folders if f["name"] == "Private")
        content = b"admin private data"
        file = _upload_file(server, "priv.bin", content, admin, private_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download",
            headers=_auth_headers(user_token),
        )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# /api/v1/item/{item_id}/download – single-file pass-through
# ---------------------------------------------------------------------------


class TestItemDownloadSingleFile:
    def test_single_file_item_returns_file_content(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"item single file"
        file = _upload_file(server, "item_single.txt", content, admin, public_folder)

        # Each uploadFile call creates an item with one file.
        items = list(Folder().childItems(public_folder))
        item = next(i for i in items if i["name"] == "item_single.txt")

        resp = http.get(
            f"/api/v1/item/{item['_id']}/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.content == content

    def test_single_file_item_format_zip_returns_zip(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"force zip"
        file = _upload_file(server, "item_zip.bin", content, admin, public_folder)
        items = list(Folder().childItems(public_folder))
        item = next(i for i in items if i["name"] == "item_zip.bin")

        resp = http.get(
            f"/api/v1/item/{item['_id']}/download?format=zip",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        names = z.namelist()
        assert any("item_zip.bin" in n for n in names)


# ---------------------------------------------------------------------------
# /api/v1/item/{item_id}/download – multi-file zip
# ---------------------------------------------------------------------------


class TestItemDownloadZip:
    def test_multi_file_item_returns_zip(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        # Create a dedicated item and upload two files into it
        item = Item().createItem(name="multi_item", creator=admin, folder=public_folder)
        for i, body in enumerate([b"file one content", b"file two content"]):
            upload = Upload().uploadFromFile(
                io.BytesIO(body),
                size=len(body),
                name=f"part{i}.txt",
                parentType="item",
                parent=item,
                user=admin,
            )

        resp = http.get(
            f"/api/v1/item/{item['_id']}/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        names = z.namelist()
        assert any("part0.txt" in n for n in names)
        assert any("part1.txt" in n for n in names)


# ---------------------------------------------------------------------------
# /api/v1/item/{item_id}/download – error cases
# ---------------------------------------------------------------------------


class TestItemDownloadErrors:
    def test_unauthenticated_returns_403(
        self, server, http, admin, fsAssetstore, token
    ):
        private_folder = _get_private_folder(admin)
        content = b"item secret"
        _upload_file(server, "item_secret.txt", content, admin, private_folder)
        items = list(Folder().childItems(private_folder))
        item = next(i for i in items if i["name"] == "item_secret.txt")

        resp = http.get(f"/api/v1/item/{item['_id']}/download")
        assert resp.status_code == 403

    def test_nonexistent_item_returns_404(self, http, token):
        resp = http.get(
            "/api/v1/item/000000000000000000000000/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# /api/v1/folder/{folder_id}/download
# ---------------------------------------------------------------------------


class TestFolderDownload:
    def test_folder_download_returns_zip(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        # Create a sub-folder with a file for a clean test
        sub = Folder().createFolder(
            parent=public_folder, name="zip_sub", creator=admin, parentType="folder"
        )
        Upload().uploadFromFile(
            io.BytesIO(b"folder zip test"),
            size=len(b"folder zip test"),
            name="f.txt",
            parentType="folder",
            parent=sub,
            user=admin,
        )

        resp = http.get(
            f"/api/v1/folder/{sub['_id']}/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        assert any("f.txt" in n for n in z.namelist())

    def test_folder_download_mime_filter(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        import json as _json

        sub = Folder().createFolder(
            parent=public_folder,
            name="mime_filter_sub",
            creator=admin,
            parentType="folder",
        )
        for name, mime, body in [
            ("keep.txt", "text/plain", b"keep"),
            ("drop.bin", "application/octet-stream", b"drop"),
        ]:
            upload = Upload().uploadFromFile(
                io.BytesIO(body),
                size=len(body),
                name=name,
                parentType="folder",
                parent=sub,
                user=admin,
                mimeType=mime,
            )

        mime_filter = _json.dumps(["text/plain"])
        resp = http.get(
            f"/api/v1/folder/{sub['_id']}/download?mimeFilter={mime_filter}",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        names = z.namelist()
        assert any("keep.txt" in n for n in names)
        assert not any("drop.bin" in n for n in names)

    def test_folder_download_content_disposition(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        resp = http.get(
            f"/api/v1/folder/{public_folder['_id']}/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        cd = resp.headers.get("Content-Disposition", "")
        assert "attachment" in cd
        assert public_folder["name"] in cd


# ---------------------------------------------------------------------------
# /api/v1/folder/{folder_id}/download – error cases
# ---------------------------------------------------------------------------


class TestFolderDownloadErrors:
    def test_unauthenticated_returns_403(self, http, admin, db):
        private_folder = _get_private_folder(admin)
        resp = http.get(f"/api/v1/folder/{private_folder['_id']}/download")
        assert resp.status_code == 403

    def test_nonexistent_folder_returns_404(self, http, token):
        resp = http.get(
            "/api/v1/folder/000000000000000000000000/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 404

    def test_user_cannot_read_private_folder(
        self, server, http, admin, fsAssetstore, user_token
    ):
        folders = list(Folder().childFolders(admin, parentType="user", user=admin))
        private_folder = next(f for f in folders if f["name"] == "Private")

        resp = http.get(
            f"/api/v1/folder/{private_folder['_id']}/download",
            headers=_auth_headers(user_token),
        )
        assert resp.status_code == 403
