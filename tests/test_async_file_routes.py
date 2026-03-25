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

from girder_async_routes.routes import async_file_routes

from girder.constants import TokenScope
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
    return Token().createToken(admin, scope=[TokenScope.DATA_READ])


@pytest.fixture
def user_token(user):
    """Create and return a Girder token for the regular user."""
    return Token().createToken(user, scope=[TokenScope.DATA_READ])


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

    def test_private_file_accessible_with_owner_token(
        self, server, http, admin, fsAssetstore, token
    ):
        """Owner can download their own private file – exercises the successful
        _authenticate() return path (routes.py line 141)."""
        private_folder = _get_private_folder(admin)
        content = b"private content"
        file = _upload_file(server, "priv_owner.txt", content, admin, private_folder)

        resp = http.get(
            f"/api/v1/file/{file['_id']}/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.content == content


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

    def test_private_item_accessible_with_owner_token(
        self, server, http, admin, fsAssetstore, token
    ):
        """Owner can download an item from their private folder."""
        private_folder = _get_private_folder(admin)
        content = b"private item content"
        _upload_file(server, "priv_item_owner.txt", content, admin, private_folder)
        items = list(Folder().childItems(private_folder))
        item = next(i for i in items if i["name"] == "priv_item_owner.txt")

        resp = http.get(
            f"/api/v1/item/{item['_id']}/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.content == content


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

    def test_private_folder_accessible_with_owner_token(
        self, server, http, admin, fsAssetstore, token
    ):
        """Owner can download their own private folder as a zip."""
        private_folder = _get_private_folder(admin)

        resp = http.get(
            f"/api/v1/folder/{private_folder['_id']}/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

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


# ---------------------------------------------------------------------------
# Link-URL files  (files that reference an external URL, not an assetstore)
# ---------------------------------------------------------------------------


class TestLinkFileDownload:
    """
    Girder supports "link files" – File documents that carry a ``linkUrl``
    field instead of an ``assetstoreId``.  The async route must issue a
    307 redirect to that URL.
    """

    EXTERNAL_URL = "https://example.com/remote-file.txt"

    @pytest.fixture
    def link_file(self, admin, public_folder):
        from girder.models.file import File as FileModel

        return FileModel().createLinkFile(
            name="remote.txt",
            parent=public_folder,
            parentType="folder",
            url=self.EXTERNAL_URL,
            creator=admin,
        )

    def test_link_file_redirects(self, link_file, http, token):
        """Downloading a link file must return a 307 redirect to the external URL."""
        resp = http.get(
            f"/api/v1/file/{link_file['_id']}/download",
            headers=_auth_headers(token),
            follow_redirects=False,
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == self.EXTERNAL_URL

    def test_link_file_via_item_redirects(self, link_file, http, token):
        """When an item's sole file is a link file, the item download must also redirect."""
        resp = http.get(
            f"/api/v1/item/{link_file['itemId']}/download",
            headers=_auth_headers(token),
            follow_redirects=False,
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == self.EXTERNAL_URL

    def test_unauthenticated_link_file_public_folder_allows_access(
        self, link_file, http
    ):
        """A link file in a public folder is accessible without a token."""
        resp = http.get(
            f"/api/v1/file/{link_file['_id']}/download",
            follow_redirects=False,
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == self.EXTERNAL_URL


# ---------------------------------------------------------------------------
# Mixed-content folder  (filesystem file + link file)
# ---------------------------------------------------------------------------


class TestFolderDownloadMixedContent:
    """
    A folder that contains both a regular filesystem-stored file and a link
    file should produce a valid ZIP where:
    - the regular file is present with its actual byte content, and
    - the link file is present with its URL as the entry content (Girder's
      behaviour when building ZIPs: headers=False path in File.download()).
    """

    EXTERNAL_URL = "https://example.com/linked-asset.bin"

    def test_folder_with_filesystem_and_link_file(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        from girder.models.file import File as FileModel

        sub = Folder().createFolder(
            parent=public_folder,
            name="mixed_content_sub",
            creator=admin,
            parentType="folder",
        )

        # Regular file stored in the filesystem assetstore
        real_content = b"real file bytes"
        Upload().uploadFromFile(
            io.BytesIO(real_content),
            size=len(real_content),
            name="real.txt",
            parentType="folder",
            parent=sub,
            user=admin,
        )

        # Link file – no assetstore, just a URL reference
        FileModel().createLinkFile(
            name="link.txt",
            parent=sub,
            parentType="folder",
            url=self.EXTERNAL_URL,
            creator=admin,
        )

        resp = http.get(
            f"/api/v1/folder/{sub['_id']}/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        names = z.namelist()

        # Both entries must be present in the archive
        real_entry = next((n for n in names if "real.txt" in n), None)
        link_entry = next((n for n in names if "link.txt" in n), None)
        assert real_entry is not None, f"real.txt missing from ZIP; entries: {names}"
        assert link_entry is not None, f"link.txt missing from ZIP; entries: {names}"

        # The regular file must contain the original bytes
        assert z.read(real_entry) == real_content

        # The link file entry contains the URL string (Girder's zip behaviour)
        assert z.read(link_entry) == self.EXTERNAL_URL.encode()


# ---------------------------------------------------------------------------
# Non-filesystem assetstore  (WSGI-backed streaming path)
# ---------------------------------------------------------------------------


class TestNonFilesystemAssetstore:
    """
    Exercises the ``_wsgi_backed_stream`` code path in routes.py that is used
    whenever ``local_path`` is None (i.e. the file lives on a non-filesystem
    assetstore such as S3 or GridFS).

    Rather than spinning up a real S3 server we upload a real file to the
    filesystem assetstore and then patch ``girder_async_routes.routes._resolve``
    to strip ``local_path`` from the result, forcing the handler to fall
    through to the WSGI-backed streaming path while still using the real
    Girder ``FileModel.download()`` implementation to produce the bytes.
    """

    def test_wsgi_backed_full_download(
        self, server, http, admin, fsAssetstore, public_folder, token, monkeypatch
    ):
        """Full download via the WSGI-backed (non-filesystem) streaming path."""
        import girder_async_routes.routes as _routes

        content = b"non-fs assetstore content" * 10
        file = _upload_file(server, "nonfs.bin", content, admin, public_folder)

        _orig_resolve = _routes._resolve

        def _patched_resolve(file_id, token_str, offset, end_byte):
            result = _orig_resolve(file_id, token_str, offset, end_byte)
            # Strip local_path to simulate a non-filesystem assetstore adapter
            result.pop("local_path", None)
            return result

        monkeypatch.setattr(_routes, "_resolve", _patched_resolve)

        resp = http.get(
            f"/api/v1/file/{file['_id']}/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.content == content

    def test_wsgi_backed_range_request(
        self, server, http, admin, fsAssetstore, public_folder, token, monkeypatch
    ):
        """Partial (Range) download via the WSGI-backed streaming path."""
        import girder_async_routes.routes as _routes

        content = b"0123456789" * 10  # 100 bytes
        file = _upload_file(server, "nonfs_range.bin", content, admin, public_folder)

        _orig_resolve = _routes._resolve

        def _patched_resolve(file_id, token_str, offset, end_byte):
            result = _orig_resolve(file_id, token_str, offset, end_byte)
            result.pop("local_path", None)
            return result

        monkeypatch.setattr(_routes, "_resolve", _patched_resolve)

        resp = http.get(
            f"/api/v1/file/{file['_id']}/download",
            headers={**_auth_headers(token), "Range": "bytes=10-19"},
        )
        assert resp.status_code == 206
        assert resp.content == b"0123456789"
        assert resp.headers["Content-Range"] == "bytes 10-19/100"


# ---------------------------------------------------------------------------
# GET|POST /api/v1/resource/download  (multi-resource bulk zip)
# ---------------------------------------------------------------------------


class TestResourceDownload:
    """
    Covers the bulk ``GET /api/v1/resource/download`` (and POST variant) that
    lets callers request a zip of arbitrary items, folders, etc. in one shot —
    mirroring Girder's ``resource.py::Resource.download()``.
    """

    def test_download_single_item(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        import json as _json

        content = b"resource item content"
        _upload_file(server, "res_item.txt", content, admin, public_folder)
        items = list(Folder().childItems(public_folder))
        item = next(i for i in items if i["name"] == "res_item.txt")

        resources = _json.dumps({"item": [str(item["_id"])]})
        resp = http.get(
            f"/api/v1/resource/download?resources={resources}",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"
        assert "Resources.zip" in resp.headers.get("Content-Disposition", "")

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        assert any("res_item.txt" in n for n in z.namelist())

    def test_download_single_folder(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        import json as _json

        sub = Folder().createFolder(
            parent=public_folder,
            name="res_folder_sub",
            creator=admin,
            parentType="folder",
        )
        Upload().uploadFromFile(
            io.BytesIO(b"folder file bytes"),
            size=len(b"folder file bytes"),
            name="res_folder_file.txt",
            parentType="folder",
            parent=sub,
            user=admin,
        )

        resources = _json.dumps({"folder": [str(sub["_id"])]})
        resp = http.get(
            f"/api/v1/resource/download?resources={resources}",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        assert any("res_folder_file.txt" in n for n in z.namelist())

    def test_download_mixed_resources(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        import json as _json

        # An item
        _upload_file(server, "mixed_item.txt", b"item bytes", admin, public_folder)
        items = list(Folder().childItems(public_folder))
        item = next(i for i in items if i["name"] == "mixed_item.txt")

        # A folder with a file
        sub = Folder().createFolder(
            parent=public_folder,
            name="mixed_folder_sub",
            creator=admin,
            parentType="folder",
        )
        Upload().uploadFromFile(
            io.BytesIO(b"folder bytes"),
            size=len(b"folder bytes"),
            name="mixed_folder_file.txt",
            parentType="folder",
            parent=sub,
            user=admin,
        )

        resources = _json.dumps(
            {"item": [str(item["_id"])], "folder": [str(sub["_id"])]}
        )
        resp = http.get(
            f"/api/v1/resource/download?resources={resources}",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        names = z.namelist()
        assert any("mixed_item.txt" in n for n in names)
        assert any("mixed_folder_file.txt" in n for n in names)

    def test_post_method_works(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        import json as _json

        content = b"post method content"
        _upload_file(server, "post_item.txt", content, admin, public_folder)
        items = list(Folder().childItems(public_folder))
        item = next(i for i in items if i["name"] == "post_item.txt")

        resources = _json.dumps({"item": [str(item["_id"])]})
        resp = http.post(
            "/api/v1/resource/download",
            data={"resources": resources},
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        assert any("post_item.txt" in n for n in z.namelist())

    def test_unauthenticated_private_resource_returns_403(
        self, server, http, admin, fsAssetstore
    ):
        import json as _json

        private_folder = _get_private_folder(admin)
        resources = _json.dumps({"folder": [str(private_folder["_id"])]})
        resp = http.get(f"/api/v1/resource/download?resources={resources}")
        assert resp.status_code == 403

    def test_nonexistent_resource_returns_404(self, http, token):
        import json as _json

        resources = _json.dumps({"item": ["000000000000000000000000"]})
        resp = http.get(
            f"/api/v1/resource/download?resources={resources}",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 404

    def test_missing_resources_param_returns_400(self, http, token):
        resp = http.get(
            "/api/v1/resource/download",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 400

    def test_invalid_resource_type_returns_400(self, http, token):
        import json as _json

        resources = _json.dumps({"banana": ["000000000000000000000000"]})
        resp = http.get(
            f"/api/v1/resource/download?resources={resources}",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 400

    def test_include_metadata_flag(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        """When includeMetadata=true, Girder adds a JSON sidecar to the zip."""
        import json as _json

        # Create an item with metadata
        item = Item().createItem(name="meta_item", creator=admin, folder=public_folder)
        Item().setMetadata(item, {"key": "value"})
        Upload().uploadFromFile(
            io.BytesIO(b"meta content"),
            size=len(b"meta content"),
            name="meta_file.txt",
            parentType="item",
            parent=item,
            user=admin,
        )

        resources = _json.dumps({"item": [str(item["_id"])]})
        resp = http.get(
            f"/api/v1/resource/download?resources={resources}&includeMetadata=true",
            headers=_auth_headers(token),
        )
        assert resp.status_code == 200
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        names = z.namelist()
        # Girder appends a .json metadata sidecar alongside the file
        assert any(n.endswith(".json") for n in names), (
            f"Expected a .json metadata entry but got: {names}"
        )
