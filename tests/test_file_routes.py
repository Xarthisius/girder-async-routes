"""Integration tests for the async file download routes."""

import pytest

import girder_async_routes.file as _file_module
from girder.models.folder import Folder

from .conftest import auth_headers, get_private_folder, upload_file


# ---------------------------------------------------------------------------
# /api/v1/file/{file_id}/download  (filesystem assetstore)
# ---------------------------------------------------------------------------


class TestFileDownload:
    def test_full_download(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"Hello, async world!" * 100
        file = upload_file(server, "hello.txt", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(f"/api/v1/file/{file_id}/download", headers=auth_headers(token))

        assert resp.status_code == 200
        assert resp.content == content

    def test_full_download_token_query_param(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"token-in-query-param"
        file = upload_file(server, "qp.txt", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(f"/api/v1/file/{file_id}/download?token={token['_id']}")
        assert resp.status_code == 200
        assert resp.content == content

    def test_full_download_with_name_segment(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        """The /download/{name} variant is cosmetic; content must be equal."""
        content = b"named download"
        file = upload_file(server, "named.bin", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download/named.bin",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.content == content

    def test_range_request_partial(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"0123456789" * 10  # 100 bytes
        file = upload_file(server, "range.bin", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download",
            headers={**auth_headers(token), "Range": "bytes=0-9"},
        )
        assert resp.status_code == 206
        assert resp.content == b"0123456789"
        assert resp.headers["Content-Range"] == "bytes 0-9/100"
        assert resp.headers["Content-Length"] == "10"

    def test_range_request_mid_file(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"abcdefghij" * 10  # 100 bytes
        file = upload_file(server, "midrange.bin", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download",
            headers={**auth_headers(token), "Range": "bytes=10-19"},
        )
        assert resp.status_code == 206
        assert resp.content == b"abcdefghij"
        assert resp.headers["Content-Range"] == "bytes 10-19/100"

    def test_open_ended_range(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"xyz" * 10  # 30 bytes
        file = upload_file(server, "openrange.bin", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download",
            headers={**auth_headers(token), "Range": "bytes=0-"},
        )
        assert resp.status_code == 206
        assert resp.content == content

    def test_content_disposition_attachment(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"disp"
        file = upload_file(server, "disp.txt", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(f"/api/v1/file/{file_id}/download", headers=auth_headers(token))
        assert "attachment" in resp.headers.get("Content-Disposition", "")
        assert "disp.txt" in resp.headers.get("Content-Disposition", "")

    def test_content_disposition_inline(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"inline-disp"
        file = upload_file(server, "inline.txt", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download?contentDisposition=inline",
            headers=auth_headers(token),
        )
        assert resp.headers.get("Content-Disposition", "").startswith("inline")

    def test_offset_query_param(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"ABCDEFGHIJ"  # 10 bytes
        file = upload_file(server, "offset.bin", content, admin, public_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download?offset=5",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.content == b"FGHIJ"

    def test_private_file_accessible_with_owner_token(
        self, server, http, admin, fsAssetstore, token
    ):
        private_folder = get_private_folder(admin)
        content = b"private content"
        file = upload_file(server, "priv_owner.txt", content, admin, private_folder)

        resp = http.get(
            f"/api/v1/file/{file['_id']}/download",
            headers=auth_headers(token),
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
        private_folder = get_private_folder(admin)
        content = b"secret"
        file = upload_file(server, "secret.txt", content, admin, private_folder)
        file_id = str(file["_id"])

        resp = http.get(f"/api/v1/file/{file_id}/download")
        assert resp.status_code == 403

    def test_invalid_token_returns_403(self, server, http, admin, fsAssetstore):
        private_folder = get_private_folder(admin)
        file = upload_file(server, "priv_token.bin", b"x", admin, private_folder)
        resp = http.get(
            f"/api/v1/file/{file['_id']}/download",
            headers={"Girder-Token": "not-a-valid-token"},
        )
        assert resp.status_code == 403

    def test_nonexistent_file_returns_404(self, http, token):
        resp = http.get(
            "/api/v1/file/000000000000000000000000/download",
            headers=auth_headers(token),
        )
        assert resp.status_code == 404

    def test_user_cannot_read_private_file(
        self, server, http, admin, user, fsAssetstore, user_token
    ):
        folders = list(Folder().childFolders(admin, parentType="user", user=admin))
        private_folder = next(f for f in folders if f["name"] == "Private")
        content = b"admin private data"
        file = upload_file(server, "priv.bin", content, admin, private_folder)
        file_id = str(file["_id"])

        resp = http.get(
            f"/api/v1/file/{file_id}/download",
            headers=auth_headers(user_token),
        )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Link-URL files
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
        resp = http.get(
            f"/api/v1/file/{link_file['_id']}/download",
            headers=auth_headers(token),
            follow_redirects=False,
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == self.EXTERNAL_URL

    def test_link_file_via_item_redirects(self, link_file, http, token):
        resp = http.get(
            f"/api/v1/item/{link_file['itemId']}/download",
            headers=auth_headers(token),
            follow_redirects=False,
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == self.EXTERNAL_URL

    def test_unauthenticated_link_file_public_folder_allows_access(
        self, link_file, http
    ):
        resp = http.get(
            f"/api/v1/file/{link_file['_id']}/download",
            follow_redirects=False,
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == self.EXTERNAL_URL


# ---------------------------------------------------------------------------
# Non-filesystem assetstore  (WSGI-backed streaming path)
# ---------------------------------------------------------------------------


class TestNonFilesystemAssetstore:
    """
    Exercises the ``_wsgi_backed_stream`` code path used when ``local_path``
    is None (e.g. S3, GridFS).  We patch ``_resolve`` to strip the
    ``local_path`` key, forcing the fallback path while still using the real
    Girder ``FileModel.download()`` implementation.
    """

    def test_wsgi_backed_full_download(
        self, server, http, admin, fsAssetstore, public_folder, token, monkeypatch
    ):
        content = b"non-fs assetstore content" * 10
        file = upload_file(server, "nonfs.bin", content, admin, public_folder)

        _orig_resolve = _file_module._resolve

        def _patched_resolve(file_id, token_str, offset, end_byte):
            result = _orig_resolve(file_id, token_str, offset, end_byte)
            result.pop("local_path", None)
            return result

        monkeypatch.setattr(_file_module, "_resolve", _patched_resolve)

        resp = http.get(
            f"/api/v1/file/{file['_id']}/download",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.content == content

    def test_wsgi_backed_range_request(
        self, server, http, admin, fsAssetstore, public_folder, token, monkeypatch
    ):
        content = b"0123456789" * 10  # 100 bytes
        file = upload_file(server, "nonfs_range.bin", content, admin, public_folder)

        _orig_resolve = _file_module._resolve

        def _patched_resolve(file_id, token_str, offset, end_byte):
            result = _orig_resolve(file_id, token_str, offset, end_byte)
            result.pop("local_path", None)
            return result

        monkeypatch.setattr(_file_module, "_resolve", _patched_resolve)

        resp = http.get(
            f"/api/v1/file/{file['_id']}/download",
            headers={**auth_headers(token), "Range": "bytes=10-19"},
        )
        assert resp.status_code == 206
        assert resp.content == b"0123456789"
        assert resp.headers["Content-Range"] == "bytes 10-19/100"
