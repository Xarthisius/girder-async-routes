"""Integration tests for the async folder download routes."""

import io
import json as _json
import zipfile

from girder.models.folder import Folder
from girder.models.upload import Upload

from .conftest import auth_headers, get_private_folder

# ---------------------------------------------------------------------------
# /api/v1/folder/{folder_id}/download
# ---------------------------------------------------------------------------


class TestFolderDownload:
    def test_folder_download_returns_zip(
        self, server, http, admin, fsAssetstore, public_folder, token,
    ):
        sub = Folder().createFolder(
            parent=public_folder, name="zip_sub", creator=admin, parentType="folder",
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
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        assert any("f.txt" in n for n in z.namelist())

    def test_folder_download_mime_filter(
        self, server, http, admin, fsAssetstore, public_folder, token,
    ):
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
            Upload().uploadFromFile(
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
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        names = z.namelist()
        assert any("keep.txt" in n for n in names)
        assert not any("drop.bin" in n for n in names)

    def test_folder_download_content_disposition(
        self, server, http, admin, fsAssetstore, public_folder, token,
    ):
        resp = http.get(
            f"/api/v1/folder/{public_folder['_id']}/download",
            headers=auth_headers(token),
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
        private_folder = get_private_folder(admin)
        resp = http.get(f"/api/v1/folder/{private_folder['_id']}/download")
        assert resp.status_code == 403

    def test_nonexistent_folder_returns_404(self, http, token):
        resp = http.get(
            "/api/v1/folder/000000000000000000000000/download",
            headers=auth_headers(token),
        )
        assert resp.status_code == 404

    def test_private_folder_accessible_with_owner_token(
        self, server, http, admin, fsAssetstore, token,
    ):
        private_folder = get_private_folder(admin)

        resp = http.get(
            f"/api/v1/folder/{private_folder['_id']}/download",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

    def test_user_cannot_read_private_folder(
        self, server, http, admin, fsAssetstore, user_token,
    ):
        folders = list(Folder().childFolders(admin, parentType="user", user=admin))
        private_folder = next(f for f in folders if f["name"] == "Private")

        resp = http.get(
            f"/api/v1/folder/{private_folder['_id']}/download",
            headers=auth_headers(user_token),
        )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Mixed-content folder  (filesystem file + link file)
# ---------------------------------------------------------------------------


class TestFolderDownloadMixedContent:
    """A folder containing both a regular filesystem-stored file and a link file
    should produce a valid ZIP where the regular file has its actual bytes and
    the link file entry contains the URL (Girder's ZIP behaviour).
    """

    EXTERNAL_URL = "https://example.com/linked-asset.bin"

    def test_folder_with_filesystem_and_link_file(
        self, server, http, admin, fsAssetstore, public_folder, token,
    ):
        from girder.models.file import File as FileModel

        sub = Folder().createFolder(
            parent=public_folder,
            name="mixed_content_sub",
            creator=admin,
            parentType="folder",
        )

        real_content = b"real file bytes"
        Upload().uploadFromFile(
            io.BytesIO(real_content),
            size=len(real_content),
            name="real.txt",
            parentType="folder",
            parent=sub,
            user=admin,
        )

        FileModel().createLinkFile(
            name="link.txt",
            parent=sub,
            parentType="folder",
            url=self.EXTERNAL_URL,
            creator=admin,
        )

        resp = http.get(
            f"/api/v1/folder/{sub['_id']}/download",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        names = z.namelist()

        real_entry = next((n for n in names if "real.txt" in n), None)
        link_entry = next((n for n in names if "link.txt" in n), None)
        assert real_entry is not None, f"real.txt missing from ZIP; entries: {names}"
        assert link_entry is not None, f"link.txt missing from ZIP; entries: {names}"

        assert z.read(real_entry) == real_content
        assert z.read(link_entry) == self.EXTERNAL_URL.encode()
