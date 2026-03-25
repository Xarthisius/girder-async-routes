"""Integration tests for the async item download routes."""

import io
import zipfile

from girder.models.folder import Folder
from girder.models.item import Item
from girder.models.upload import Upload

from .conftest import auth_headers, get_private_folder, upload_file


# ---------------------------------------------------------------------------
# /api/v1/item/{item_id}/download – single-file pass-through
# ---------------------------------------------------------------------------


class TestItemDownloadSingleFile:
    def test_single_file_item_returns_file_content(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"item single file"
        upload_file(server, "item_single.txt", content, admin, public_folder)

        items = list(Folder().childItems(public_folder))
        item = next(i for i in items if i["name"] == "item_single.txt")

        resp = http.get(
            f"/api/v1/item/{item['_id']}/download",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.content == content

    def test_single_file_item_format_zip_returns_zip(
        self, server, http, admin, fsAssetstore, public_folder, token
    ):
        content = b"force zip"
        upload_file(server, "item_zip.bin", content, admin, public_folder)
        items = list(Folder().childItems(public_folder))
        item = next(i for i in items if i["name"] == "item_zip.bin")

        resp = http.get(
            f"/api/v1/item/{item['_id']}/download?format=zip",
            headers=auth_headers(token),
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
        item = Item().createItem(name="multi_item", creator=admin, folder=public_folder)
        for i, body in enumerate([b"file one content", b"file two content"]):
            Upload().uploadFromFile(
                io.BytesIO(body),
                size=len(body),
                name=f"part{i}.txt",
                parentType="item",
                parent=item,
                user=admin,
            )

        resp = http.get(
            f"/api/v1/item/{item['_id']}/download",
            headers=auth_headers(token),
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
        private_folder = get_private_folder(admin)
        content = b"item secret"
        upload_file(server, "item_secret.txt", content, admin, private_folder)
        items = list(Folder().childItems(private_folder))
        item = next(i for i in items if i["name"] == "item_secret.txt")

        resp = http.get(f"/api/v1/item/{item['_id']}/download")
        assert resp.status_code == 403

    def test_nonexistent_item_returns_404(self, http, token):
        resp = http.get(
            "/api/v1/item/000000000000000000000000/download",
            headers=auth_headers(token),
        )
        assert resp.status_code == 404

    def test_private_item_accessible_with_owner_token(
        self, server, http, admin, fsAssetstore, token
    ):
        private_folder = get_private_folder(admin)
        content = b"private item content"
        upload_file(server, "priv_item_owner.txt", content, admin, private_folder)
        items = list(Folder().childItems(private_folder))
        item = next(i for i in items if i["name"] == "priv_item_owner.txt")

        resp = http.get(
            f"/api/v1/item/{item['_id']}/download",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.content == content
