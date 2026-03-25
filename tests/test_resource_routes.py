"""Integration tests for the async resource download routes."""

import io
import json as _json
import zipfile

from girder.models.folder import Folder
from girder.models.item import Item
from girder.models.upload import Upload

from .conftest import auth_headers, get_private_folder, upload_file

# ---------------------------------------------------------------------------
# GET|POST /api/v1/resource/download  (multi-resource bulk zip)
# ---------------------------------------------------------------------------


class TestResourceDownload:
    def test_download_single_item(
        self, server, http, admin, fsAssetstore, public_folder, token,
    ):
        content = b"resource item content"
        upload_file(server, "res_item.txt", content, admin, public_folder)
        items = list(Folder().childItems(public_folder))
        item = next(i for i in items if i["name"] == "res_item.txt")

        resources = _json.dumps({"item": [str(item["_id"])]})
        resp = http.get(
            f"/api/v1/resource/download?resources={resources}",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"
        assert "Resources.zip" in resp.headers.get("Content-Disposition", "")

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        assert any("res_item.txt" in n for n in z.namelist())

    def test_download_single_folder(
        self, server, http, admin, fsAssetstore, public_folder, token,
    ):
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
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        assert any("res_folder_file.txt" in n for n in z.namelist())

    def test_download_mixed_resources(
        self, server, http, admin, fsAssetstore, public_folder, token,
    ):
        upload_file(server, "mixed_item.txt", b"item bytes", admin, public_folder)
        items = list(Folder().childItems(public_folder))
        item = next(i for i in items if i["name"] == "mixed_item.txt")

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
            {"item": [str(item["_id"])], "folder": [str(sub["_id"])]},
        )
        resp = http.get(
            f"/api/v1/resource/download?resources={resources}",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        names = z.namelist()
        assert any("mixed_item.txt" in n for n in names)
        assert any("mixed_folder_file.txt" in n for n in names)

    def test_post_method_works(
        self, server, http, admin, fsAssetstore, public_folder, token,
    ):
        content = b"post method content"
        upload_file(server, "post_item.txt", content, admin, public_folder)
        items = list(Folder().childItems(public_folder))
        item = next(i for i in items if i["name"] == "post_item.txt")

        resources = _json.dumps({"item": [str(item["_id"])]})
        resp = http.post(
            "/api/v1/resource/download",
            data={"resources": resources},
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/zip"

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        assert any("post_item.txt" in n for n in z.namelist())

    def test_unauthenticated_private_resource_returns_403(
        self, server, http, admin, fsAssetstore,
    ):
        private_folder = get_private_folder(admin)
        resources = _json.dumps({"folder": [str(private_folder["_id"])]})
        resp = http.get(f"/api/v1/resource/download?resources={resources}")
        assert resp.status_code == 403

    def test_nonexistent_resource_returns_404(self, http, token):
        resources = _json.dumps({"item": ["000000000000000000000000"]})
        resp = http.get(
            f"/api/v1/resource/download?resources={resources}",
            headers=auth_headers(token),
        )
        assert resp.status_code == 404

    def test_missing_resources_param_returns_400(self, http, token):
        resp = http.get(
            "/api/v1/resource/download",
            headers=auth_headers(token),
        )
        assert resp.status_code == 400

    def test_invalid_resource_type_returns_400(self, http, token):
        resources = _json.dumps({"banana": ["000000000000000000000000"]})
        resp = http.get(
            f"/api/v1/resource/download?resources={resources}",
            headers=auth_headers(token),
        )
        assert resp.status_code == 400

    def test_include_metadata_flag(
        self, server, http, admin, fsAssetstore, public_folder, token,
    ):
        """When includeMetadata=true, Girder adds a JSON sidecar to the zip."""
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
            headers=auth_headers(token),
        )
        assert resp.status_code == 200
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        names = z.namelist()
        assert any(n.endswith(".json") for n in names), (
            f"Expected a .json metadata entry but got: {names}"
        )
