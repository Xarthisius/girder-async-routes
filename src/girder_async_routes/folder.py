"""Async-native folder download route."""

from __future__ import annotations

import json

import anyio
from girder.constants import AccessType
from girder.exceptions import AccessException
from girder.models.folder import Folder as FolderModel
from girder.utility import ziputil
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from .utils import (
    _authenticate,
    _demand_driven_zip_stream,
    _get_token,
    _json_error,
    _log_access,
)

# ---------------------------------------------------------------------------
# Blocking helpers – called via anyio.to_thread.run_sync
# ---------------------------------------------------------------------------


def _resolve_folder(folder_id: str, token_str: str | None):
    """Authenticate and ACL-check a folder.  Runs in a thread pool."""
    user, _ = _authenticate(token_str)
    try:
        folder = FolderModel().load(folder_id, user=user, level=AccessType.READ)
    except AccessException:
        return {"status_code": 403}
    if not folder:
        return {"status_code": 404}
    return {"status_code": 200, "folder": folder, "user": user}


def _build_folder_zip_gen(folder, user, mime_filter=None):
    """Return a sync generator that produces zip bytes for the folder.  Runs in a thread."""
    z = ziputil.ZipGenerator(folder["name"])

    def stream():
        for path, file_gen in FolderModel().fileList(
            folder, user=user, subpath=False, mimeFilter=mime_filter,
        ):
            yield from z.addFile(file_gen, path)
        yield z.footer()

    return stream()


# ---------------------------------------------------------------------------
# Async handler
# ---------------------------------------------------------------------------


async def _handle_folder_download(request, folder_id: str) -> Response:
    token_str = _get_token(request)
    mime_filter = None
    raw = request.query_params.get("mimeFilter")
    if raw:
        try:
            mime_filter = json.loads(raw)
        except (ValueError, TypeError):
            mime_filter = None

    info = await anyio.to_thread.run_sync(lambda: _resolve_folder(folder_id, token_str))
    status = info.get("status_code", 500)
    if status == 403:
        return _json_error(403, "Access denied.")
    if status == 404:
        return _json_error(404, "Folder not found.")

    folder = info["folder"]
    user = info["user"]
    gen = await anyio.to_thread.run_sync(
        lambda: _build_folder_zip_gen(folder, user, mime_filter),
    )
    return StreamingResponse(
        _demand_driven_zip_stream(gen),
        status_code=200,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{folder["name"]}.zip"'},
    )


# ---------------------------------------------------------------------------
# Route handler
# ---------------------------------------------------------------------------


@_log_access
async def folder_download(request):
    return await _handle_folder_download(request, request.path_params["folder_id"])


folder_routes = [
    Route("/api/v1/folder/{folder_id}/download", folder_download, methods=["GET"]),
]
