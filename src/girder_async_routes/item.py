"""Async-native item download route."""

from __future__ import annotations

import anyio
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from girder.constants import AccessType
from girder.exceptions import AccessException
from girder.models.item import Item as ItemModel
from girder.utility import ziputil

from .file import _handle_download
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


def _resolve_item(item_id: str, token_str: str | None, format_param: str | None):
    """
    Authenticate, ACL-check, and decide whether this is a single-file
    pass-through or a multi-file zip.  Runs in a thread pool.
    """
    user, _ = _authenticate(token_str)
    try:
        item = ItemModel().load(item_id, user=user, level=AccessType.READ)
    except AccessException:
        return {"status_code": 403}
    if not item:
        return {"status_code": 404}

    files = list(ItemModel().childFiles(item=item, limit=2))
    if len(files) == 1 and format_param not in ("zip",):
        return {
            "status_code": 200,
            "type": "single_file",
            "file": files[0],
            "user": user,
        }
    return {"status_code": 200, "type": "zip", "item": item, "user": user}


def _build_item_zip_gen(item, user):
    """Return a sync generator that produces zip bytes for the item.  Runs in a thread."""
    z = ziputil.ZipGenerator(item["name"])

    def stream():
        for path, file_gen in ItemModel().fileList(item, user=user, subpath=False):
            yield from z.addFile(file_gen, path)
        yield z.footer()

    return stream()


# ---------------------------------------------------------------------------
# Async handler
# ---------------------------------------------------------------------------


async def _handle_item_download(request, item_id: str) -> Response:
    token_str = _get_token(request)
    format_param = request.query_params.get("format", "")

    info = await anyio.to_thread.run_sync(
        lambda: _resolve_item(item_id, token_str, format_param)
    )
    status = info.get("status_code", 500)
    if status == 403:
        return _json_error(403, "Access denied.")
    if status == 404:
        return _json_error(404, "Item not found.")

    if info["type"] == "single_file":
        # Delegate entirely to the file handler; offset / Range / contentDisposition
        # are read directly from the request object there.
        return await _handle_download(request, str(info["file"]["_id"]))

    item = info["item"]
    user = info["user"]
    gen = await anyio.to_thread.run_sync(lambda: _build_item_zip_gen(item, user))
    return StreamingResponse(
        _demand_driven_zip_stream(gen),
        status_code=200,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{item["name"]}.zip"'},
    )


# ---------------------------------------------------------------------------
# Route handler
# ---------------------------------------------------------------------------


@_log_access
async def item_download(request):
    return await _handle_item_download(request, request.path_params["item_id"])


item_routes = [
    Route("/api/v1/item/{item_id}/download", item_download, methods=["GET"]),
]
