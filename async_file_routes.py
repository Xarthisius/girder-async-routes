"""
Async-native file download routes.

Root cause of the buffering problem
-------------------------------------
Starlette's WSGIMiddleware uses::

    anyio.create_memory_object_stream(math.inf)

The WSGI thread pushes every chunk into this *unbounded* queue without
waiting for the client to drain it.  For large files with a slow consumer
this means the entire file content accumulates in memory inside the worker.

Fix
---
Register native ASGI ``Route`` objects for the two Girder download paths
*before* the ``Mount('/', WSGIMiddleware(...))`` catch-all in asgi.py.
Starlette tries routes in order, so these are matched first; the WSGI layer
never sees a download request.

- **Filesystem assetstore** → ``anyio.open_file`` (true async I/O, backpressure
  provided by the ``StreamingResponse`` / ``FileResponse`` caller awaiting each
  ``send()`` before requesting the next chunk).
- **Non-filesystem assetstore** (S3, GridFS, …) → the sync Girder generator is
  iterated *one chunk at a time* inside ``anyio.to_thread.run_sync``, so the
  event-loop only advances to the next chunk after Uvicorn has handed the
  previous one off to the OS send-buffer.  This converts the unbounded queue
  problem into bounded, demand-driven iteration.
- **Link files** → 302 redirect, identical to CherryPy behaviour.

All Girder auth, ACL checks, and download events are preserved.
"""

from __future__ import annotations

import datetime
import logging
import os

import anyio
from girder import events
from girder.constants import AccessType, TokenScope
from girder.exceptions import AccessException

# Resolve to a local path for filesystem assetstores
from girder.models.assetstore import Assetstore
from girder.models.file import File as FileModel
from girder.models.token import Token
from girder.models.user import User
from girder.utility.assetstore_utilities import getAssetstoreAdapter
from girder.utility.filesystem_assetstore_adapter import FilesystemAssetstoreAdapter
from starlette.responses import (
    RedirectResponse,
    Response,
    StreamingResponse,
)
from starlette.routing import Route

logger = logging.getLogger(__name__)

BUF_SIZE = 65536  # 64 KiB — matches filesystem_assetstore_adapter


# ---------------------------------------------------------------------------
# Helpers – all blocking Girder code, called via anyio.to_thread.run_sync
# ---------------------------------------------------------------------------


def _authenticate(token_str: str | None):
    """Return (user, token) or (None, None)."""
    if not token_str:
        return None, None

    token = Token().load(token_str, force=True, objectId=False)
    if (
        token is None
        or token["expires"] < datetime.datetime.now(datetime.timezone.utc)
        or "userId" not in token
        or not Token().hasScope(token, TokenScope.DATA_READ)
    ):
        return None, None

    return User().load(token["userId"], force=True), token


def _resolve(file_id: str, token_str: str | None, offset: int, end_byte: int | None):
    """
    Authenticate, ACL-check, and resolve everything needed to stream the file.
    Returns a dict with keys: file, local_path, link_url, status_code.

    Runs inside a thread pool – blocking Girder calls are fine here.
    """
    user, _ = _authenticate(token_str)

    try:
        file = FileModel().load(file_id, user=user, level=AccessType.READ)
    except AccessException:
        return {"status_code": 403}
    if not file:
        return {"status_code": 404}

    events.trigger(
        "model.file.download.request",
        info={
            "file": file,
            "startByte": offset,
            "endByte": end_byte,
        },
    )

    # Link file, caller will redirect
    if file.get("linkUrl") and not file.get("assetstoreId"):
        return {"status_code": 200, "file": file, "link_url": file["linkUrl"]}

    if not file.get("assetstoreId"):
        return {"status_code": 404}

    assetstore = Assetstore().load(file["assetstoreId"])
    adapter = getAssetstoreAdapter(assetstore)

    local_path = None
    if isinstance(adapter, FilesystemAssetstoreAdapter):
        candidate = adapter.fullPath(file)
        if os.path.isfile(candidate):
            local_path = candidate

    return {
        "status_code": 200,
        "file": file,
        "local_path": local_path,
        "link_url": None,
        "adapter": adapter,
    }


def _build_sync_generator(
    file,
    offset: int,
    end_byte: int | None,
    content_disposition: str,
    extra_parameters=None,
):
    """
    Invoke Girder's WSGI download path with headers suppressed and return
    the raw chunk iterator.  Runs inside a thread pool.
    """

    stream_fn = FileModel().download(
        file,
        offset,
        endByte=end_byte,
        contentDisposition=content_disposition,
        extraParameters=extra_parameters,
    )
    return stream_fn()


def _fire_complete_event(
    file, offset: int, end_byte: int | None, redirect: bool = False
):

    events.trigger(
        "model.file.download.complete",
        info={
            "file": file,
            "startByte": offset,
            "endByte": end_byte,
            "redirect": redirect,
        },
    )


# ---------------------------------------------------------------------------
# Core async handler
# ---------------------------------------------------------------------------


async def _handle_download(request, file_id: str) -> Response:
    token_str = (
        request.query_params.get("token")
        or request.headers.get("Girder-Token")
        or request.cookies.get("girderToken")
    )
    content_disposition = request.query_params.get("contentDisposition", "attachment")
    logger.info(
        "Download request for file %s (token=%s)",
        file_id,
        token_str[:4] if token_str else None,
    )
    # Parse Range header before entering the thread so we can pass offset/end_byte
    # to the event fired inside _resolve.
    file_size_hint = 0  # unknown yet; refined later
    offset, end_byte = _parse_range(request.headers.get("range"), file_size_hint)
    info = await anyio.to_thread.run_sync(
        lambda: _resolve(file_id, token_str, offset, end_byte)
    )
    logger.info(
        "Download request for file %s (offset=%d, end_byte=%s) → status %d",
        file_id,
        offset,
        str(end_byte),
        info.get("status_code", 200),
    )
    status = info.get("status_code", 500)
    if status == 403:
        return Response(
            '{"message": "Access denied."}',
            status_code=403,
            media_type="application/json",
        )
    if status == 404:
        return Response(
            '{"message": "File not found."}',
            status_code=404,
            media_type="application/json",
        )

    file = info["file"]
    link_url = info.get("link_url")

    # Link file: redirect (same as CherryPy behaviour)
    if link_url:
        await anyio.to_thread.run_sync(
            lambda: _fire_complete_event(file, offset, end_byte, redirect=True)
        )
        return RedirectResponse(link_url)

    local_path = info.get("local_path")
    mime_type = file.get("mimeType") or "application/octet-stream"
    filename = file.get("name", "download")
    file_size = file.get("size", 0)

    # Re-parse range now that we know file_size
    offset, end_byte = _parse_range(request.headers.get("range"), file_size)
    is_partial = request.headers.get("range") is not None

    # ── Filesystem assetstore ────────────────────────────────────────────────
    if local_path:
        cd_header = f'{content_disposition}; filename="{filename}"'

        effective_end = min(end_byte, file_size) if end_byte else file_size
        content_length = effective_end - offset

        async def _range_stream():
            try:
                async with await anyio.open_file(local_path, "rb") as f:
                    await f.seek(offset)
                    remaining = content_length
                    while remaining > 0:
                        chunk = await f.read(min(BUF_SIZE, remaining))
                        if not chunk:
                            break
                        remaining -= len(chunk)
                        yield chunk
            finally:
                await anyio.to_thread.run_sync(
                    lambda: _fire_complete_event(file, offset, end_byte)
                )

        return StreamingResponse(
            _range_stream(),
            status_code=206,
            media_type=mime_type,
            headers={
                "Content-Range": f"bytes {offset}-{effective_end - 1}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(content_length),
                "Content-Disposition": cd_header,
            },
        )

    # ── Non-filesystem assetstore (S3, GridFS, …) ───────────────────────────
    # Each chunk is fetched with a separate thread-pool hop so the event loop
    # only advances after Uvicorn has consumed the previous chunk — proper
    # demand-driven backpressure without an unbounded in-memory queue.
    async def _wsgi_backed_stream():
        gen = await anyio.to_thread.run_sync(
            lambda: _build_sync_generator(file, offset, end_byte, content_disposition)
        )
        try:
            while True:
                chunk = await anyio.to_thread.run_sync(lambda: next(gen, None))
                if chunk is None:
                    break
                yield chunk
        finally:
            await anyio.to_thread.run_sync(
                lambda: _fire_complete_event(file, offset, end_byte)
            )

    base_headers: dict[str, str] = {
        "Accept-Ranges": "bytes",
        "Content-Disposition": f'{content_disposition}; filename="{filename}"',
    }
    if is_partial and end_byte:
        effective_end = min(end_byte, file_size)
        base_headers["Content-Range"] = (
            f"bytes {offset}-{effective_end - 1}/{file_size}"
        )
        base_headers["Content-Length"] = str(effective_end - offset)

    return StreamingResponse(
        _wsgi_backed_stream(),
        status_code=206 if is_partial else 200,
        media_type=mime_type,
        headers=base_headers,
    )


def _parse_range(range_header: str | None, file_size: int) -> tuple[int, int | None]:
    """Return (offset, end_byte) where end_byte is *exclusive* (like Girder's endByte)."""
    if not range_header:
        return 0, None
    try:
        spec = range_header.split("=", 1)[1]
        start_str, end_str = spec.split("-", 1)
        start = int(start_str) if start_str.strip() else 0
        # HTTP Range end is *inclusive*; Girder endByte is *exclusive*
        end = (int(end_str) + 1) if end_str.strip() else file_size
        return start, end
    except (IndexError, ValueError):
        return 0, None


# ---------------------------------------------------------------------------
# Route handlers (thin wrappers so the route table stays readable)
# ---------------------------------------------------------------------------


async def file_download(request):
    return await _handle_download(request, request.path_params["file_id"])


async def file_download_with_name(request):
    # The trailing name segment is cosmetic (wget-style); ignore it.
    return await _handle_download(request, request.path_params["file_id"])


# Registered in asgi.py **before** the WSGIMiddleware Mount so these routes
# take precedence over the WSGI catch-all.
async_file_routes = [
    Route("/api/v1/file/{file_id}/download", file_download, methods=["GET"]),
    Route(
        "/api/v1/file/{file_id}/download/{name:path}",
        file_download_with_name,
        methods=["GET"],
    ),
]
