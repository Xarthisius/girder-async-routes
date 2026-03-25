"""
Async-native file download route.

Root cause of the buffering problem
-------------------------------------
Starlette's WSGIMiddleware uses::

    anyio.create_memory_object_stream(math.inf)

The WSGI thread pushes every chunk into this *unbounded* queue without
waiting for the client to drain it.  For large files with a slow consumer
this means the entire file content accumulates in memory inside the worker.

Fix
---
Register native ASGI ``Route`` objects for the Girder file download paths
*before* the ``Mount('/', WSGIMiddleware(...))`` catch-all in asgi.py.
Starlette tries routes in order, so these are matched first; the WSGI layer
never sees a download request.

- **Filesystem assetstore** → ``anyio.open_file`` (true async I/O, backpressure
  provided by the ``StreamingResponse`` caller awaiting each ``send()``).
- **Non-filesystem assetstore** (S3, GridFS, …) → the sync Girder generator is
  iterated *one chunk at a time* inside ``anyio.to_thread.run_sync``, converting
  the unbounded queue problem into bounded, demand-driven iteration.
- **Link files** → 302 redirect, identical to CherryPy behaviour.
"""

from __future__ import annotations

import os

import anyio
from starlette.responses import RedirectResponse, Response, StreamingResponse
from starlette.routing import Route

from girder import events
from girder.constants import AccessType
from girder.exceptions import AccessException
from girder.models.assetstore import Assetstore
from girder.models.file import File as FileModel
from girder.utility.assetstore_utilities import getAssetstoreAdapter
from girder.utility.filesystem_assetstore_adapter import FilesystemAssetstoreAdapter

from .utils import BUF_SIZE, _authenticate, _get_token, _json_error, _log_access


# ---------------------------------------------------------------------------
# Blocking helpers – called via anyio.to_thread.run_sync
# ---------------------------------------------------------------------------


def _resolve(file_id: str, token_str: str | None, offset: int, end_byte: int | None):
    """
    Authenticate, ACL-check, and resolve everything needed to stream the file.
    Returns a dict with keys: file, local_path, link_url, status_code.
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

    # Link file – caller will redirect
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
    Invoke Girder's WSGI download path and return the raw chunk iterator.
    Runs inside a thread pool.
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
# Core async handler
# ---------------------------------------------------------------------------


async def _handle_download(request, file_id: str) -> Response:
    token_str = _get_token(request)
    content_disposition = request.query_params.get("contentDisposition", "attachment")
    # Range header takes precedence over the ?offset= query param, matching CherryPy.
    offset_qp = int(request.query_params.get("offset", 0) or 0)
    range_header = request.headers.get("range")
    # Preliminary values used for the download.request telemetry event;
    # refined once file_size is known.
    prelim_offset, prelim_end = _parse_range(range_header, 0)
    if not range_header:
        prelim_offset = offset_qp

    info = await anyio.to_thread.run_sync(
        lambda: _resolve(file_id, token_str, prelim_offset, prelim_end)
    )
    status = info.get("status_code", 500)
    if status == 403:
        return _json_error(403, "Access denied.")
    if status == 404:
        return _json_error(404, "File not found.")

    file = info["file"]
    link_url = info.get("link_url")

    if link_url:
        await anyio.to_thread.run_sync(
            lambda: _fire_complete_event(file, prelim_offset, prelim_end, redirect=True)
        )
        return RedirectResponse(link_url)

    local_path = info.get("local_path")
    mime_type = file.get("mimeType") or "application/octet-stream"
    filename = file.get("name", "download")
    file_size = file.get("size", 0)

    # Final offset/end_byte now that file_size is known.
    if range_header:
        offset, end_byte = _parse_range(range_header, file_size)
        is_partial = True
    else:
        offset = offset_qp
        end_byte = None
        is_partial = False

    cd_header = f'{content_disposition}; filename="{filename}"'

    # ── Filesystem assetstore ────────────────────────────────────────────────
    if local_path:
        if is_partial:
            effective_end = min(end_byte, file_size) if end_byte else file_size
            content_length = effective_end - offset

            async def _partial_stream():
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
                _partial_stream(),
                status_code=206,
                media_type=mime_type,
                headers={
                    "Content-Range": f"bytes {offset}-{effective_end - 1}/{file_size}",
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(content_length),
                    "Content-Disposition": cd_header,
                },
            )

        async def _full_stream():
            try:
                async with await anyio.open_file(local_path, "rb") as f:
                    if offset:
                        await f.seek(offset)
                    while True:
                        chunk = await f.read(BUF_SIZE)
                        if not chunk:
                            break
                        yield chunk
            finally:
                await anyio.to_thread.run_sync(
                    lambda: _fire_complete_event(file, offset, None)
                )

        return StreamingResponse(
            _full_stream(),
            status_code=200,
            media_type=mime_type,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size - offset),
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
        "Content-Disposition": cd_header,
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


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


@_log_access
async def file_download(request):
    return await _handle_download(request, request.path_params["file_id"])


@_log_access
async def file_download_with_name(request):
    # The trailing name segment is cosmetic (wget-style); ignore it.
    return await _handle_download(request, request.path_params["file_id"])


file_routes = [
    Route("/api/v1/file/{file_id}/download", file_download, methods=["GET"]),
    Route(
        "/api/v1/file/{file_id}/download/{name:path}",
        file_download_with_name,
        methods=["GET"],
    ),
]
