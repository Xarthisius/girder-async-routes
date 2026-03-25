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
import json
import logging
import os

import anyio
from starlette.responses import (
    RedirectResponse,
    Response,
    StreamingResponse,
)
from starlette.routing import Route

from girder import events
from girder.constants import AccessType, TokenScope
from girder.exceptions import AccessException

# Resolve to a local path for filesystem assetstores
from girder.models.assetstore import Assetstore
from girder.models.file import File as FileModel
from girder.models.folder import Folder as FolderModel
from girder.models.item import Item as ItemModel
from girder.models.token import Token
from girder.models.user import User
from girder.utility import ziputil
from girder.utility.assetstore_utilities import getAssetstoreAdapter
from girder.utility.filesystem_assetstore_adapter import FilesystemAssetstoreAdapter

logger = logging.getLogger(__name__)
# Use the same logger namespace as CherryPy so existing log config applies.
access_logger = logging.getLogger("cherrypy.access")

BUF_SIZE = 65536  # 64 KiB — matches filesystem_assetstore_adapter


# ---------------------------------------------------------------------------
# Access-log decorator  (matches CherryPy Combined Log Format)
# ---------------------------------------------------------------------------


def _log_access(handler):
    """
    Decorator that emits a CherryPy-compatible Combined Log Format entry after
    each request completes, e.g.:

        127.0.0.1 - - [25/Mar/2026:08:11:41] "GET /api/v1/file/.../download HTTP/1.1" 200 - "-" "curl/8.x"

    For streaming responses the ``Content-Length`` header value is used when
    present; otherwise ``-`` (unknown) is logged, matching CherryPy's own
    behaviour for chunked/streaming replies.
    """
    import functools

    @functools.wraps(handler)
    async def wrapper(request):
        response = await handler(request)

        remote = request.client.host if request.client else "-"
        now = datetime.datetime.now().strftime("%d/%b/%Y:%H:%M:%S")
        method = request.method
        path = request.url.path
        qs = request.url.query
        full_path = f"{path}?{qs}" if qs else path
        version = request.scope.get("http_version", "1.1")
        status = response.status_code
        size = response.headers.get("content-length", "-")
        referer = request.headers.get("referer", "") or "-"
        ua = request.headers.get("user-agent", "") or "-"

        access_logger.info(
            '%s - - [%s] "%s %s HTTP/%s" %s %s "%s" "%s"',
            remote,
            now,
            method,
            full_path,
            version,
            status,
            size,
            referer,
            ua,
        )
        return response

    return wrapper


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
# Item download helpers
# ---------------------------------------------------------------------------


def _resolve_item(item_id: str, token_str: str | None, format_param: str | None):
    """
    Authenticate, ACL-check, and decide whether this is a single-file pass-through
    or a multi-file zip.  Runs in a thread pool.
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
# Folder download helpers
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
            folder, user=user, subpath=False, mimeFilter=mime_filter
        ):
            yield from z.addFile(file_gen, path)
        yield z.footer()

    return stream()


async def _demand_driven_zip_stream(gen):
    """
    Iterate a sync zip generator one chunk per event-loop tick via the thread pool.
    Each ``next()`` call runs in a worker thread; the event loop only advances to
    the next chunk after Uvicorn has handed the previous one to the OS send-buffer.
    """
    while True:
        chunk = await anyio.to_thread.run_sync(lambda: next(gen, None))
        if chunk is None:
            break
        yield chunk


# ---------------------------------------------------------------------------
# Item / folder async handlers
# ---------------------------------------------------------------------------


async def _handle_item_download(request, item_id: str) -> Response:
    token_str = (
        request.query_params.get("token")
        or request.headers.get("Girder-Token")
        or request.cookies.get("girderToken")
    )
    format_param = request.query_params.get("format", "")

    info = await anyio.to_thread.run_sync(
        lambda: _resolve_item(item_id, token_str, format_param)
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
            '{"message": "Item not found."}',
            status_code=404,
            media_type="application/json",
        )

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


async def _handle_folder_download(request, folder_id: str) -> Response:

    token_str = (
        request.query_params.get("token")
        or request.headers.get("Girder-Token")
        or request.cookies.get("girderToken")
    )
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
        return Response(
            '{"message": "Access denied."}',
            status_code=403,
            media_type="application/json",
        )
    if status == 404:
        return Response(
            '{"message": "Folder not found."}',
            status_code=404,
            media_type="application/json",
        )

    folder = info["folder"]
    user = info["user"]
    gen = await anyio.to_thread.run_sync(
        lambda: _build_folder_zip_gen(folder, user, mime_filter)
    )
    return StreamingResponse(
        _demand_driven_zip_stream(gen),
        status_code=200,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{folder["name"]}.zip"'},
    )


# ---------------------------------------------------------------------------
# Route handlers (thin wrappers so the route table stays readable)
# ---------------------------------------------------------------------------


@_log_access
async def file_download(request):
    return await _handle_download(request, request.path_params["file_id"])


@_log_access
async def file_download_with_name(request):
    # The trailing name segment is cosmetic (wget-style); ignore it.
    return await _handle_download(request, request.path_params["file_id"])


@_log_access
async def item_download(request):
    return await _handle_item_download(request, request.path_params["item_id"])


@_log_access
async def folder_download(request):
    return await _handle_folder_download(request, request.path_params["folder_id"])


# Registered in asgi.py **before** the WSGIMiddleware Mount so these routes
# take precedence over the WSGI catch-all.
async_file_routes = [
    Route("/api/v1/file/{file_id}/download", file_download, methods=["GET"]),
    Route(
        "/api/v1/file/{file_id}/download/{name:path}",
        file_download_with_name,
        methods=["GET"],
    ),
    Route("/api/v1/item/{item_id}/download", item_download, methods=["GET"]),
    Route("/api/v1/folder/{folder_id}/download", folder_download, methods=["GET"]),
]
