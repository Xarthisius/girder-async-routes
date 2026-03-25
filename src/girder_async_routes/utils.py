"""Shared utilities: auth helpers, response helpers, logging, and streaming."""

from __future__ import annotations

import datetime
import functools
import json
import logging

import anyio
from girder.constants import TokenScope
from girder.models.token import Token
from girder.models.user import User
from starlette.responses import Response

logger = logging.getLogger(__name__)
# Use the same logger namespace as CherryPy so existing log config applies.
access_logger = logging.getLogger("cherrypy.access")

BUF_SIZE = 65536  # 64 KiB — matches filesystem_assetstore_adapter


# ---------------------------------------------------------------------------
# Access-log decorator  (matches CherryPy Combined Log Format)
# ---------------------------------------------------------------------------


def _log_access(handler):
    """Decorator that emits a CherryPy-compatible Combined Log Format entry after
    each request completes, e.g.:

        127.0.0.1 - - [25/Mar/2026:08:11:41] "GET /api/v1/file/.../download HTTP/1.1" 200 - "-" "curl/8.x"

    For streaming responses the ``Content-Length`` header value is used when
    present; otherwise ``-`` (unknown) is logged, matching CherryPy's own
    behaviour for chunked/streaming replies.
    """

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
# Shared request helpers
# ---------------------------------------------------------------------------


def _get_token(request) -> str | None:
    """Extract a Girder token from query params, header, or cookie."""
    return (
        request.query_params.get("token")
        or request.headers.get("Girder-Token")
        or request.cookies.get("girderToken")
    )


def _json_error(status_code: int, message: str) -> Response:
    """Return a JSON error response with a standard ``{"message": ...}`` body."""
    return Response(
        json.dumps({"message": message}),
        status_code=status_code,
        media_type="application/json",
    )


# ---------------------------------------------------------------------------
# Blocking auth helper – called via anyio.to_thread.run_sync
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


# ---------------------------------------------------------------------------
# Demand-driven async zip streaming
# ---------------------------------------------------------------------------


async def _demand_driven_zip_stream(gen):
    """Iterate a sync zip generator one chunk per event-loop tick via the thread pool.
    Each ``next()`` call runs in a worker thread; the event loop only advances to
    the next chunk after Uvicorn has handed the previous one to the OS send-buffer.
    """
    while True:
        chunk = await anyio.to_thread.run_sync(lambda: next(gen, None))
        if chunk is None:
            break
        yield chunk
