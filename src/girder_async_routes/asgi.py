import logging
from contextlib import asynccontextmanager

from girder.notification import UserNotificationsSocket
from girder.wsgi import app as wsgi_app
from starlette.applications import Starlette
from starlette.middleware.wsgi import WSGIMiddleware
from starlette.routing import Mount, WebSocketRoute

from girder_async_routes import async_file_routes

logger = logging.getLogger(__name__)
_BODY_METHODS = {"POST", "PUT", "PATCH"}


class BodyBufferingMiddleware:
    """
    Pre-reads the entire request body asynchronously in the event loop before
    passing the request to the synchronous WSGI layer.

    Starlette's WSGIMiddleware reads the body lazily from inside the WSGI thread,
    which requires repeated round-trips from the sync thread back into the async
    event loop (one per read() call). For large uploads this causes significant
    latency. By pre-buffering the body here, the WSGI thread reads from an
    in-memory bytes object with no async overhead.
    """

    def __init__(self, app):
        self._inner = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http" or scope.get("method") not in _BODY_METHODS:
            await self._inner(scope, receive, send)
            return

        body_chunks = []
        while True:
            message = await receive()
            body_chunks.append(message.get("body", b""))
            if not message.get("more_body", False):
                break

        body = b"".join(body_chunks)
        body_sent = False

        async def buffered_receive():
            nonlocal body_sent
            if not body_sent:
                body_sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return {"type": "http.disconnect"}

        await self._inner(scope, buffered_receive, send)


@asynccontextmanager
async def lifespan(app):
    logger.info("Girder server running")
    yield

_wsgi_middleware = WSGIMiddleware(wsgi_app)
_buffered_wsgi = BodyBufferingMiddleware(_wsgi_middleware)

# Route order matters: specific ASGI routes are evaluated before the
# WSGIMiddleware catch-all Mount.  File download routes must come first
# so they are never forwarded into the unbounded-buffering WSGI path.
app = Starlette(
    lifespan=lifespan,
    routes=[
        WebSocketRoute("/notifications/me", UserNotificationsSocket),
        *async_file_routes,
        Mount("/", app=_buffered_wsgi),
    ],
)
