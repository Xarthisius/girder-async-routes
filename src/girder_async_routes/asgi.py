import logging
from contextlib import asynccontextmanager

from girder.notification import UserNotificationsSocket
from girder.wsgi import app as wsgi_app
from starlette.applications import Starlette
from starlette.middleware.wsgi import WSGIMiddleware
from starlette.routing import Mount, WebSocketRoute

from girder_async_routes import async_file_routes


@asynccontextmanager
async def lifespan(app):
    logger = logging.getLogger(__name__)
    logger.info("Girder server running")
    yield


# Route order matters: specific ASGI routes are evaluated before the
# WSGIMiddleware catch-all Mount.  File download routes must come first
# so they are never forwarded into the unbounded-buffering WSGI path.
app = Starlette(
    lifespan=lifespan,
    routes=[
        WebSocketRoute("/notifications/me", UserNotificationsSocket),
        *async_file_routes,
        Mount("/", app=WSGIMiddleware(wsgi_app)),
    ],
)
