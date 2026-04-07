"""Tests for BodyBufferingMiddleware.

Unit tests exercise the middleware in isolation using a lightweight in-memory
ASGI echo app so that no Girder database is required.

Integration tests verify that a real Girder file upload (POST /api/v1/file/chunk)
succeeds end-to-end when the request body is routed through the middleware,
including multi-chunk sequential uploads.
"""

import asyncio

import pytest
from girder.constants import TokenScope
from girder.models.token import Token
from starlette.applications import Starlette
from starlette.middleware.wsgi import WSGIMiddleware
from starlette.routing import Mount
from starlette.testclient import TestClient

from girder_async_routes.asgi import BodyBufferingMiddleware
from girder_async_routes.routes import async_file_routes

pytest_plugins = ["pytest_girder"]


# ---------------------------------------------------------------------------
# Unit-test helpers
# ---------------------------------------------------------------------------


def _echo_body_app():
    """Minimal ASGI app that echoes the full request body as its response body."""

    async def app(scope, receive, send):
        chunks = []
        while True:
            msg = await receive()
            chunks.append(msg.get("body", b""))
            if not msg.get("more_body", False):
                break
        body = b"".join(chunks)
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [[b"content-length", str(len(body)).encode()]],
            }
        )
        await send({"type": "http.response.body", "body": body})

    return app


def _http_scope(method="POST", path="/"):
    return {
        "type": "http",
        "asgi": {"version": "3.0"},
        "method": method,
        "path": path,
        "query_string": b"",
        "headers": [],
    }


# ---------------------------------------------------------------------------
# Unit tests – no Girder required
# ---------------------------------------------------------------------------


class TestBodyBufferingMiddlewareUnit:
    """Exercise BodyBufferingMiddleware in isolation."""

    def test_post_buffers_body(self):
        """POST body must arrive intact at the inner app."""
        content = b"hello from POST"
        client = TestClient(BodyBufferingMiddleware(_echo_body_app()))
        resp = client.post("/", content=content)
        assert resp.status_code == 200
        assert resp.content == content

    def test_put_buffers_body(self):
        """PUT is in _BODY_METHODS; body should also be buffered."""
        content = b"put-data"
        client = TestClient(BodyBufferingMiddleware(_echo_body_app()))
        resp = client.put("/", content=content)
        assert resp.status_code == 200
        assert resp.content == content

    def test_patch_buffers_body(self):
        """PATCH is in _BODY_METHODS; body should also be buffered."""
        content = b"patch-data"
        client = TestClient(BodyBufferingMiddleware(_echo_body_app()))
        resp = client.patch("/", content=content)
        assert resp.status_code == 200
        assert resp.content == content

    def test_get_bypasses_buffering(self):
        """GET is not in _BODY_METHODS; request passes straight through."""
        client = TestClient(BodyBufferingMiddleware(_echo_body_app()))
        resp = client.get("/")
        assert resp.status_code == 200
        assert resp.content == b""

    def test_delete_bypasses_buffering(self):
        """DELETE is not in _BODY_METHODS; request passes straight through."""
        client = TestClient(BodyBufferingMiddleware(_echo_body_app()))
        resp = client.delete("/")
        assert resp.status_code == 200

    def test_multiple_receive_chunks_are_concatenated(self):
        """Middleware must read all chunked ASGI receive messages and present the
        inner app with a single consolidated body message instead of forwarding
        each chunk individually.
        """
        chunk1, chunk2, chunk3 = b"aaa", b"bbb", b"ccc"
        expected = chunk1 + chunk2 + chunk3

        async def _run():
            inner_receive_call_count = 0
            inner_body = None

            async def inner(scope, receive, send):
                nonlocal inner_body, inner_receive_call_count
                calls = []
                while True:
                    inner_receive_call_count += 1
                    msg = await receive()
                    calls.append(msg)
                    if not msg.get("more_body", False):
                        break
                # The middleware must consolidate all chunks into ONE message.
                assert inner_receive_call_count == 1, (
                    f"Expected 1 consolidated receive call inside inner app, "
                    f"got {inner_receive_call_count}"
                )
                inner_body = calls[0].get("body", b"")
                await send(
                    {"type": "http.response.start", "status": 200, "headers": []}
                )
                await send({"type": "http.response.body", "body": b""})

            raw_messages = iter(
                [
                    {"type": "http.request", "body": chunk1, "more_body": True},
                    {"type": "http.request", "body": chunk2, "more_body": True},
                    {"type": "http.request", "body": chunk3, "more_body": False},
                ]
            )

            async def raw_receive():
                return next(raw_messages)

            sent = []

            async def mock_send(msg):
                sent.append(msg)

            await BodyBufferingMiddleware(inner)(
                _http_scope("POST"), raw_receive, mock_send
            )
            return inner_body

        result = asyncio.run(_run())
        assert result == expected

    def test_inner_receive_returns_disconnect_after_body(self):
        """After the buffered body has been consumed by the inner app, a subsequent
        call to receive() must return an ``http.disconnect`` message rather than
        hanging or raising.
        """
        messages_seen = []

        async def _run():
            async def inner(scope, receive, send):
                msg1 = await receive()  # buffered body
                msg2 = await receive()  # should be disconnect
                messages_seen.extend([msg1, msg2])
                await send(
                    {"type": "http.response.start", "status": 200, "headers": []}
                )
                await send({"type": "http.response.body", "body": b""})

            raw_messages = iter(
                [{"type": "http.request", "body": b"payload", "more_body": False}]
            )

            async def raw_receive():
                return next(raw_messages)

            async def mock_send(msg):
                pass

            await BodyBufferingMiddleware(inner)(
                _http_scope("POST"), raw_receive, mock_send
            )

        asyncio.run(_run())
        assert messages_seen[0] == {
            "type": "http.request",
            "body": b"payload",
            "more_body": False,
        }
        assert messages_seen[1] == {"type": "http.disconnect"}

    def test_non_http_scope_passed_through_unchanged(self):
        """Non-HTTP scopes (e.g. lifespan) must be forwarded as-is without any
        body-reading or modification.
        """
        seen_scope = []

        async def _run():
            async def inner(scope, receive, send):
                seen_scope.append(scope.copy())
                msg = await receive()
                if msg["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})

            lifespan_messages = iter([{"type": "lifespan.startup"}])

            async def raw_receive():
                return next(lifespan_messages)

            sent = []

            async def mock_send(msg):
                sent.append(msg)

            await BodyBufferingMiddleware(inner)(
                {"type": "lifespan"}, raw_receive, mock_send
            )

        asyncio.run(_run())
        assert seen_scope[0]["type"] == "lifespan"


# ---------------------------------------------------------------------------
# Integration fixtures – full Girder WSGI app behind the middleware
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def buffered_app():
    """Starlette app with BodyBufferingMiddleware applied to the Girder WSGI mount."""
    from girder.wsgi import app as wsgi_app

    return Starlette(
        routes=[
            *async_file_routes,
            Mount("/", app=BodyBufferingMiddleware(WSGIMiddleware(wsgi_app))),
        ]
    )


@pytest.fixture(scope="module")
def buffered_client(buffered_app):
    with TestClient(buffered_app, raise_server_exceptions=True) as client:
        yield client


@pytest.fixture
def write_token(admin):
    """Girder token with DATA_READ + DATA_WRITE scopes for the admin user."""
    return Token().createToken(
        admin, scope=[TokenScope.DATA_READ, TokenScope.DATA_WRITE]
    )


@pytest.fixture
def small_min_chunk_size(db):
    """Temporarily lower Girder's upload minimum chunk size to 1 KB so that
    multi-chunk integration tests can use small payloads without triggering the
    5 MB default minimum.
    """
    from girder.models.setting import Setting
    from girder.settings import SettingKey

    original = Setting().get(SettingKey.UPLOAD_MINIMUM_CHUNK_SIZE)
    Setting().set(SettingKey.UPLOAD_MINIMUM_CHUNK_SIZE, 1024)
    yield
    Setting().set(SettingKey.UPLOAD_MINIMUM_CHUNK_SIZE, original)


def _auth_headers(token):
    return {"Girder-Token": str(token["_id"])}


# ---------------------------------------------------------------------------
# Integration tests – POST /api/v1/file/chunk through BodyBufferingMiddleware
# ---------------------------------------------------------------------------


class TestFileChunkUploadViaMiddleware:
    """Verify that Girder file uploads work end-to-end through BodyBufferingMiddleware."""

    def test_small_file_upload(
        self, server, buffered_client, admin, fsAssetstore, write_token, public_folder
    ):
        """Upload a small file in a single POST /file/chunk request."""
        content = b"small file body buffering test"
        name = "buf_small.txt"

        init = buffered_client.post(
            "/api/v1/file",
            headers=_auth_headers(write_token),
            params={
                "parentType": "folder",
                "parentId": str(public_folder["_id"]),
                "name": name,
                "size": len(content),
            },
        )
        assert init.status_code == 200, init.text
        upload_id = init.json()["_id"]

        resp = buffered_client.post(
            "/api/v1/file/chunk",
            headers={
                **_auth_headers(write_token),
                "Content-Type": "application/octet-stream",
            },
            params={"uploadId": upload_id, "offset": 0},
            content=content,
        )
        assert resp.status_code == 200, resp.text
        file_doc = resp.json()
        assert file_doc["name"] == name
        assert file_doc["size"] == len(content)

    def test_large_file_upload(
        self, server, buffered_client, admin, fsAssetstore, write_token, public_folder
    ):
        """Upload a 2 MB file through the middleware to stress-test body buffering."""
        content = b"A" * (2 * 1024 * 1024)
        name = "buf_large.bin"

        init = buffered_client.post(
            "/api/v1/file",
            headers=_auth_headers(write_token),
            params={
                "parentType": "folder",
                "parentId": str(public_folder["_id"]),
                "name": name,
                "size": len(content),
            },
        )
        assert init.status_code == 200, init.text
        upload_id = init.json()["_id"]

        resp = buffered_client.post(
            "/api/v1/file/chunk",
            headers={
                **_auth_headers(write_token),
                "Content-Type": "application/octet-stream",
            },
            params={"uploadId": upload_id, "offset": 0},
            content=content,
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["size"] == len(content)

    def test_multi_chunk_upload(
        self,
        server,
        buffered_client,
        admin,
        fsAssetstore,
        write_token,
        public_folder,
        small_min_chunk_size,
    ):
        """Upload a file via three sequential POST /file/chunk requests.

        Each chunk goes through BodyBufferingMiddleware independently.  The
        upload is only complete on the final chunk, so only the last response
        contains a fully-formed file document.
        """
        chunk_size = 64 * 1024  # 64 KB per chunk
        total_size = 3 * chunk_size
        content = b"C" * total_size
        name = "buf_multichunk.bin"

        init = buffered_client.post(
            "/api/v1/file",
            headers=_auth_headers(write_token),
            params={
                "parentType": "folder",
                "parentId": str(public_folder["_id"]),
                "name": name,
                "size": total_size,
            },
        )
        assert init.status_code == 200, init.text
        upload_id = init.json()["_id"]

        file_doc = None
        for offset in range(0, total_size, chunk_size):
            chunk = content[offset : offset + chunk_size]
            is_last = offset + chunk_size >= total_size

            resp = buffered_client.post(
                "/api/v1/file/chunk",
                headers={
                    **_auth_headers(write_token),
                    "Content-Type": "application/octet-stream",
                },
                params={"uploadId": upload_id, "offset": offset},
                content=chunk,
            )
            assert resp.status_code == 200, f"chunk at offset {offset}: {resp.text}"
            if is_last:
                file_doc = resp.json()

        assert file_doc is not None
        assert file_doc["size"] == total_size
