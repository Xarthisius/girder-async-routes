"""Async-native bulk resource download route."""

from __future__ import annotations

import json

import anyio
from girder.constants import AccessType
from girder.exceptions import AccessException
from girder.utility import ziputil
from girder.utility.model_importer import ModelImporter
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


def _resolve_resource_download(
    resources_json: str | None,
    include_metadata: bool,
    token_str: str | None,
):
    """Parse the ``resources`` JSON param, authenticate, ACL-check every document,
    and return the list of (model, doc) pairs to zip.  Runs inside a thread pool.
    """
    if not resources_json:
        return {"status_code": 400, "message": "No resources specified."}

    try:
        resources = json.loads(resources_json)
        if not isinstance(resources, dict):
            raise ValueError
    except (ValueError, TypeError):
        return {"status_code": 400, "message": "Invalid resources format."}

    total = sum(len(v) for v in resources.values())
    if total == 0:
        return {"status_code": 400, "message": "No resources specified."}

    user, _ = _authenticate(token_str)

    resolved = []  # list of (model, doc)
    for kind, ids in resources.items():
        try:
            model = ModelImporter.model(kind)
        except Exception:
            model = None
        if not model or not hasattr(model, "fileList"):
            return {"status_code": 400, "message": f"Invalid resource type: {kind}"}
        for resource_id in ids:
            try:
                doc = model.load(id=resource_id, user=user, level=AccessType.READ)
            except AccessException:
                return {"status_code": 403}
            if not doc:
                return {"status_code": 404}
            resolved.append((model, doc))

    return {
        "status_code": 200,
        "resolved": resolved,
        "user": user,
        "include_metadata": include_metadata,
    }


def _build_resources_zip_gen(resolved, user, include_metadata: bool):
    """Return a sync generator that produces zip bytes for a set of mixed resources."""
    z = ziputil.ZipGenerator()

    def stream():
        for model, doc in resolved:
            for path, file_gen in model.fileList(
                doc=doc, user=user, includeMetadata=include_metadata, subpath=True,
            ):
                yield from z.addFile(file_gen, path)
        yield z.footer()

    return stream()


# ---------------------------------------------------------------------------
# Async handler
# ---------------------------------------------------------------------------


async def _handle_resource_download(request) -> Response:
    """Handle GET /api/v1/resource/download (and POST variant)."""
    token_str = _get_token(request)

    if request.method == "POST":
        from urllib.parse import parse_qs

        raw = await request.body()
        parsed = parse_qs(raw.decode("utf-8", errors="replace"), keep_blank_values=True)
        resources_json = (parsed.get("resources") or [None])[0]
        include_metadata = (parsed.get("includeMetadata") or ["false"])[0].lower() in (
            "true",
            "1",
        )
    else:
        resources_json = request.query_params.get("resources")
        include_metadata = request.query_params.get(
            "includeMetadata", "false",
        ).lower() in (
            "true",
            "1",
        )

    info = await anyio.to_thread.run_sync(
        lambda: _resolve_resource_download(resources_json, include_metadata, token_str),
    )
    status = info.get("status_code", 500)
    if status == 400:
        return _json_error(400, info.get("message", "Bad request."))
    if status == 403:
        return _json_error(403, "Access denied.")
    if status == 404:
        return _json_error(404, "Resource not found.")

    resolved = info["resolved"]
    user = info["user"]
    gen = await anyio.to_thread.run_sync(
        lambda: _build_resources_zip_gen(resolved, user, include_metadata),
    )
    return StreamingResponse(
        _demand_driven_zip_stream(gen),
        status_code=200,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="Resources.zip"'},
    )


# ---------------------------------------------------------------------------
# Route handler
# ---------------------------------------------------------------------------


@_log_access
async def resource_download(request):
    return await _handle_resource_download(request)


resource_routes = [
    Route("/api/v1/resource/download", resource_download, methods=["GET", "POST"]),
]
