# Girder Async Routes

## Problem description

When running Girder with `gunicorn girder.asgi:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8080`, large file downloads experience producer-consumer rate mismatch issues. This occurs because:

1. **Architecture Flow**: File downloads → CherryPy (WSGI) → WSGIMiddleware → Starlette (ASGI) → Uvicorn
2. **Sync Generator Issue**: The synchronous generator in `filesystem_assetstore_adapter.py` doesn't properly communicate backpressure to the async framework
3. **Buffering Problems**: WSGIMiddleware may buffer entire responses in memory, causing:
   - High memory usage for large files
   - Slow downloads
   - Potential timeouts
   - Worker hangs

This plugin adds support for asynchronous routes in Girder to circumvent these issues and improve performance for large file downloads.

## Usage

```
gunicorn girder_async_routes.asgi:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8080
```
