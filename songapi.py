#!/usr/bin/env python3
"""
songapi — fast song resolver API for the Daily Tune bots.

Deploy (phone /root/RobinChan):
    python3 -u songapi.py   (under api_watchdog.sh)

Endpoints (binds 127.0.0.1 only):
    GET  /v1/healthz               -> {ok, yt}
    POST /v1/resolve               -> full resolve
        body: {"query": "de dan ghera"}   or  {"url": "https://youtube.com/watch?v=..."}
    GET  /v1/resolve?q=...         -> same, via query string
    GET  /v1/stream/<vidid>        -> just the source resolve (stream_url + source)

Auth: Authorization: Bearer $SONG_API_TOKEN  (401 otherwise)
"""

import asyncio
import hmac
import json
import logging
import os
import re
import sys

sys.path.insert(0, "/root/RobinChan")

import robin.music.config as config
from aiohttp import web

API_TOKEN = os.getenv("SONG_API_TOKEN", "").strip()
BIND = os.getenv("SONG_API_BIND", "127.0.0.1")
PORT = int(os.getenv("PORT") or os.getenv("SONG_API_PORT", "8765"))  # Render injects PORT

logging.basicConfig(level=logging.INFO, format="%(asctime)s [songapi] %(message)s")
log = logging.getLogger("songapi")

if not API_TOKEN:
    log.error("SONG_API_TOKEN is empty — refusing to start unauthenticated")
    sys.exit(1)

from robin.music.platforms import YouTube  # noqa: E402

# Bound concurrent resolves so a burst of API calls can't starve the machine
_RESOLVE_SEM = asyncio.Semaphore(int(os.getenv("SONG_API_WORKERS", "4")))
YT_LINK = "https://www.youtube.com/watch?v={}"
VIDEO_RE = re.compile(r"(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/)([a-zA-Z0-9_-]{11})")


def _authorized(request: web.Request) -> bool:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False
    return hmac.compare_digest(auth[7:], API_TOKEN)


@web.middleware
async def auth(request: web.Request, handler):
    if request.path == "/v1/healthz":
        return await handler(request)
    if not _authorized(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    return await handler(request)


def _ok(data: dict) -> web.Response:
    return web.json_response({"ok": True, **data})


def _err(status: int, code: str, error: str) -> web.Response:
    return web.json_response({"ok": False, "error": error, "code": code}, status=status)


def _split_artist(title: str) -> str:
    """Minimal artist split for display metadata (avoids the PTB dependency)."""
    t = re.sub(r"\s*[\(\[{].*?[\)\]}]\s*", " ", title)
    t = re.sub(r"\s*[|–—].*$", "", t).strip()
    parts = re.split(r"\s*[-:\u2013\u2014]\s*", t, maxsplit=1)
    if len(parts) == 2:
        left, right = parts
        # Prefer "Artist - Song" when the first half looks like a name
        if left and len(left.split()) <= 5:
            return left.strip()
        return right.strip()
    return ""


async def healthz(request: web.Request) -> web.Response:
    try:
        yt_ok = await asyncio.wait_for(_yt_probe(), timeout=5)
    except Exception:
        yt_ok = False
    return web.json_response({"ok": True, "service": "songapi", "yt": yt_ok})


async def _yt_probe():
    await YouTube.initialize()
    return True


async def _resolve_vidid(body: dict, query_params=None):
    """Returns (vidid, search_result_dict_or_None, err_response_or_None)."""
    query = (body or {}).get("query") or (query_params.get("q") if query_params else None)
    url = (body or {}).get("url") or (query_params.get("url") if query_params else None)
    if query:
        m = VIDEO_RE.search(query)
        if m:
            return m.group(1), None, None
        search = await asyncio.wait_for(YouTube.search(query), timeout=config.SEARCH_TIMEOUT)
        if not search:
            return None, None, _err(404, "NOT_FOUND", "No results found.")
        return search.get("vidid"), search, None
    if url:
        m = VIDEO_RE.search(url)
        if not m:
            return None, None, _err(400, "BAD_URL", "Not a YouTube URL.")
        return m.group(1), None, None
    return None, None, _err(400, "NO_QUERY", "Provide 'query' or 'url'.")


async def resolve(request: web.Request) -> web.Response:
    if request.method == "GET":
        body, qp = {}, request.query
    else:
        try:
            body = await request.json()
        except Exception:
            return _err(400, "BAD_JSON", "Invalid JSON body.")
        qp = None

    async with _RESOLVE_SEM:
        vidid, search, err = await _resolve_vidid(body, qp)
    if err:
        return err

    link = YT_LINK.format(vidid)
    try:
        stream_url = await asyncio.wait_for(
            YouTube.get_stream_url(link), timeout=config.STREAM_URL_TIMEOUT
        )
    except Exception as e:
        log.warning("resolve failed vid=%s: %s", vidid, e)
        return _err(502, "RESOLVE_FAILED", "Could not resolve a stream URL.")

    if not stream_url:
        return _err(502, "RESOLVE_FAILED", "Could not resolve a stream URL.")

    if search:
        title = search.get("title") or ""
        artist = _split_artist(title)
        meta = {
            "title": title,
            "duration": search.get("duration") or "0:00",
            "artist": artist,
            "thumbnail_url": search.get("thumbnail_url") or f"https://i.ytimg.com/vi/{vidid}/hqdefault.jpg",
        }
    else:
        meta = {
            "title": getattr(YouTube, "_info_cache", {}).get(vidid, {}).get("title") or "YouTube Video",
            "duration": "0:00",
            "artist": "",
            "thumbnail_url": f"https://i.ytimg.com/vi/{vidid}/hqdefault.jpg",
        }

    source = YouTube.classify_stream_source(stream_url)
    source = sorted(source)[0] if source else ("cache" if not stream_url.startswith("http") else "unknown")

    return _ok({
        "vidid": vidid,
        **meta,
        "stream_url": stream_url,
        "source": source,
        "cached": True if source in ("cache", "local", "storage") else False,
    })


async def stream(request: web.Request) -> web.Response:
    vidid = request.match_info["vidid"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", vidid):
        return _err(400, "BAD_VIDID", "Invalid video id.")
    link = YT_LINK.format(vidid)
    async with _RESOLVE_SEM:
        try:
            stream_url = await asyncio.wait_for(
                YouTube.get_stream_url(link), timeout=config.STREAM_URL_TIMEOUT
            )
        except Exception as e:
            log.warning("stream resolve failed vid=%s: %s", vidid, e)
            return _err(502, "RESOLVE_FAILED", "Could not resolve the stream URL.")
    if not stream_url:
        return _err(502, "RESOLVE_FAILED", "Could not resolve the stream URL.")
    source = YouTube.classify_stream_source(stream_url)
    source = sorted(source)[0] if source else ("cache" if not stream_url.startswith("http") else "unknown")
    return _ok({"vidid": vidid, "stream_url": stream_url, "source": source})


async def start_backgrounds(app: web.Application):
    # Eager-initialize YouTube (caches from disk) so the first call isn't cold
    asyncio.create_task(_yt_probe())


def make_app() -> web.Application:
    app = web.Application(middlewares=[auth])
    app.router.add_get("/v1/healthz", healthz)
    app.router.add_route("*", "/v1/resolve", resolve)
    app.router.add_get("/v1/stream/{vidid}", stream)
    app.on_startup.append(lambda app_: start_backgrounds(app_))
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host=BIND, port=PORT, access_log=None)
