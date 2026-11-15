"""
songapi — the shared song-resolver API for the Daily Tune bots.

Runs INSIDE the bot process (started once at music module init) so it can
share the same Youtube instance, disk caches (search / stream-URL / info),
and executor — the fastest correct way to serve the serverless side later.

Endpoints (default binds 127.0.0.1 only):
    GET  /v1/healthz              -> {"ok": true, "service": "songapi", "yt": bool}
    POST /v1/resolve               -> body {"query": "..."} | {"url": "..."}
                                       GET /v1/resolve?q=... | ?url=...
    GET  /v1/stream/<vidid>        -> resolve just the stream URL

Auth: Authorization: Bearer $SONG_API_TOKEN  (401 otherwise)
"""

import asyncio
import hmac
import logging
import os
import re

from aiohttp import web

import robin.music.config as config

log = logging.getLogger("robin.music.api_server")

API_TOKEN = os.getenv("SONG_API_TOKEN", "").strip()
BIND = os.getenv("SONG_API_BIND", "127.0.0.1")
PORT = int(os.getenv("SONG_API_PORT", "8765"))

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


def _classify(stream_url: str):
    from robin.music.platforms import YouTube
    src = YouTube.classify_stream_source(stream_url)
    label = sorted(src)[0] if src else ("cache" if not stream_url.startswith("http") else "unknown")
    return label


async def healthz(request: web.Request) -> web.Response:
    try:
        from robin.music.platforms import YouTube
        await YouTube.initialize()
        yt_ok = True
    except Exception:
        yt_ok = False
    return web.json_response({"ok": True, "service": "songapi", "yt": yt_ok})


async def resolve(request: web.Request) -> web.Response:
    from robin.music.platforms import YouTube
    from robin.music.commands.autoplay import _extract_artist_and_song

    if request.method == "GET":
        body, qp = {}, request.query
    else:
        try:
            body = await request.json()
        except Exception:
            return _err(400, "BAD_JSON", "Invalid JSON body.")
        qp = None

    query = (body or {}).get("query") or (qp.get("q") if qp else None)
    url = (body or {}).get("url") or (qp.get("url") if qp else None)

    async with _RESOLVE_SEM:
        search = None
        if query:
            m = VIDEO_RE.search(query)
            if m:
                vidid = m.group(1)
            else:
                try:
                    search = await asyncio.wait_for(YouTube.search(query), timeout=config.SEARCH_TIMEOUT)
                except Exception as e:
                    log.warning("songapi search failed for %r: %s", query, e)
                    search = None
                vidid = search.get("vidid") if isinstance(search, dict) else None
                if not vidid:
                    return _err(404, "NOT_FOUND", "No results found.")
        elif url:
            m = VIDEO_RE.search(url)
            if not m:
                return _err(400, "BAD_URL", "Not a YouTube URL.")
            vidid = m.group(1)
        else:
            return _err(400, "NO_QUERY", "Provide 'query' or 'url'.")

        link = YT_LINK.format(vidid)
        try:
            stream_url = await asyncio.wait_for(
                YouTube.get_stream_url(link, skip_api=True), timeout=config.STREAM_URL_TIMEOUT
            )
        except Exception as e:
            log.warning("songapi resolve failed vid=%s: %s", vidid, e)
            return _err(502, "RESOLVE_FAILED", "Could not resolve a stream URL.")

    if not stream_url:
        return _err(502, "RESOLVE_FAILED", "Could not resolve a stream URL.")

    if search:
        title = search.get("title") or ""
        artist, _ = _extract_artist_and_song(title)
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

    source = _classify(stream_url)
    return _ok({
        "vidid": vidid,
        **meta,
        "stream_url": stream_url,
        "source": source,
        "cached": source in ("cache", "local", "storage"),
    })


async def stream(request: web.Request) -> web.Response:
    from robin.music.platforms import YouTube

    vidid = request.match_info["vidid"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", vidid):
        return _err(400, "BAD_VIDID", "Invalid video id.")
    link = YT_LINK.format(vidid)
    async with _RESOLVE_SEM:
        try:
            stream_url = await asyncio.wait_for(
                YouTube.get_stream_url(link, skip_api=True), timeout=config.STREAM_URL_TIMEOUT
            )
        except Exception as e:
            log.warning("songapi stream resolve failed vid=%s: %s", vidid, e)
            return _err(502, "RESOLVE_FAILED", "Could not resolve the stream URL.")
    if not stream_url:
        return _err(502, "RESOLVE_FAILED", "Could not resolve the stream URL.")
    source = _classify(stream_url)
    return _ok({"vidid": vidid, "stream_url": stream_url, "source": source})


def make_app() -> web.Application:
    app = web.Application(middlewares=[auth])
    app.router.add_get("/v1/healthz", healthz)
    app.router.add_route("*", "/v1/resolve", resolve)
    app.router.add_get("/v1/stream/{vidid}", stream)
    return app


async def start_song_api():
    if not API_TOKEN:
        log.warning("SONG_API_TOKEN unset — songapi disabled (set it in .env)")
        return None
    app = make_app()
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, BIND, PORT)
        await site.start()
    except OSError as e:
        log.warning("songapi could not bind %s:%d (%s) — disabled", BIND, PORT, e)
        try:
            await runner.cleanup()
        except Exception:
            pass
        return None
    log.info("songapi serving on http://%s:%d (binds either via SONG_API_BIND)", BIND, PORT)
    return runner
