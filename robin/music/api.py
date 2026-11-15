"""
Loopback client for the in-process songapi hub (http://127.0.0.1:8765).

Both bots share the same resolver. This client exists so this very bot
consumes the API — the same code path the FubukiMusic vc_worker and the
serverless bots use later.
"""

import asyncio
import hmac
import logging
import os
import re

import aiohttp

log = logging.getLogger("robin.music.api")

API_URL = os.getenv("SONG_API_URL", "").rstrip("/")
API_TOKEN = os.getenv("SONG_API_TOKEN", "").strip()
_VIDID_RE = re.compile(r"[A-Za-z0-9_-]{11}")


async def resolve_stream(vidid: str, timeout: float = 4.0) -> str | None:
    if not API_URL or not API_TOKEN:
        return None
    if not vidid or not _VIDID_RE.fullmatch(vidid):
        return None
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as sess:
            async with sess.get(
                f"{API_URL}/v1/stream/{vidid}",
                headers={"Authorization": f"Bearer {API_TOKEN}"},
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                url = data.get("stream_url") if isinstance(data, dict) else None
                return url or None
    except Exception as e:
        log.debug("songapi resolve failed for %s: %s", vidid, e)
        return None
