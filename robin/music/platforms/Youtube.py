"""
YouTube Music Platform Integration
====================================
YouTube search, metadata, and download functionality adapted from
Shakky-Music's shakky/platforms/Youtube.py.

Provides:
  - High-precision YouTube search with scoring
  - Multi-source download (Sentinel DB, SmashDB, @YouMusicRobot, yt-dlp)
  - Song caching and deduplication
  - Auto-DJ related track fetching
"""

import asyncio
import fnmatch
import hashlib
import json
import logging
import os
import random
import re
import tempfile
import time
from typing import Optional, Dict, List, Tuple, Union

import aiohttp
import yt_dlp
from youtubesearchpython.__future__ import VideosSearch

import robin.music.config as config

# ─── Yuki circuit breaker ────────────────────────────────────────────────────
# Yuki's origin goes down hard (HTTP 530 / Cloudflare error pages) for long
# stretches and takes a HEAD round-trip with it on EVERY play. When the probe
# shows a blanket origin failure, skip yuki entirely for a cooldown instead of
# paying it per request. Video-specific misses (404/410 on one video) do NOT
# trip the breaker.
_yuki_fail_until = 0.0
_YUKI_COOLDOWN = 600.0  # 10 min

logger = logging.getLogger("robin.music.youtube")

# ─── In-memory direct-URL cache ─────────────────────────────
# vid_id -> (url, monotonic fetched_at). Stream URLs stay valid for hours, so
# repeat plays skip the 0.5-2.5s race entirely. Entries whose source is
# excluded (join-failure retry) are dropped so retries re-resolve fresh, and
# entries expire after _STREAM_URL_CACHE_TTL so an expired URL is never served.
_STREAM_URL_CACHE: Dict[str, Tuple[str, float]] = {}
_STREAM_URL_CACHE_TTL = 18000.0  # 5h — googlevideo links from the android client last ~6h
_STREAM_URL_CACHE_MAX = 256
_STREAM_URL_CACHE_FILE = os.path.join("downloads", ".stream_url_cache.json")
_STREAM_URL_CACHE_LOADED = [False]

# Search-result cache persisted to disk (24h TTL) so repeat /play requests
# resolve the video ID instantly even after a bot restart (the phone's
# watchdog restarts the bot often, wiping the in-memory cache).
_SEARCH_CACHE: Dict[str, Tuple[Dict, float]] = {}
_SEARCH_CACHE_TTL = 86400.0
_SEARCH_CACHE_MAX = 512
_SEARCH_CACHE_FILE = os.path.join("downloads", ".search_cache.json")
_SEARCH_CACHE_LOADED = [False]

# Info-cache (title/duration/uploader) persisted to disk so the Now Streaming
# card survives a restart even when the stream URL is still cached.
_INFO_CACHE: Dict[str, Dict] = {}
_INFO_CACHE_MAX = 256
_INFO_CACHE_FILE = os.path.join("downloads", ".info_cache.json")
_INFO_CACHE_LOADED = [False]

# ─── Helpers ────────────────────────────────────────────────

def _seconds_to_min(seconds):
    """Convert seconds to MM:SS format."""
    try:
        seconds = int(seconds)
        m, s = divmod(seconds, 60)
        return f"{m}:{s:02d}"
    except (ValueError, TypeError):
        return "0:00"


def _time_to_seconds(time_str):
    """Convert MM:SS or HH:MM:SS to seconds."""
    try:
        parts = list(map(int, str(time_str).split(':')))
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
        elif len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
    except (ValueError, IndexError):
        pass
    return 0


class YouTubeAPI:
    """YouTube search and download integration for Daily Tune."""

    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.regex = r"(?:youtube\.com|youtu\.be)"
        self.download_folder = "downloads"
        self.cache_file = "song_cache.json"
        self.keyword_cache_file = "keyword_cache.json"
        
        self.cache = {}
        self.keyword_cache = {}
        # Disk-persisted search cache (loaded in initialize, survives restarts)
        self._search_result_cache = _SEARCH_CACHE
        self._SEARCH_CACHE_TTL = getattr(config, "SEARCH_CACHE_TTL", 86400)
        self._cleanup_task = None
        self._initialized = False
        
        # Concurrency controls
        self._download_semaphore = asyncio.Semaphore(10)
        self._search_semaphore = asyncio.Semaphore(15)
        self._inflight_downloads = {}
        self._inflight_lock = asyncio.Lock()
        
        # In-memory search result cache (avoids re-searching same song)
        # In-memory search cache now disk-backed (see _SEARCH_CACHE above)
        
        # Persistent HTTP session for fast YouTube API calls
        self._http_session: Optional[aiohttp.ClientSession] = None
        # Persistent yt-dlp instance (avoids creation overhead per search)
        self._ytdlp_instance: Optional[yt_dlp.YoutubeDL] = None
        self._ytdlp_lock = asyncio.Lock()
        # Small FIFO cache of resolved info per vid_id — lets direct-URL
        # plays show the real title/duration instead of placeholders.
        # Backed by downloads/.info_cache.json so it survives restarts.
        self._info_cache: Dict[str, dict] = {}
        
        os.makedirs(self.download_folder, exist_ok=True)

    def _get_http_session(self) -> aiohttp.ClientSession:
        """Get or create persistent HTTP session with connection pooling."""
        if self._http_session is None or self._http_session.closed:
            connector = aiohttp.TCPConnector(
                limit=30,
                limit_per_host=30,
                ttl_dns_cache=600,
                keepalive_timeout=120,
                enable_cleanup_closed=True,
            )
            timeout = aiohttp.ClientTimeout(total=5, connect=2, sock_connect=2)
            self._http_session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'application/json, text/plain, */*',
                },
            )
        return self._http_session

    def _get_ytdlp_instance(self) -> yt_dlp.YoutubeDL:
        """Get or create persistent yt-dlp instance for search (avoids creation overhead)."""
        if self._ytdlp_instance is None:
            opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,
                'noplaylist': True,
                'default_search': 'ytsearch1',
                'socket_timeout': 3,
                'cachedir': os.path.join('downloads', 'yt-dlp-cache'),
            }
            self._ytdlp_instance = yt_dlp.YoutubeDL(opts)
        return self._ytdlp_instance

    async def initialize(self):
        """Initialize the YouTube API."""
        if self._initialized:
            return True
        try:
            self.cache = self._load_cache(self.cache_file)
            self.keyword_cache = self._load_cache(self.keyword_cache_file)
            self._load_search_cache_from_disk()
            self._load_stream_cache_from_disk()
            self._load_info_cache_from_disk()
            self._cleanup_task = asyncio.create_task(self._cleanup_scheduler())
            self._initialized = True
            logger.info("YouTube API initialized")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize YouTube API: {e}")
            return False

    async def close(self):
        """Cleanup resources."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None
        if self._ytdlp_instance:
            self._ytdlp_instance = None
        self._initialized = False
        logger.info("YouTube API closed")

    # ─── Search ────────────────────────────────────────────

    async def search(self, query: str, executor=None) -> Optional[Dict]:
        """
        ULTRA-FAST YouTube search (~200-500ms).
        Strategy:
          1. Check in-memory cache (instant)
          2. Direct YouTube search API via aiohttp (~200-400ms)
          3. Fallback to persistent yt-dlp instance (~500-800ms)
        Returns dict with title, duration, vidid, thumbnail_url.
        """
        await self.initialize()
        
        # 1. Cache check (instant)
        cache_key = query.lower().strip()
        cached = self._get_cached_search(cache_key)
        if cached:
            return cached[0]
        
        # Use user query directly — appending extra terms like 'official audio'
        # or '-karaoke -lofi ...' completely breaks YouTube's fuzzy search, spelling correction,
        # and regional song matching (turning queries like 'de dan ghera' into unrelated content).
        search_query = query.strip()

        # 2. Direct YouTube search API (FASTEST: ~200-400ms)
        try:
            result_entry = await self._fast_youtube_search(search_query, user_query=query)
            if result_entry:
                result = self._parse_search_result(result_entry, query)
                if result:
                    self._set_cached_search(cache_key, result)
                    return result
        except Exception as e:
            logger.debug(f"Direct YouTube search failed: {e}")

        # 3. Fallback: persistent yt-dlp instance (~500-800ms)
        try:
            result_entry = await self._ytsearch_fallback(search_query, executor=executor)
            if result_entry:
                result = self._parse_search_result(result_entry, query)
                if result:
                    self._set_cached_search(cache_key, result)
                    return result
        except Exception as e:
            logger.debug(f"yt-dlp fallback search failed: {e}")

        return None

    def _parse_search_result(self, entry: dict, query: str) -> Optional[Dict]:
        """Parse a raw YouTube search entry into our standard format."""
        if not entry:
            return None
        dur = entry.get("duration") or 0
        # Handle duration as string (MM:SS) or int (seconds)
        if isinstance(dur, str):
            dur_str = dur
        else:
            dur_str = _seconds_to_min(dur) if dur else "0:00"
        return {
            "title": entry.get("title") or query,
            "duration": dur_str,
            "vidid": entry.get("id"),
            "thumbnail_url": entry.get("thumbnail") or config.STREAM_IMG_URL,
        }

    async def _fast_youtube_search(self, query: str, user_query: str = "") -> Optional[dict]:
        """
        Direct YouTube search API call (~200-400ms).
        Bypasses yt-dlp entirely — uses YouTube's internal search endpoint.
        """
        session = self._get_http_session()
        
        # YouTube's internal API endpoint for search suggestions
        # This returns video IDs and metadata directly as JSON
        search_url = "https://www.youtube.com/youtubei/v1/search"
        
        # Build the request payload (YouTube's internal API format)
        payload = {
            "context": {
                "client": {
                    "clientName": "WEB",
                    "clientVersion": "2.20240101.00.00",
                    "hl": "en",
                    "gl": "US",
                }
            },
            "query": query,
            "params": "EgIQAQ%3D%3D",  # Videos filter
        }
        
        try:
            async with session.post(search_url, json=payload) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                
                # Extract video results from the response
                contents = data.get("contents", {})
                two_col = contents.get("twoColumnSearchResultsRenderer", {})
                primary = two_col.get("primaryContents", {})
                section_list = primary.get("sectionListRenderer", {})
                sections = section_list.get("contents", [])
                
                for section in sections:
                    item_section = section.get("itemSectionRenderer", {})
                    items = item_section.get("contents", [])
                    
                    for item in items:
                        video_renderer = item.get("videoRenderer", {})
                        if video_renderer:
                            vid_id = video_renderer.get("videoId")
                            if vid_id:
                                title_runs = video_renderer.get("title", {}).get("runs", [])
                                title = title_runs[0].get("text", "") if title_runs else ""
                                
                                # Get duration
                                length_text = video_renderer.get("lengthText", {})
                                duration = length_text.get("simpleText", "0:00")
                                
                                # Get thumbnail
                                thumbnails = video_renderer.get("thumbnail", {}).get("thumbnails", [])
                                thumbnail = thumbnails[-1].get("url", "") if thumbnails else ""
                                
                                # Skip shorts (< 45s)
                                if duration and duration != "0:00":
                                    try:
                                        parts = duration.split(":")
                                        if len(parts) == 2:
                                            secs = int(parts[0]) * 60 + int(parts[1])
                                        elif len(parts) == 3:
                                            secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                                        else:
                                            secs = 0
                                        if 0 < secs < 45:
                                            continue
                                    except (ValueError, IndexError):
                                        pass

                                # Post-filter: reject titles with clearly unwanted keywords
                                # But skip filter if user explicitly asked for that type
                                title_lower = title.lower()
                                bad_title_words = ["karaoke", "8d audio", "lofi", "lo-fi",
                                                   "sped up", "slowed", "nightcore"]
                                user_q = user_query.lower()
                                if not any(asked in user_q for asked in bad_title_words):
                                    if any(bad in title_lower for bad in bad_title_words):
                                        logger.debug(f"Skipping bad result: {title}")
                                        continue

                                return {
                                    "id": vid_id,
                                    "title": title,
                                    "duration": duration,
                                    "thumbnail": thumbnail,
                                }
        except asyncio.TimeoutError:
            logger.debug("YouTube search API timed out")
        except Exception as e:
            logger.debug(f"YouTube search API error: {e}")
        
        return None

    async def fast_search_multi(self, query: str, limit: int = 10) -> list[dict]:
        """
        Fast multi-result search using YouTube's internal API (~200-400ms).
        Returns list of {id, title, duration, thumbnail} dicts.
        Used by inline mode for instant results.
        """
        await self.initialize()
        session = self._get_http_session()

        search_url = "https://www.youtube.com/youtubei/v1/search"
        payload = {
            "context": {
                "client": {
                    "clientName": "WEB",
                    "clientVersion": "2.20240101.00.00",
                    "hl": "en",
                    "gl": "US",
                }
            },
            "query": query,
            "params": "EgIQAQ%3D%3D",
        }

        results = []
        try:
            async with session.post(search_url, json=payload) as resp:
                if resp.status != 200:
                    return results
                data = await resp.json()

                contents = data.get("contents", {})
                two_col = contents.get("twoColumnSearchResultsRenderer", {})
                primary = two_col.get("primaryContents", {})
                section_list = primary.get("sectionListRenderer", {})
                sections = section_list.get("contents", [])

                for section in sections:
                    item_section = section.get("itemSectionRenderer", {})
                    items = item_section.get("contents", [])

                    for item in items:
                        video_renderer = item.get("videoRenderer", {})
                        if not video_renderer:
                            continue
                        vid_id = video_renderer.get("videoId")
                        if not vid_id:
                            continue

                        title_runs = video_renderer.get("title", {}).get("runs", [])
                        title = title_runs[0].get("text", "") if title_runs else ""
                        if not title:
                            continue

                        length_text = video_renderer.get("lengthText", {})
                        duration = length_text.get("simpleText", "0:00")

                        thumbnails = video_renderer.get("thumbnail", {}).get("thumbnails", [])
                        thumbnail = thumbnails[-1].get("url", "") if thumbnails else ""

                        # Skip shorts (< 45s)
                        if duration and duration != "0:00":
                            try:
                                parts = duration.split(":")
                                if len(parts) == 2:
                                    secs = int(parts[0]) * 60 + int(parts[1])
                                elif len(parts) == 3:
                                    secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                                else:
                                    secs = 0
                                if 0 < secs < 45:
                                    continue
                            except (ValueError, IndexError):
                                pass

                        results.append({
                            "id": vid_id,
                            "title": title,
                            "duration": duration,
                            "thumbnail": thumbnail,
                        })

                        if len(results) >= limit:
                            break
                    if len(results) >= limit:
                        break

        except Exception as e:
            logger.debug(f"Fast multi search error: {e}")

        return results

    async def _ytsearch_fallback(self, query: str, executor=None) -> Optional[dict]:
        """
        Fallback search using persistent yt-dlp instance (~500-800ms).
        Uses ytsearch1 for maximum speed.
        """
        def _search():
            try:
                ydl = self._get_ytdlp_instance()
                data = ydl.extract_info(f"ytsearch1:{query}", download=False)
                if data and 'entries' in data and data['entries']:
                    return data['entries'][0]
            except Exception:
                # Reset instance on error and retry once
                self._ytdlp_instance = None
                try:
                    ydl = self._get_ytdlp_instance()
                    data = ydl.extract_info(f"ytsearch1:{query}", download=False)
                    if data and 'entries' in data and data['entries']:
                        return data['entries'][0]
                except Exception:
                    pass
            return None

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(executor, _search)

    # ─── Stream URL (fast path: no download) ──────────────────────────────

    async def get_stream_url(self, link: str, executor=None,
                             exclude: Optional[set] = None,
                             url_only: bool = False,
                             video: bool = False,
                             skip_api: bool = False) -> Optional[str]:
        """
        Get a playable source for a YouTube link.

        Strategy (speed first, reliability via fallback):
          1. Direct stream URL from yt-dlp (~0.3s) — fastest, no disk usage
             If it fails at runtime, _handle_stream_end detects it and
             auto-downloads locally within 20s.
          2. Local file cache (instant)
          3. Telegram storage download (~1-2s)
          4. Full yt-dlp download (~3-5s) — always works

        Args:
            exclude: source names to skip in the direct-URL race
                ({"yuki"}, {"ytdlp", "innertube"}, ...). Used when a previous
                URL from that source was rejected by PyTgCalls.
            url_only: resolve WITHOUT downloading anything new — only the
                direct-URL race and an already-present local cache are used.
                Callers use this to re-resolve a rejected stream URL before
                falling back to a full download.
            video: resolve a VIDEO source (progressive MP4 with both tracks)
                for /vplay. Audio candidates are rejected at every step —
                handing an audio-only file to a video=True join silently
                plays with no picture.

        The _joining guard in call.py ensures StreamEnded fired during
        INTERDC retries does NOT trigger the fallback prematurely.
        """
        await self.initialize()
        vid_id = self._get_video_id(link)

        # songapi hub first (shares this process's caches). Falls back to the
        # in-process pipeline on any failure. Joined-retries skip it.
        if not skip_api and not url_only and not exclude:
            from robin.music.api import resolve_stream
            api_url = await resolve_stream(vid_id)
            if api_url:
                logger.info(f"Stream URL via songapi for {vid_id}")
                return api_url

        # 0. Local file cache (instant — played before or previously downloaded).
        #    One stat() call is faster than any I/O race, so check it first.
        if vid_id:
            local = self._check_local_file(vid_id, video=video)
            if local:
                ext = os.path.splitext(local)[1].lower()
                logger.info(f"Stream from local cache: {local}")
                return local

        # 1. Direct stream URL race + Telegram storage download, IN PARALLEL.
        #    Previously the storage step only ran after the URL race failed,
        #    so a storage hit still cost the race's full latency. Now the
        #    first valid source wins. With url_only the storage download
        #    is skipped entirely (never mint anything new).
        if vid_id:
            tasks = [asyncio.create_task(self._get_direct_stream_url(link, vid_id,
                                                                      executor=executor,
                                             exclude=exclude, video=video))]
            if not url_only:
                tasks.append(asyncio.create_task(self._storage_file(vid_id, video=video)))
            for done in asyncio.as_completed(tasks):
                try:
                    result = await done
                except Exception:
                    result = None
                if result:
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    logger.info(f"Parallel source won for {vid_id}")
                    return result

        # url_only: nothing cached — stop here instead of downloading.
        if url_only:
            logger.debug(f"url_only: no direct URL or local cache for {vid_id}")
            return None

        # 3. Full download from YouTube (slowest but always works)
        result = await self._do_download(link, link, vid_id, link, executor=executor, video=video)
        if result and os.path.exists(result):
            size = os.path.getsize(result)
            if size < 10240:
                logger.warning(f"Downloaded file too small ({size}B): {result}")
            else:
                logger.info(f"Downloaded from YouTube: {vid_id} ({size} bytes)")
                return result

        return None




    async def _storage_file(self, vid_id: str, video: bool = False):
        """
        Look up vid_id in Telegram storage and download it if present.
        Returns a local file path or None. Never raises.
        """
        try:
            from robin.music.storage import storage, STORAGE_GROUP_ID
            if not (storage._initialized and vid_id):
                return None
            stored = await storage.check_storage(vid_id)
            if not stored:
                return None
            file_id = stored.get("file_id")
            if not file_id:
                return None
            local = await storage.download_by_file_id(
                file_id,
                label=vid_id,
                message_id=stored.get("message_id"),
                chat_id=STORAGE_GROUP_ID,
            )
            if not (local and os.path.exists(local)):
                return None
            ext = os.path.splitext(local)[1].lower()
            if video:
                if ext == '.mp4':
                    logger.info(f"Stream video from Telegram storage: {vid_id}")
                    return local
                logger.info(f"Skipping audio-only {ext} from storage for video {vid_id}")
                return None
            if ext in ('.m4a', '.mp3', '.mp4', '.ogg'):
                logger.info(f"Stream from Telegram storage: {vid_id} ({ext})")
                return local
            logger.info(f"Skipping storage {ext} file for {vid_id} (causes stutter), re-downloading")
            try:
                os.remove(local)
            except Exception:
                pass
            return None
        except Exception as e:
            logger.debug(f"Storage check failed in _storage_file: {e}")
            return None

    def _get_ytdlp_stream_instance(self, use_cookies=False) -> yt_dlp.YoutubeDL:
        """Get or create persistent yt-dlp instance for stream extraction to bypass init overhead."""
        if getattr(self, '_ytdlp_stream_instances', None) is None:
            self._ytdlp_stream_instances = {}
            
        key = "cookies" if use_cookies else "nocookies"
        if key not in self._ytdlp_stream_instances:
            class _QuietLog:
                def debug(self, msg): pass
                def info(self, msg): pass
                def warning(self, msg): pass
                def error(self, msg): pass

            opts = {
                'quiet': True,
                'no_warnings': True,
                'logger': _QuietLog(),
                'noplaylist': True,
                'nocheckcertificate': True,
                'socket_timeout': 10,
                # Persistent cache dir — reuses yt-dlp's player JS / signature
                # extracts across invocations and restarts. Without this,
                # every /play pays the full JS-challenge solve again.
                'cachedir': os.path.join('downloads', 'yt-dlp-cache'),
                'http_headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': '*/*',
                },
                'extractor_args': {
                    'youtube': {
                        'player_client': ['android'],
                    }
                },
                'geo_bypass': True,
                'extract_flat': False,
                # Node 22 on PATH - REQUIRED for YouTube's JS challenges
                # (n-signature). Without it yt-dlp only gets image formats.
                'js_runtimes': {'node': {}},
            }
            if use_cookies:
                cookies_file = self._get_cookies_file()
                if cookies_file:
                    opts['cookiefile'] = cookies_file
            
            self._ytdlp_stream_instances[key] = yt_dlp.YoutubeDL(opts)
        
        return self._ytdlp_stream_instances[key]

    async def _get_direct_stream_url(self, link: str, vid_id: str, executor=None,
                                     exclude: Optional[set] = None,
                                     video: bool = False) -> Optional[str]:
        """
        Get direct stream URL for a YouTube video.

        Strategy: RACE all sources in parallel — first response wins.
          - Yuki API (fast, non-seekable): ~0.5s          [audio only]
          - yt-dlp (seekable, mp4/AAC): ~2-4s             [audio + video]

        Video mode races yt-dlp ONLY: yuki's type=video endpoint answers
        every variant (360/480/720/no-quality) with an HTML error page
        behind HTTP 200 — verified with ffprobe (invalid data). Racing it
        would win in 0.5s with an unplayable URL.

        Removed sources — they can never work with NTgCalls/FFmpeg:
          - Innertube direct-URL clients (ANDROID/IOS): YouTube answers
            ffmpeg's open-ended "Range: bytes=0-" with 403 (only bounded
            ranges pass, which NTgCalls never sends) <tg-emoji emoji-id=\"6160983904388911924\">▶️</tg-emoji> NoAudioSourceFound.
          - NexGen: no API key configured, always returned None.

        Args:
            exclude: source names to drop from the race. Used on retry when a
                URL from one source was rejected by PyTgCalls
                (e.g. NoAudioSourceFound on a Yuki stream URL).
            video: pick a progressive MP4 (video+audio muxed in one URL)
                instead of the best audio-only format.
        """
        exclude = set(exclude or ())
        loop = asyncio.get_event_loop()

        # Fast path: a recently resolved URL for this video — skip the race
        # entirely (repeat play of the same song: 0ms instead of 0.5-2.5s).
        # Not served when its source is excluded by a join-failure retry.
        # Audio and video URLs live under separate keys — an audio URL in a
        # video join plays with no picture.
        cached = self._get_cached_stream_url(vid_id, exclude, video=video)
        if cached:
            logger.info(
                f"[PERF] Stream URL cache hit for {vid_id} "
                f"({'video' if video else 'audio'} race skipped)"
            )
            return cached

        async def _try_ytdlp():
            try:
                has_cookies = bool(self._get_cookies_file())

                def _extract_url(use_cookies):
                    # Returns (url, fmt_meta). The fmt log happens in the
                    # CALLER, not here: this runs in an executor thread that
                    # survives race cancellation and used to log "yt-dlp URL"
                    # seconds after losing (zombie log noise).
                    ydl = self._get_ytdlp_stream_instance(use_cookies)
                    info = ydl.extract_info(link, download=False)
                    if info:
                        # Stash title/duration for the direct-URL Now Streaming card
                        try:
                            self._set_info_cache(vid_id, info)
                        except Exception:
                            pass
                    if not info:
                        return None, None

                    if video:
                        # /vplay: single PROGRESSIVE MP4 — video+audio muxed
                        # into one URL so FFmpeg gets both tracks from one
                        # input. Cap at 480p (group-call bandwidth); YouTube's
                        # current muxed formats are 91-96 (classic 22 is gone).
                        fmts = info.get("formats") or []
                        prog = []
                        for f in fmts:
                            if f.get("vcodec", "none") == "none":
                                continue
                            if f.get("acodec", "none") == "none":
                                continue
                            fu = f.get("url", "")
                            if not fu:
                                continue
                            if fu.endswith(".mpd") or fu.endswith(".m3u8") or "manifest" in fu:
                                continue
                            if f.get("ext") not in ("mp4", "m4v", "mov"):
                                continue
                            prog.append(f)
                        if not prog:
                            return None, None
                        capped = [f for f in prog if (f.get("height") or 0) <= 480]
                        if capped:
                            capped.sort(key=lambda f: f.get("height") or 0, reverse=True)
                            chosen = capped[0]
                        else:
                            prog.sort(key=lambda f: f.get("height") or 99999)
                            chosen = prog[0]
                        meta = (f"fmt={chosen.get('format_id')} "
                                f"{chosen.get('height')}p ext={chosen.get('ext')} (video+audio)")
                        return chosen.get("url", ""), meta

                    url = info.get('url')
                    if url and not url.endswith('.mpd') and 'manifest' not in url:
                        ext = info.get('ext', '')
                        if ext in ('m4a', 'mp3', 'aac', 'opus', 'webm', ''):
                            return url, None

                    formats = info.get("formats", [])
                    if not formats:
                        return None, None

                    audio_formats = []
                    for f in formats:
                        if f.get('acodec') == 'none': continue
                        fmt_url = f.get('url', '')
                        if not fmt_url: continue
                        if fmt_url.endswith('.mpd') or fmt_url.endswith('.m3u8') or 'manifest' in fmt_url: continue
                        audio_formats.append(f)

                    if not audio_formats:
                        return None, None

                    def _sort_key(f):
                        ext = f.get('ext', '')
                        acodec = f.get('acodec', '')
                        abr = f.get('abr') or f.get('tbr') or 0
                        if ext == 'm4a' or acodec in ('mp4a.40.2', 'mp4a.40.5', 'aac'): return 2000 + abr
                        elif ext == 'mp3': return 1000 + abr
                        else: return abr

                    audio_formats.sort(key=_sort_key, reverse=True)
                    chosen = audio_formats[0]
                    meta = f"fmt={chosen.get('format_id')} ext={chosen.get('ext')} abr={chosen.get('abr', '?')}kbps acodec={chosen.get('acodec')}"
                    return chosen.get('url', ''), meta

                stream_url, fmt_meta = await asyncio.wait_for(
                    loop.run_in_executor(executor, _extract_url, has_cookies),
                    timeout=8,
                )
                if stream_url:
                    if fmt_meta:
                        logger.info(f"yt-dlp URL: {fmt_meta}")
                    logger.info(f"yt-dlp stream URL for {vid_id} (seekable, has_cookies={has_cookies})")
                    return stream_url

                if has_cookies:
                    stream_url, fmt_meta = await asyncio.wait_for(
                        loop.run_in_executor(executor, _extract_url, False),
                        timeout=8,
                    )
                    if stream_url:
                        if fmt_meta:
                            logger.info(f"yt-dlp URL: {fmt_meta}")
                        logger.info(f"yt-dlp stream URL for {vid_id} (seekable, no cookies)")
                        return stream_url
            except asyncio.TimeoutError:
                logger.debug(f"yt-dlp timed out for {vid_id}")
            except Exception as e:
                logger.debug(f"yt-dlp extraction failed for {vid_id}: {e}")
            return None

        async def _try_yuki():
            global _yuki_fail_until
            try:
                if time.monotonic() < _yuki_fail_until:
                    logger.debug(f"Yuki circuit open — skipping for {vid_id}")
                    return None
                yuki_url = await self._yuki_get_stream_url(vid_id)
                if yuki_url:
                    logger.info(f"Yuki API stream URL for {vid_id} (fast)")
                    return yuki_url
            except Exception as e:
                logger.debug(f"Yuki API failed for {vid_id}: {e}")
            return None

        # Fire remaining sources simultaneously — first NON-NONE response wins
        import time as _time_t
        race_start = _time_t.monotonic()
        if video:
            # Yuki has no working video stream endpoint (HTML error page
            # behind HTTP 200 for every type=video variant) — race yt-dlp only.
            racers = {"ytdlp": _try_ytdlp}
        else:
            racers = {
                "yuki": _try_yuki,
                "ytdlp": _try_ytdlp,
            }
        if exclude:
            dropped = exclude & set(racers)
            if dropped:
                logger.info(f"Stream race excluding {sorted(dropped)} for {vid_id}")
            racers = {n: fn for n, fn in racers.items() if n not in exclude}
        if not racers:
            logger.warning(f"All stream sources excluded for {vid_id}")
            return None
        tasks = [asyncio.create_task(fn(), name=name) for name, fn in racers.items()]

        for fut in asyncio.as_completed(tasks):
            try:
                result = await fut
                if result:
                    race_ms = (_time_t.monotonic() - race_start) * 1000
                    # Cancel remaining tasks
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    logger.info(f"[PERF] Stream race won in {race_ms:.0f}ms for {vid_id}")
                    self._set_cached_stream_url(vid_id, result, video=video)
                    return result
            except Exception as e:
                logger.debug(f"Stream race task exception: {e}")

        logger.warning(f"All stream sources failed for {vid_id}")
        return None

    def classify_stream_source(self, url: str) -> set:
        """
        Map a resolved stream URL back to the race source(s) that produced it.

        Returns the set of racer names to EXCLUDE on a retry:
          - Yuki proxy URL   -> {"yuki"}
          - NexGen API URL   -> {"nexgen"}
          - googlevideo/yt   -> {"ytdlp", "innertube"}  (both emit the same
            CDN URLs, so excluding just one would let the other hand back the
            identical rejected URL)
          - anything else / local path -> empty set (nothing to exclude)

        Used on join failure: the caller re-runs the race minus this set so
        PyTgCalls gets a URL from a different provider, instead of falling
        straight back to a local download.
        """
        if not url or not isinstance(url, str) or not url.startswith("http"):
            return set()
        try:
            yuki_base = (config.YUKI_API_URL or "").rstrip("/")
            if yuki_base and url.startswith(yuki_base):
                return {"yuki"}
            api_base = (config.API_URL or "").rstrip("/")
            if api_base and url.startswith(api_base):
                return {"nexgen"}
        except Exception:
            pass
        if "googlevideo.com" in url or "youtube.com" in url or "ytimg.com" in url:
            return {"ytdlp", "innertube"}
        return set()

    @staticmethod
    def _stream_cache_key(vid_id: str, video: bool = False) -> str:
        """Cache key — audio and video URLs must never mix: a cached audio
        URL served to a video=True join plays with no picture."""
        return f"{vid_id}|video" if video else vid_id

    def _get_cached_stream_url(self, vid_id: str, exclude: Optional[set] = None,
                               video: bool = False) -> Optional[str]:
        """Return a recently resolved stream URL for this video, if still fresh.

        Respects the retry `exclude` set: if the cached URL came from a source
        that was just rejected (e.g. yuki returned a dead URL), drop the entry
        so the caller re-resolves fresh instead of serving the same URL again.
        """
        key = self._stream_cache_key(vid_id, video)
        self._load_stream_cache_from_disk()
        entry = _STREAM_URL_CACHE.get(key)
        if not entry:
            return None
        url, fetched_at = entry
        if time.time() - fetched_at > _STREAM_URL_CACHE_TTL:
            _STREAM_URL_CACHE.pop(key, None)
            self._persist_stream_cache_to_disk()
            return None
        if exclude and (self.classify_stream_source(url) & set(exclude)):
            _STREAM_URL_CACHE.pop(key, None)
            self._persist_stream_cache_to_disk()
            return None
        return url

    def _load_stream_cache_from_disk(self) -> None:
        if _STREAM_URL_CACHE_LOADED[0]:
            return
        _STREAM_URL_CACHE_LOADED[0] = True
        try:
            if not os.path.exists(_STREAM_URL_CACHE_FILE):
                return
            with open(_STREAM_URL_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            now = time.time()
            for key, entry in (data.get("items") or {}).items():
                url, at = entry
                if isinstance(url, str) and (now - float(at)) <= _STREAM_URL_CACHE_TTL:
                    _STREAM_URL_CACHE[key] = (url, float(at))
            logger.info(f"Loaded {len(_STREAM_URL_CACHE)} cached stream URLs from disk")
        except Exception as e:
            logger.debug(f"Stream URL disk cache load failed: {e}")

    def _persist_stream_cache_to_disk(self) -> None:
        try:
            items = [(k, list(v)) for k, v in _STREAM_URL_CACHE.items()]
            tmp = _STREAM_URL_CACHE_FILE + ".tmp"
            os.makedirs(os.path.dirname(_STREAM_URL_CACHE_FILE), exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"items": dict(items)}, f)
            os.replace(tmp, _STREAM_URL_CACHE_FILE)
        except Exception as e:
            logger.debug(f"Stream URL disk cache save failed: {e}")

    def _set_cached_stream_url(self, vid_id: str, url: str, video: bool = False) -> None:
        """Remember a stream URL that won the race (bounded, oldest evicted)."""
        if not vid_id or not url or not isinstance(url, str):
            return
        key = self._stream_cache_key(vid_id, video)
        if key in _STREAM_URL_CACHE:
            _STREAM_URL_CACHE.pop(key, None)
        elif len(_STREAM_URL_CACHE) >= _STREAM_URL_CACHE_MAX:
            _STREAM_URL_CACHE.pop(next(iter(_STREAM_URL_CACHE)), None)
        _STREAM_URL_CACHE[key] = (url, time.time())
        self._persist_stream_cache_to_disk()

    def _load_search_cache_from_disk(self) -> None:
        if _SEARCH_CACHE_LOADED[0]:
            return
        _SEARCH_CACHE_LOADED[0] = True
        try:
            if not os.path.exists(_SEARCH_CACHE_FILE):
                return
            with open(_SEARCH_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            now = time.time()
            for key, entry in (data.get("items") or {}).items():
                result, at = entry
                if isinstance(result, dict) and (now - float(at)) <= self._SEARCH_CACHE_TTL:
                    _SEARCH_CACHE[key] = (result, float(at))
            logger.info(f"Loaded {len(_SEARCH_CACHE)} cached searches from disk")
        except Exception as e:
            logger.debug(f"Search disk cache load failed: {e}")

    def _persist_search_cache_to_disk(self) -> None:
        try:
            items = [(k, [v[0], v[1]]) for k, v in _SEARCH_CACHE.items()]
            tmp = _SEARCH_CACHE_FILE + ".tmp"
            os.makedirs(os.path.dirname(_SEARCH_CACHE_FILE), exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"items": dict(items)}, f)
            os.replace(tmp, _SEARCH_CACHE_FILE)
        except Exception as e:
            logger.debug(f"Search disk cache save failed: {e}")

    def _get_cached_search(self, cache_key: str):
        """Return cached (result, ts) tuple or None; honors TTL."""
        self._load_search_cache_from_disk()
        entry = _SEARCH_CACHE.get(cache_key)
        if not entry:
            return None
        result, ts = entry
        if time.time() - ts > self._SEARCH_CACHE_TTL:
            _SEARCH_CACHE.pop(cache_key, None)
            self._persist_search_cache_to_disk()
            return None
        return entry

    def _set_cached_search(self, cache_key: str, result: Dict) -> None:
        """Remember a search result (bounded LRU, persisted to disk)."""
        if not cache_key or not isinstance(result, dict):
            return
        if cache_key in _SEARCH_CACHE:
            _SEARCH_CACHE.pop(cache_key, None)
        elif len(_SEARCH_CACHE) >= _SEARCH_CACHE_MAX:
            _SEARCH_CACHE.pop(next(iter(_SEARCH_CACHE)), None)
        _SEARCH_CACHE[cache_key] = (result, time.time())
        self._persist_search_cache_to_disk()

    def _get_cookies_file(self) -> Optional[str]:
        """Return path to cookies.txt if it exists and is non-empty."""
        bot_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        p = os.path.join(bot_root, "cookies.txt")
        return p if os.path.exists(p) and os.path.getsize(p) > 0 else None

    # ─── Download ──────────────────────────────────────────

    async def download(self, link: str, mystic=None, video=False, raw_query: str = None, title: str = None, duration: str = None, executor=None, **kwargs) -> Tuple[Optional[str], bool]:
        """
        Main download method. Returns (file_path, is_direct).
        
        Download strategy:
          1. Check Telegram storage group (instant replay)
          2. Check local cache
          3. Download from YouTube via yt-dlp
          4. Upload to Telegram storage for future plays
        """
        await self.initialize()

        try:
            vid_id = self._get_video_id(link)
            query = raw_query or link

            # 1. Check Telegram storage group (FAST: 1-3s)
            # Try vidid first, then title-based lookup
            try:
                from robin.music.storage import storage
                if storage._initialized:
                    stored = await storage.check_storage(vid_id or "", title=query)
                    if stored:
                        # Download from Telegram (much faster than YouTube)
                        actual_vidid = stored.get("vidid", vid_id)
                        local_path = await storage.download_from_storage(actual_vidid)
                        if local_path:
                            ext = os.path.splitext(local_path)[1].lower()
                            if video and ext != ".mp4":
                                # Audio-only cache cannot serve video=True
                                logger.info(f"Storage {ext} is audio-only, ignoring for video: {query}")
                            else:
                                logger.info(f"Playing from Telegram storage: {query}")
                                return local_path, True
            except Exception as e:
                logger.debug(f"Storage check failed, falling back to download: {e}")

            # 2. Local file check
            if vid_id:
                local = self._check_local_file(vid_id, video=video)
                if local:
                    logger.info(f"Local file found: {local}")
                    return local, True

            # Deduplication
            dedup_key = vid_id or query
            async with self._inflight_lock:
                if dedup_key in self._inflight_downloads:
                    existing_lock = self._inflight_downloads[dedup_key]
                else:
                    existing_lock = asyncio.Lock()
                    self._inflight_downloads[dedup_key] = existing_lock

            async with existing_lock:
                if vid_id:
                    local = self._check_local_file(vid_id, video=video)
                    if local:
                        return local, True

                # 3. Download from YouTube (SLOWER: 4-6s first time)
                result = await self._do_download(
                    link, query, vid_id, raw_query, executor=executor, video=video,
                )
                
                # 4. Upload to Telegram storage for future plays (async background task)
                # Use title/duration passed in to skip the re-search entirely
                if result and vid_id:
                    try:
                        from robin.music.storage import storage
                        if storage._initialized:
                            _bg_title = title or raw_query or link
                            _bg_dur = duration or "0:00"
                            async def _bg_upload(t=_bg_title, d=_bg_dur):
                                try:
                                    await storage.upload_to_storage(vid_id, result, t, d, 0)
                                except Exception:
                                    pass
                            asyncio.create_task(_bg_upload())
                    except Exception as e:
                        logger.debug(f"Storage upload failed (non-critical): {e}")
                
                return result, True if result else None

        except Exception as e:
            logger.error(f"Download error: {e}")
            return None, None
        finally:
            try:
                async with self._inflight_lock:
                    self._inflight_downloads.pop(dedup_key, None)
            except:
                pass

    async def _do_download(self, link, query, vid_id, raw_query, executor=None, video: bool = False):
        """Actual download under dedup lock.

        Audio priority:
          1. Yuki API (primary — fast, reliable)
          2. yt-dlp (fallback — local download)
          3. NexGen API (last resort)

        Video: yt-dlp progressive MP4 only. Yuki's type=video endpoint
        returns an HTML error page (HTTP 200, verified with ffprobe) and
        NexGen is audio-only (no API key either).
        """
        async with self._download_semaphore:
            if video:
                try:
                    result = await self._ytdlp_download(
                        link, vid_id or "unknown", executor=executor, video=True,
                    )
                    if result:
                        logger.info(f"Downloaded video via yt-dlp: {vid_id}")
                        return result
                except Exception as e:
                    logger.warning(f"yt-dlp video download failed: {e}")
                return None

            # Primary: Yuki API
            try:
                result = await self._yuki_download_audio(vid_id or "unknown")
                if result:
                    logger.info(f"Downloaded via Yuki API: {vid_id}")
                    return result
            except Exception as e:
                logger.warning(f"Yuki API download failed: {e}")

            # Fallback 1: yt-dlp
            try:
                return await self._ytdlp_download(link, vid_id or "unknown", executor=executor)
            except Exception as e:
                logger.warning(f"yt-dlp download failed: {e}")

            # Fallback 2: NexGen API
            return await self._download_fallback(link, vid_id or "unknown")

    async def _ytdlp_download(self, link: str, video_id: str, executor=None,
                              video: bool = False) -> Optional[str]:
        """Download audio via yt-dlp, converted to m4a for smooth VC playback.
        
        Optimized for weak connections:
          - Smaller chunk sizes for reliability
          - Larger read-ahead buffer
          - Retry on network errors
          - Lower audio bitrate (96k AAC) for faster download on slow links

        video=True: download a progressive MP4 (video+audio, ≤480p) with NO
        audio-extraction postprocessor — FFmpegExtractAudio would strip the
        picture and leave an m4a.
        """
        tmp_dir = tempfile.gettempdir()
        
        ydl_opts = {
            # Audio: 'bestaudio/best' always resolves — the FFmpegExtractAudio
            # postprocessor below converts to aac. Do NOT use format IDs (like
            # 140) or ext filters combined with postprocessors; yt-dlp fails to
            # validate the chain and throws "Requested format is not available".
            #
            # Video: single progressive file (both tracks in one container),
            # mp4 preferred, ≤480p for group-call bandwidth.
            'format': ('b[ext=mp4][height<=480]/b[ext=mp4]/b' if video
                       else 'bestaudio/best'),
            'outtmpl': os.path.join(tmp_dir, f"robin_{video_id}.%(ext)s"),
            'quiet': True,
            'no_warnings': True,
            'ignoreerrors': False,   # MUST be False — otherwise format errors are swallowed silently
            'concurrent_fragment_downloads': 4,  # Fewer concurrent = more reliable on weak net
            'buffersize': 2097152,  # 2MB read-ahead buffer (was 1MB)
            'http_chunk_size': 5242880,  # 5MB chunks (was 10MB — smaller = more reliable)
            'retries': 5,  # More retries for weak connections
            'fragment_retries': 5,
            'nocheckcertificate': True,
            'noplaylist': True,
            'js_runtimes': {'node': {}},
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': '*/*',
                'Accept-Encoding': 'identity',  # No compression — faster for streaming
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
            },
            'extractor_args': {
                'youtube': {
                    'player_client': ['android', 'web', 'mweb', 'ios', 'tv'],
                }
            },
            'geo_bypass': True,
        }
        if not video:
            # Force conversion to m4a — webm/opus causes VC stuttering
            # 96k AAC: smaller file = faster download on weak connections, still sounds great.
            # Skipped entirely for video: extracting audio would destroy the picture.
            ydl_opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'aac',
                'preferredquality': '96',
            }]
        
        # Use cookies if available (look in bot root directory)
        bot_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        cookies_file = os.path.join(bot_root, "cookies.txt")
        has_cookies = os.path.exists(cookies_file) and os.path.getsize(cookies_file) > 0
        if has_cookies:
            ydl_opts['cookiefile'] = cookies_file
            logger.info(f"Using cookies from {cookies_file}")

        def _download(opts):
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([link])

        loop = asyncio.get_event_loop()
        try:
            # Use dedicated executor if provided (avoids blocking default asyncio pool)
            await loop.run_in_executor(executor, _download, ydl_opts)
        except Exception as e:
            err_str = str(e)
            # Handle age verification and other errors
            if has_cookies and ("403" in err_str or "Forbidden" in err_str or "Sign in to confirm" in err_str):
                logger.warning(f"Download with cookies failed. Retrying without cookies: {e}")
                opts_without_cookies = dict(ydl_opts)
                opts_without_cookies.pop('cookiefile', None)
                try:
                    await loop.run_in_executor(executor, _download, opts_without_cookies)
                except Exception as e2:
                    logger.error(f"Download without cookies also failed: {e2}")
                    raise
            elif "Sign in to confirm" in err_str:
                # Age verification error - try with different player clients
                logger.warning(f"Age verification error, trying with tv client: {e}")
                opts_tv = dict(ydl_opts)
                opts_tv['extractor_args'] = {
                    'youtube': {
                        'player_client': ['tv', 'tv_embedded'],
                    }
                }
                if has_cookies:
                    opts_tv['cookiefile'] = cookies_file
                try:
                    await loop.run_in_executor(executor, _download, opts_tv)
                except Exception as e2:
                    logger.error(f"Download with tv client also failed: {e2}")
                    raise
            else:
                raise

        result_path = None
        # Video: mp4 first (webm only as last resort). The audio order puts
        # .m4a first, which would hand a stale audio file to a video download.
        ext_order = (["mp4", "webm"] if video
                     else ["m4a", "mp3", "mp4", "ogg", "opus", "webm"])
        for ext in ext_order:
            p = os.path.join(tmp_dir, f"robin_{video_id}.{ext}")
            if os.path.exists(p) and os.path.getsize(p) > 20480:
                result_path = p
                break
        
        if not result_path:
            raise RuntimeError(f"yt-dlp completed but no output file found for {video_id}")
        
        return result_path

    async def _download_fallback(self, link: str, video_id: str) -> Optional[str]:
        """API-based fallback download to temp file."""
        if config.API_URL and config.API_KEY:
            try:
                url = f"{config.API_URL}/song/{video_id}?api={config.API_KEY}"
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("status", "").lower() == "done":
                                dl_url = data.get("link")
                                if dl_url:
                                    ext = data.get("format", "mp3").lower()
                                    tmp_dir = tempfile.gettempdir()
                                    fp = os.path.join(tmp_dir, f"robin_{video_id}.{ext}")
                                    async with session.get(dl_url) as fresp:
                                        with open(fp, 'wb') as f:
                                            while True:
                                                chunk = await fresp.content.read(8192)
                                                if not chunk:
                                                    break
                                                f.write(chunk)
                                    if os.path.exists(fp) and os.path.getsize(fp) > 10240:
                                        logger.info(f"Downloaded via API fallback: {video_id}")
                                        return fp
                                    else:
                                        # Remove invalid file
                                        try:
                                            os.remove(fp)
                                        except:
                                            pass
            except Exception as e:
                logger.debug(f"API fallback download failed: {e}")
        return None

    # ─── Yuki API (Primary Source) ─────────────────────────

    async def _yuki_download_audio(self, video_id: str) -> Optional[str]:
        """
        Download audio via Yuki API (primary source).
        API endpoint: {YUKI_API_URL}/stream/{video_id}?key={key}&type=audio&quality=128
        """
        if not config.YUKI_API_URL or not config.YUKI_API_KEY:
            return None

        os.makedirs(self.download_folder, exist_ok=True)
        file_path = os.path.join(self.download_folder, f"{video_id}.mp3")

        # Use cached file if it exists and is valid
        if os.path.exists(file_path) and os.path.getsize(file_path) > 10000:
            logger.info(f"Yuki API: using cached audio for {video_id}")
            return file_path

        try:
            stream_url = f"{config.YUKI_API_URL}/stream/{video_id}?key={config.YUKI_API_KEY}&type=audio&quality=128"
            timeout = aiohttp.ClientTimeout(total=300)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(stream_url) as resp:
                    if resp.status != 200:
                        logger.debug(f"Yuki API audio returned status {resp.status} for {video_id}")
                        return None
                    with open(file_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(131072):
                            f.write(chunk)

            if os.path.exists(file_path) and os.path.getsize(file_path) > 10000:
                logger.info(f"Yuki API: downloaded audio for {video_id} ({os.path.getsize(file_path)} bytes)")
                return file_path
            return None
        except Exception as e:
            logger.debug(f"Yuki API audio download failed for {video_id}: {e}")
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception:
                pass
            return None

    async def _yuki_get_stream_url(self, video_id: str) -> Optional[str]:
        """
        Get a direct stream URL from Yuki API for streaming (no download needed).

        Probes with HEAD (2s cap) and validates status AND Content-Type.
        Status alone is not enough: yuki serves dead/gone streams as an
        nginx "410 Gone" HTML page that Cloudflare caches with HTTP 200 +
        Content-Type: text/html, which a status-only check accepts — NTgCalls
        then rejects the URL at join time (NoAudioSourceFound).
        """
        if not config.YUKI_API_URL or not config.YUKI_API_KEY:
            return None
        global _yuki_fail_until

        def _trip_breaker(reason: str) -> None:
            global _yuki_fail_until
            _yuki_fail_until = time.monotonic() + _YUKI_COOLDOWN
            logger.warning(f"Yuki circuit breaker TRIPPED ({reason}) — skipping for {int(_YUKI_COOLDOWN)}s")

        try:
            stream_url = f"{config.YUKI_API_URL}/stream/{video_id}?key={config.YUKI_API_KEY}&type=audio&quality=128"
            session = self._get_http_session()
            async with session.head(stream_url, timeout=aiohttp.ClientTimeout(total=2.0)) as resp:
                ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
                # An audio stream is never text/* — text/html here is the
                # cached 410 error page, so make the race fall through to the
                # next source instead of handing NTgCalls an unusable URL.
                if resp.status == 200 and ctype and not ctype.startswith("text/"):
                    logger.info(f"Yuki API: got stream URL for {video_id}")
                    return stream_url
                logger.info(
                    f"Yuki API stream rejected for {video_id}: "
                    f"HTTP {resp.status} content-type={ctype or 'none'}"
                )
                # Blanket origin failure → trip the breaker. 404/410 can be
                # video-specific (deleted/unavailable) — do not trip on those.
                if resp.status >= 500 or (ctype or "").startswith("text/"):
                    _trip_breaker(f"HTTP {resp.status} text/html" if (ctype or '').startswith('text/') else f"HTTP {resp.status}")
                return None
        except Exception as e:
            logger.debug(f"Yuki API stream URL failed for {video_id}: {e}")
            _trip_breaker(f"{type(e).__name__}: {e}")
            return None

    # ─── Utility Methods ───────────────────────────────────

    def _load_info_cache_from_disk(self) -> None:
        """Load the persisted title/duration cache for direct-URL cards."""
        if _INFO_CACHE_LOADED[0]:
            return
        _INFO_CACHE_LOADED[0] = True
        try:
            if not os.path.exists(_INFO_CACHE_FILE):
                return
            with open(_INFO_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for key, entry in (data.get("items") or {}).items():
                if isinstance(entry, dict):
                    _INFO_CACHE[key] = entry
            logger.info(f"Loaded {len(_INFO_CACHE)} cached info entries from disk")
        except Exception as e:
            logger.debug(f"Info disk cache load failed: {e}")

    def _persist_info_cache_to_disk(self) -> None:
        try:
            items = dict(_INFO_CACHE)
            tmp = _INFO_CACHE_FILE + ".tmp"
            os.makedirs(os.path.dirname(_INFO_CACHE_FILE), exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"items": items}, f)
            os.replace(tmp, _INFO_CACHE_FILE)
        except Exception as e:
            logger.debug(f"Info disk cache save failed: {e}")

    def _set_info_cache(self, vid_id: str, info: dict) -> None:
        """Stash the resolved info dict for a video (used by direct-URL plays to
        fill in the real title/duration; bounded FIFO, disk-persisted)."""
        if not vid_id or not isinstance(info, dict):
            return
        _INFO_CACHE.pop(vid_id, None)
        while len(_INFO_CACHE) >= _INFO_CACHE_MAX:
            _INFO_CACHE.pop(next(iter(_INFO_CACHE)))
        _INFO_CACHE[vid_id] = {
            "title": info.get("title") or "",
            "duration": info.get("duration") or 0,
            "uploader": info.get("uploader") or info.get("channel") or "",
            "thumbnail": info.get("thumbnail") or "",
        }
        self._persist_info_cache_to_disk()

    def get_info_cache(self, vid_id: str) -> Optional[dict]:
        self._load_info_cache_from_disk()
        return _INFO_CACHE.get(vid_id)

    async def get_oembed(self, link: str) -> Optional[dict]:
        """YouTube oEmbed metadata (title/author/thumbnail) — one tiny HTTP call,
        used to fill the Now Streaming card for direct-URL plays."""
        try:
            from urllib.parse import quote
            session = self._get_http_session()
            url = f"https://www.youtube.com/oembed?url={quote(link, safe='')}&format=json"
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                return {
                    "title": data.get("title") or "",
                    "author": data.get("author_name") or "",
                    "thumbnail": data.get("thumbnail_url") or "",
                }
        except Exception as e:
            logger.debug(f"oEmbed fetch failed: {e}")
            return None


    def _get_video_id(self, link: str) -> str:
        """Extract YouTube video ID from URL or return placeholder."""
        if not link:
            return ""
        if re.match(r'^[a-zA-Z0-9_-]{11}$', link):
            return link

        patterns = [
            r'(?:v=|youtu\.be/|youtube\.com/shorts/)([a-zA-Z0-9_-]{11})',
            r'youtube\.com/embed/([a-zA-Z0-9_-]{11})',
            r'youtube\.com/v/([a-zA-Z0-9_-]{11})',
        ]
        for p in patterns:
            m = re.search(p, link)
            if m:
                return m.group(1)

        if 'youtube.com/watch?v=' in link:
            return link.split('v=')[1].split('&')[0]
        if 'youtu.be/' in link:
            return link.split('youtu.be/')[1].split('?')[0]

        if not bool(re.search(self.regex, link)) and len(link.strip()) > 0:
            return "kw_" + hashlib.md5(link.encode()).hexdigest()[:8]
        return ""

    def _check_local_file(self, video_id: str, video: bool = False) -> Optional[str]:
        """Check for existing cached file for instant playback.

        Video mode only accepts .mp4 — the audio candidates (.m4a/.mp3/...)
        are listed first for audio plays and would silently satisfy a
        video=True request with a file that has no picture.
        """
        if not video_id or video_id == "unknown":
            return None
        tmp_dir = tempfile.gettempdir()
        exts = ["mp4"] if video else ["m4a", "opus", "mp4", "webm", "mp3", "ogg"]
        for ext in exts:
            candidates = [
                os.path.join(tmp_dir, f"robin_{video_id}.{ext}"),
                os.path.join(self.download_folder, f"{video_id}.{ext}"),
            ]
            for fp in candidates:
                if os.path.exists(fp) and os.path.getsize(fp) > 20480:
                    return fp
        return None

    async def get_related(self, video_id: str, title: str) -> Optional[Dict]:
        """Fetch a related/recommended track (Auto-DJ)."""
        try:
            from youtubesearchpython.__future__ import Video
            details = Video.get(f"https://www.youtube.com/watch?v={video_id}")
            suggestions = details.get("suggestions", [])
            for sug in suggestions[:10]:
                if sug.get("id") and sug.get("title") and len(sug["title"]) > 10:
                    return {
                        "title": sug["title"],
                        "vidid": sug["id"],
                        "duration": sug.get("duration", "4:00"),
                        "thumbnail_url": sug.get("thumbnails", [{}])[0].get("url"),
                    }
        except Exception:
            pass

        # Fallback: search for related content
        try:
            search_query = f"related to {title} music"
            results = VideosSearch(search_query, limit=5)
            sr = await results.next()
            if sr.get("result"):
                for res in sr["result"]:
                    if res.get("id") != video_id:
                        return {
                            "title": res.get("title"),
                            "vidid": res.get("id"),
                            "duration": res.get("duration", "4:00"),
                            "thumbnail_url": res.get("thumbnails", [{}])[0].get("url"),
                        }
        except Exception:
            pass
        return None

    async def track(self, link: str, videoid: Union[bool, str] = None) -> Tuple[Optional[Dict], Optional[str]]:
        """Get track metadata."""
        if videoid:
            link = self.base + link
        try:
            info = await asyncio.wait_for(self.search(link), timeout=config.SEARCH_TIMEOUT)
            if info:
                return info, info.get("vidid")
        except asyncio.TimeoutError:
            logger.warning(f"track() search timed out after {config.SEARCH_TIMEOUT}s for {link!r}")
        except Exception:
            pass
        return None, None

    async def details(self, link: str, videoid: Union[bool, str] = None):
        """Get detailed track info."""
        if videoid:
            link = self.base + link
        try:
            info = await asyncio.wait_for(self.search(link), timeout=config.SEARCH_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning(f"details() search timed out after {config.SEARCH_TIMEOUT}s for {link!r}")
            return None, None, 0, None, vid
        except Exception:
            return None, None, 0, None, vid
        if info:
            dur = info.get("duration", "0:00")
            dur_sec = _time_to_seconds(dur)
            return info["title"], dur, dur_sec, info.get("thumbnail_url"), info.get("vidid")
        return None, None, 0, None, vid

    async def playlist(self, playlist_id: str, limit: int = 50, user_id: int = 0, _: bool = False):
        """Fetch playlist tracks (simplified - returns first page)."""
        logger.info(f"Playlist fetch requested: {playlist_id}")
        return []

    async def slider(self, query: str, query_type: int):
        """Get next/prev search result for slider UI."""
        try:
            results = VideosSearch(query, limit=10)
            sr = await results.next()
            if sr.get("result") and query_type < len(sr["result"]):
                item = sr["result"][query_type]
                dur = item.get("duration", "0:00")
                thumb = config.STREAM_IMG_URL
                if item.get('thumbnails'):
                    thumb = item['thumbnails'][0].get('url', config.STREAM_IMG_URL)
                return item.get("title"), dur, thumb, item.get("id")
        except Exception:
            pass
        return query, "0:00", config.STREAM_IMG_URL, None

    # ─── Caching ───────────────────────────────────────────

    def _load_cache(self, cache_file: str) -> Dict:
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                pass
        return {}

    def _save_cache(self, cache_file: str, cache: Dict):
        try:
            with open(cache_file, 'w') as f:
                json.dump(cache, f, indent=2)
        except (OSError, TypeError):
            pass

    async def _cleanup_scheduler(self):
        """Periodic cleanup of old downloads."""
        await asyncio.sleep(60)
        while True:
            try:
                await self._cleanup_downloads()
                await asyncio.sleep(3600)
            except:
                await asyncio.sleep(300)

    async def _cleanup_downloads(self, max_size_mb: int = 1024):
        """Keep downloads/ bounded.

        Evict oldest files first once the folder exceeds max_size_mb
        (LRU by mtime). Cache JSONs (.stream_url_cache.json /
        .search_cache.json / .song_cache.json-style dotfiles), hidden
        files, and directories (yt-dlp-cache/) are never deleted.
        """
        try:
            files = []
            total = 0
            for fname in os.listdir(self.download_folder):
                if fname.startswith("."):
                    continue
                fp = os.path.join(self.download_folder, fname)
                if not os.path.isfile(fp):
                    continue
                try:
                    st = os.stat(fp)
                    files.append((st.st_mtime, st.st_size, fp))
                    total += st.st_size
                except OSError:
                    continue
            cap = max_size_mb * 1024 * 1024
            if total <= cap:
                return
            files.sort()
            deleted = 0
            for _, size, fp in files:
                if total <= cap:
                    break
                try:
                    os.remove(fp)
                    total -= size
                    deleted += 1
                except OSError:
                    pass
            if deleted:
                logger.info(f"Cleanup: evicted {deleted} files (downloads/ over {max_size_mb}MB)")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")


# Global singleton instance
youtube = YouTubeAPI()

# Backward compatibility functions
download_song = youtube.download
initialize_module = youtube.initialize
close_module = youtube.close
