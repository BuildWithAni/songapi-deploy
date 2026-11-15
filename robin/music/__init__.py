"""
Shakky Music Integration for Daily Tune
====================================
Seamless music streaming module adapted from Shakky-Music,
integrated into Daily Tune's python-telegram-bot ecosystem.

Provides:
  - Voice chat playback via PyTgCalls
  - YouTube, Spotify, Apple Music, Resso, SoundCloud support
  - Queue management, pause/resume/skip/stop controls
  - Assistant userbot management for voice calls
"""

# ── Compatibility shim: must run before ANY pytgcalls import ──
# pytgcalls expects `GroupcallForbidden` from pyrogram.errors,
# but pyrogram 2.x renamed it to `GroupCallInvalid`.
import pyrogram.errors as _pyro_errors
if not hasattr(_pyro_errors, "GroupcallForbidden"):
    try:
        _pyro_errors.GroupcallForbidden = _pyro_errors.GroupCallInvalid
    except AttributeError:
        class GroupcallForbidden(Exception):
            pass
        _pyro_errors.GroupcallForbidden = GroupcallForbidden
# ── End shim ──

import asyncio
import logging
import os
from typing import Optional

logger = logging.getLogger("robin.music")

# ── Monkey-patch pytgcalls build_command ──
# pytgcalls hardcodes `-reconnect 1 -reconnect_at_eof 1 -reconnect_streamed 1
# -reconnect_delay_max 2` for all non-local file (URL) streams.
#   * `-reconnect_at_eof 1` is the dangerous one: at end-of-stream FFmpeg
#     restarts from byte 0, the song replays forever, StreamEnded never fires
#     and the queue never advances (v1.73 bug).
#   * Turning ALL of them off (the previous patch) swings the other way: any
#     mid-stream CDN cut ends the song early and the queue skips ahead, which
#     is what "songs end in the middle" looked like.
# The patch therefore rewrites them to the safe set:
#   -reconnect 1            -> resume a connection that dropped mid-song
#   -reconnect_at_eof 0     -> a real EOF still ends the song (queue advances)
#   -reconnect_streamed 0   -> only resume when Content-Length + Range are
#                              known; never reconnect blindly on streamed
#                              responses (verified: that is the configuration
#                              that can loop forever on a proxy that ignores
#                              Range requests)
#   -reconnect_delay_max 2  -> short backoff between resume attempts
try:
    import pytgcalls.ffmpeg as _pyffmpeg
    import pytgcalls.types.stream.media_stream as _pyms

    _orig_pytgcalls_build_cmd = _pyffmpeg.build_command

    # Desired value for every FFmpeg option that can trigger a stream restart.
    _SAFE_RECONNECT = {
        '-reconnect': '1',
        '-reconnect_at_eof': '0',
        '-reconnect_streamed': '0',
        '-reconnect_delay_max': '2',
    }

    def _safe_pytgcalls_build_cmd(name, ffmpeg_parameters, path, stream_parameters, before_commands=None, headers=None, is_livestream=False):
        cmd = _orig_pytgcalls_build_cmd(name, ffmpeg_parameters, path, stream_parameters, before_commands, headers, is_livestream)
        new_cmd = []
        i = 0
        patched = False
        while i < len(cmd):
            tok = cmd[i]
            if tok in _SAFE_RECONNECT:
                # Rewrite in place; always take the value that follows so the
                # option can never end up dangling.
                new_cmd.extend([tok, _SAFE_RECONNECT[tok]])
                patched = True
                i += 2 if i + 1 < len(cmd) else 1
            elif tok == '-reconnect_max_retries':
                # Not an FFmpeg option at all — drop it with its value.
                i += 2 if i + 1 < len(cmd) else 1
            else:
                new_cmd.append(tok)
                i += 1
        if patched:
            logger.debug(f"build_command reconnect flags rewritten: {' '.join(new_cmd)}")
        return new_cmd

    _pyffmpeg.build_command = _safe_pytgcalls_build_cmd
    _pyms.build_command = _safe_pytgcalls_build_cmd
    logger.info(
        "Patched pytgcalls build_command: -reconnect 1 / -reconnect_at_eof 0 / "
        "-reconnect_streamed 0 (resume mid-song cuts, still exit at EOF)"
    )
except Exception as _patch_err:
    logger.warning(f"Could not monkey-patch pytgcalls build_command: {_patch_err}")
# ── End pytgcalls patch ──

# Global music state mirroring Shakky's misc.db
# Each entry: chat_id -> list of track dicts
music_queue: dict = {}

# Active voice chat sessions (chat_id -> True/False)
active_chats: list = []

# Active video voice chat sessions
active_video_chats: list = []

# Track the last played song for auto-DJ
last_played: dict = {}

# Global music module state
_music_initialized = False
_music_bot_app = None  # Pyrogram bot client
_music_call_handler = None  # PyTgCalls wrapper instance
_music_userbot = None  # Assistant userbot manager

# Call handler singleton for other modules to access
_call_handler = None


def get_call_handler():
    """Get the global MusicCallHandler instance."""
    return _call_handler


def set_call_handler(handler):
    """Set the global MusicCallHandler instance."""
    global _call_handler
    _call_handler = handler


def get_queue(chat_id: int) -> list:
    """Get the music queue for a specific chat."""
    return music_queue.get(chat_id, [])


def set_queue(chat_id: int, queue: list):
    """Set the music queue for a specific chat."""
    music_queue[chat_id] = queue


def clear_queue(chat_id: int):
    """Clear the music queue for a specific chat."""
    if chat_id in music_queue:
        music_queue[chat_id] = []


def is_active(chat_id: int) -> bool:
    """Check if a voice chat is active in a chat."""
    return chat_id in active_chats


async def _warm_stream_cache(limit: int = 20, concurrency: int = 2, per_item: float = 12.0):
    """Background prefetch: resolve stream URLs for the most recently cached
    (Telegram-storage) songs so first/ repeat plays skip the yt-dlp race.

    Bounded: does not block module init, never raises into the caller, only
    touches the URL race (no downloads), and respects the per-item timeout.
    Plays of freshly resolved songs re-cache as usual afterwards.
    """
    try:
        import os
        import sqlite3
        from robin.music.platforms import YouTube

        if not os.path.exists("robin_music.db"):
            return
        rows = sqlite3.connect("robin_music.db").execute(
            "SELECT vidid FROM music_storage ORDER BY rowid DESC LIMIT ?",
            (limit,),
        ).fetchall()
        vids = [r[0] for r in rows if r and r[0]]
        if not vids:
            logger.info("Stream cache warm-up: no cached vidids, skipping")
            return
        logger.info(f"Stream cache warm-up: prefetching {len(vids)} recently cached vidid(s)")
        sem = asyncio.Semaphore(concurrency)

        async def _one(vid):
            async with sem:
                try:
                    await asyncio.wait_for(
                        YouTube.get_stream_url(
                            f"https://www.youtube.com/watch?v={vid}", url_only=True
                        ),
                        timeout=per_item,
                    )
                except Exception:
                    pass

        await asyncio.gather(*[_one(v) for v in vids])
        logger.info("Stream cache warm-up: done")
    except Exception as e:
        logger.debug(f"Stream cache warm-up failed: {e}")


async def music_init(bot_app=None, loop=None):
    """
    Initialize the music module.
    Must be called after the database is connected.
    
    Args:
        bot_app: The PTB Application instance (for job scheduling)
        loop: Optional asyncio event loop
    """
    global _music_initialized
    global _music_userbot
    global _music_call_handler
    
    if _music_initialized:
        return True
    
    try:
        from robin.music.core.userbot import MusicUserbotManager
        from robin.music.core.call import MusicCallHandler
        from robin.music.platforms import YouTube
        
        # Initialize userbot manager
        _music_userbot = MusicUserbotManager()
        await _music_userbot.initialize()
        logger.info("Music userbot manager initialized")
        
        # Initialize PyTgCalls handler - pass PTB app for invite functionality
        _music_call_handler = MusicCallHandler(_music_userbot, ptb_application=bot_app)
        await _music_call_handler.initialize()
        logger.info("Music call handler initialized")
        
        # Set global call handler reference for commands
        global _call_handler
        _call_handler = _music_call_handler
        
        # Wire up call handler to command modules
        try:
            from robin.music.commands.play import set_call_handler as set_play_handler
            from robin.music.commands.vplay import set_call_handler as set_vplay_handler
            from robin.music.commands.controls import set_call_handler as set_control_handler
            from robin.music.commands.callbacks import set_call_handler as set_callback_handler
            set_play_handler(_music_call_handler)
            set_vplay_handler(_music_call_handler)
            set_control_handler(_music_call_handler)
            set_callback_handler(_music_call_handler)
            logger.info("Call handler wired to play/vplay/controls/callbacks commands")
        except Exception as e:
            logger.warning(f"Failed to wire call handler to commands: {e}")
        
        # Initialize YouTube API
        await YouTube.initialize()
        logger.info("YouTube API initialized")
        
        # Initialize Telegram Storage System
        try:
            from robin.music.storage import storage
            from robin.music.utils.database import MusicDatabase
            music_db = MusicDatabase()
            await music_db.connect()
            await storage.initialize(bot_app.bot, music_db, userbot_manager=_music_userbot)
            logger.info("Telegram storage system initialized")
        except Exception as storage_err:
            logger.warning(f"Storage initialization failed (non-critical): {storage_err}")

        # Clean stale vplay files from previous sessions
        try:
            from robin.music.commands.vplay import cleanup_stale_vplay_files
            cleanup_stale_vplay_files()
            logger.info("Stale vplay files cleaned")
        except Exception as vplay_err:
            logger.debug(f"VPlay cleanup skipped: {vplay_err}")
        
        # Warm the stream-URL cache in the background so the first play of a
        # popular song doesn't pay the yt-dlp race — bounded, non-blocking.
        try:
            asyncio.create_task(_warm_stream_cache())
        except Exception as e:
            logger.debug(f"Stream cache warm-up skipped: {e}")

        # Start the shared song-resolve API (127.0.0.1:8765) for the sibling bots
        try:
            from robin.music.api_server import start_song_api
            asyncio.create_task(start_song_api())
        except Exception as e:
            logger.warning(f"songapi failed to start: {e}")

        _music_initialized = True
        logger.info("Music module fully initialized")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize music module: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


async def music_shutdown():
    """Gracefully shutdown the music module."""
    global _music_initialized
    global _music_userbot
    global _music_call_handler
    global _call_handler
    
    try:
        if _music_call_handler:
            await _music_call_handler.shutdown()
        
        if _music_userbot:
            await _music_userbot.shutdown()

        from robin.music.platforms import YouTube
        await YouTube.close()

        logger.info("Music module shut down")
    except Exception as e:
        logger.error(f"Error during music module shutdown: {e}")
    finally:
        # Drop references so PyTgCalls / userbot / HTTP clients can be GC'd
        # even if one of the shutdown coroutines above raised.
        _music_call_handler = None
        _music_userbot = None
        _call_handler = None
        _music_initialized = False


__all__ = [
    'music_queue', 'active_chats', 'active_video_chats', 'last_played',
    'get_queue', 'set_queue', 'clear_queue', 'is_active',
    'music_init', 'music_shutdown',
    'get_call_handler', 'set_call_handler', '_call_handler',
]
