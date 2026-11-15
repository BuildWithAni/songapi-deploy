"""
Telegram File Platform Integration
====================================
Stream audio/video files directly from Telegram messages.
Adapted from Shakky-Music's shakky/platforms/Telegram.py.
"""

import asyncio
import logging
import os
from typing import Optional, Tuple

from robin.music.config import FILE_FETCH_TIMEOUT, DOWNLOAD_TIMEOUT

logger = logging.getLogger("robin.music.telegram")


class TeleAPI:
    """Telegram file streaming support."""

    async def get_filepath(self, audio=None, video=None) -> Optional[str]:
        """Generate a file path for a Telegram media file."""
        if audio:
            return audio.file_id or f"tg_audio_{id(audio)}"
        if video:
            return video.file_id or f"tg_video_{id(video)}"
        return None

    async def get_filename(self, media, audio: bool = True) -> str:
        """Extract filename from Telegram media."""
        if hasattr(media, 'file_name') and media.file_name:
            return media.file_name
        if audio:
            return "Telegram Audio"
        return "Telegram Video"

    async def get_duration(self, media, file_name: str) -> str:
        """Extract duration from Telegram media."""
        if hasattr(media, 'duration') and media.duration:
            minutes = media.duration // 60
            seconds = media.duration % 60
            return f"{minutes}:{seconds:02d}"
        return "0:00"

    async def download(self, _, message, mystic, file_name: str) -> Optional[str]:
        """
        Download a Telegram file. Uses PTB's bot to download.
        NOTE: This requires the PTB bot instance, which is passed as _ (bot).
        """
        try:
            import telegram
            if isinstance(_, telegram.ext.Application):
                bot = _.bot
            else:
                bot = _

            file = None
            if message.audio:
                file = await asyncio.wait_for(
                    bot.get_file(message.audio.file_id), timeout=FILE_FETCH_TIMEOUT
                )
            elif message.voice:
                file = await asyncio.wait_for(
                    bot.get_file(message.voice.file_id), timeout=FILE_FETCH_TIMEOUT
                )
            elif message.video:
                file = await asyncio.wait_for(
                    bot.get_file(message.video.file_id), timeout=FILE_FETCH_TIMEOUT
                )
            elif message.document:
                file = await asyncio.wait_for(
                    bot.get_file(message.document.file_id), timeout=FILE_FETCH_TIMEOUT
                )

            if file:
                await asyncio.wait_for(
                    file.download_to_drive(custom_path=file_name),
                    timeout=DOWNLOAD_TIMEOUT,
                )
                if os.path.exists(file_name):
                    return file_name
            return None
        except Exception as e:
            logger.error(f"Telegram download failed: {e}")
            return None
