"""
SoundCloud Platform Integration
=================================
SoundCloud track resolution.
Adapted from Shakky-Music's shakky/platforms/Soundcloud.py.
"""

import logging
from typing import Optional, Dict, Tuple

logger = logging.getLogger("robin.music.soundcloud")


class SoundAPI:
    """SoundCloud integration - uses yt-dlp for downloads."""

    async def valid(self, query: str) -> bool:
        """Check if the query is a SoundCloud URL."""
        return "soundcloud.com" in query.lower() or "snd.cloud" in query.lower()
