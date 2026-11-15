"""
Spotify Platform Integration
=============================
Spotify track, playlist, album, and artist resolution.
Adapted from Shakky-Music's shakky/platforms/Spotify.py.
"""

import logging
import re
from typing import Optional, Dict, Tuple, List
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from robin.music.config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET

logger = logging.getLogger("robin.music.spotify")


class SpotifyAPI:
    """Spotify integration for resolving tracks/playlists/albums/artists."""

    def __init__(self):
        self._client = None
        self._initialized = False
        self.pattern = r"(?:https?://)?(?:open\.)?spotify\.com/(?:intl-\w+/)?(track|playlist|album|artist)/([\w]+)"

    async def _ensure_client(self):
        """Lazy-init the Spotipy client."""
        if not self._initialized:
            try:
                auth = SpotifyClientCredentials(
                    client_id=SPOTIFY_CLIENT_ID,
                    client_secret=SPOTIFY_CLIENT_SECRET,
                )
                self._client = spotipy.Spotify(auth_manager=auth)
                self._initialized = True
                logger.info("Spotify client initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Spotify client: {e}")

    async def valid(self, query: str) -> bool:
        """Check if the query is a valid Spotify URL."""
        return bool(re.search(self.pattern, query))

    async def _parse_url(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract (type, id) from a Spotify URL."""
        m = re.search(self.pattern, url)
        if m:
            return m.group(1), m.group(2)
        return None, None

    async def track(self, url: str) -> Tuple[Optional[Dict], Optional[str]]:
        """Resolve a Spotify track URL to searchable metadata."""
        await self._ensure_client()
        if not self._client:
            return None, None

        try:
            _, track_id = await self._parse_url(url)
            if not track_id:
                return None, None

            track = self._client.track(track_id)
            artists = ", ".join(a["name"] for a in track["artists"])
            title = f"{artists} - {track['name']}"
            
            result = {
                "title": title,
                "duration_min": f"{track['duration_ms'] // 60000}:{(track['duration_ms'] % 60000) // 1000:02d}",
                "thumb": track.get("album", {}).get("images", [{}])[0].get("url", ""),
                "vidid": f"sp_{track_id}",
                "link": url,
            }
            return result, track_id
        except Exception as e:
            logger.error(f"Spotify track resolve failed: {e}")
            return None, None

    async def playlist(self, url: str) -> Tuple[Optional[List[str]], Optional[str]]:
        """Resolve a Spotify playlist URL to a list of search queries."""
        await self._ensure_client()
        if not self._client:
            return [], None

        try:
            _, playlist_id = await self._parse_url(url)
            if not playlist_id:
                return [], None

            results = self._client.playlist_tracks(playlist_id, limit=50)
            tracks = []
            for item in results.get("items", []):
                if item and item.get("track"):
                    t = item["track"]
                    artists = ", ".join(a["name"] for a in t["artists"])
                    tracks.append(f"{artists} - {t['name']}")

            return tracks, playlist_id
        except Exception as e:
            logger.error(f"Spotify playlist resolve failed: {e}")
            return [], None

    async def album(self, url: str) -> Tuple[Optional[List[str]], Optional[str]]:
        """Resolve a Spotify album URL to a list of search queries."""
        await self._ensure_client()
        if not self._client:
            return [], None

        try:
            _, album_id = await self._parse_url(url)
            if not album_id:
                return [], None

            results = self._client.album_tracks(album_id, limit=50)
            album_info = self._client.album(album_id)
            artist = album_info.get("artists", [{}])[0].get("name", "")

            tracks = []
            for item in results.get("items", []):
                tracks.append(f"{artist} - {item['name']}")

            return tracks, album_id
        except Exception as e:
            logger.error(f"Spotify album resolve failed: {e}")
            return [], None

    async def artist(self, url: str) -> Tuple[Optional[List[str]], Optional[str]]:
        """Resolve a Spotify artist URL to a list of top tracks."""
        await self._ensure_client()
        if not self._client:
            return [], None

        try:
            _, artist_id = await self._parse_url(url)
            if not artist_id:
                return [], None

            results = self._client.artist_top_tracks(artist_id)
            artist_info = self._client.artist(artist_id)
            artist_name = artist_info.get("name", "")

            tracks = [f"{artist_name} - {t['name']}" for t in results.get("tracks", [])[:20]]
            return tracks, artist_id
        except Exception as e:
            logger.error(f"Spotify artist resolve failed: {e}")
            return [], None
