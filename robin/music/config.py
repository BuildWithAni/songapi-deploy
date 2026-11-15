"""
Minimal, env-only config for the songapi bundle.
NO secrets or credentials hardcoded — everything comes from env vars.
"""

import os

# ─── Resolution sources (all optional — missing ones just disable that path) ─
API_URL = os.getenv("API_URL", "").strip()            # NexGen song API
API_KEY = os.getenv("API_KEY", "").strip()
YUKI_API_URL = os.getenv("YUKI_API_URL", "").strip()  # Yuki proxy
YUKI_API_KEY = os.getenv("YUKI_API_KEY", "").strip()
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "").strip()
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "").strip()

# ─── Images ────────────────────────────────────────────────────
STREAM_IMG_URL = "https://i.ytimg.com/vi/{}/maxresdefault.jpg"

# ─── Limits ───────────────────────────────────────────────────
SEARCH_TIMEOUT = int(os.getenv("SEARCH_TIMEOUT", "8"))

# ─── Referenced by Youtube.py but unused on the API path ──────
DURATION_LIMIT_MIN = int(os.getenv("DURATION_LIMIT", "36000"))
PLAYLIST_FETCH_LIMIT = int(os.getenv("PLAYLIST_FETCH_LIMIT", "2500"))
SERVER_PLAYLIST_LIMIT = int(os.getenv("SERVER_PLAYLIST_LIMIT", "3000"))
SONG_DOWNLOAD_DURATION = int(os.getenv("SONG_DOWNLOAD_DURATION", "9999999"))
TG_AUDIO_FILESIZE_LIMIT = int(os.getenv("TG_AUDIO_FILESIZE_LIMIT", "5242880000"))
TG_VIDEO_FILESIZE_LIMIT = int(os.getenv("TG_VIDEO_FILESIZE_LIMIT", "5242880000"))

# Spotify / sessions (unused by songapi but imported by platform modules)
STRING1 = os.getenv("STRING1", "")
STRING2 = os.getenv("STRING2", "")
STRING3 = os.getenv("STRING3", "")
STRING4 = os.getenv("STRING4", "")
STRING5 = os.getenv("STRING5", "")
MUSIC_API_ID = os.getenv("MUSIC_API_ID", "")
MUSIC_API_HASH = os.getenv("MUSIC_API_HASH", "")
STICKER = os.getenv("STICKER", "")

DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "120"))
FILE_FETCH_TIMEOUT = int(os.getenv("FILE_FETCH_TIMEOUT", "20"))
PP_API_KEY = os.getenv("PP_API_KEY", "")
