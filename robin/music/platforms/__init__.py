"""
Music Platform Integrations
=============================
YouTube, Spotify, Apple Music, Resso, SoundCloud, and Telegram file support.
"""

from .Youtube import YouTubeAPI
from .Spotify import SpotifyAPI
from .Apple import AppleAPI
from .Resso import RessoAPI
from .Soundcloud import SoundAPI
from .Telegram import TeleAPI

# Global singleton instances
YouTube = YouTubeAPI()
Spotify = SpotifyAPI()
Apple = AppleAPI()
Resso = RessoAPI()
SoundCloud = SoundAPI()
Telegram = TeleAPI()

__all__ = [
    'YouTube', 'Spotify', 'Apple', 'Resso', 'SoundCloud', 'Telegram',
    'YouTubeAPI', 'SpotifyAPI', 'AppleAPI', 'RessoAPI', 'SoundAPI', 'TeleAPI',
]
