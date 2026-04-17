import os
from pathlib import Path

BASE_DIR = Path(__file__).parent
RECORDINGS_DIR = BASE_DIR / "recordings"
LIBRARY_DIR = BASE_DIR / "library"
THUMBNAILS_DIR = BASE_DIR / "thumbnails"
DB_PATH = BASE_DIR / "vod.db"

for _d in [RECORDINGS_DIR, LIBRARY_DIR, THUMBNAILS_DIR]:
    _d.mkdir(exist_ok=True)

FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")

SERVER_HOST = os.environ.get("VOD_HOST", "0.0.0.0")
SERVER_PORT = int(os.environ.get("VOD_PORT", "8090"))

# H.264 resolution profiles — name, target dimensions, bitrates
RESOLUTION_PROFILES = [
    {
        "name": "1080p",
        "width": 1920,
        "height": 1080,
        "video_bitrate": 4000,   # kbps
        "maxrate":        4400,
        "bufsize":        8000,
        "audio_bitrate": "192k",
    },
    {
        "name": "720p",
        "width": 1280,
        "height": 720,
        "video_bitrate": 2500,
        "maxrate":        2750,
        "bufsize":        5000,
        "audio_bitrate": "128k",
    },
    {
        "name": "480p",
        "width": 854,
        "height": 480,
        "video_bitrate": 1000,
        "maxrate":        1100,
        "bufsize":        2000,
        "audio_bitrate": "96k",
    },
    {
        "name": "360p",
        "width": 640,
        "height": 360,
        "video_bitrate": 500,
        "maxrate":        550,
        "bufsize":        1000,
        "audio_bitrate": "64k",
    },
]

DEFAULT_RESOLUTIONS = ["1080p", "720p", "480p"]

HLS_SEGMENT_DURATION = 6       # seconds per HLS segment
DASH_SEGMENT_DURATION = 6      # seconds per DASH segment
RETENTION_CHECK_INTERVAL = 3600  # seconds between retention sweeps
THUMBNAIL_OFFSET = 5.0           # seconds into video for thumbnail
THUMBNAIL_WIDTH = 320
