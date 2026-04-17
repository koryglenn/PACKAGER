"""
FFmpeg transcoding pipeline.

Each recording is encoded in two sequential passes:
  1. HLS  — H.264 + AAC, segmented MPEG-TS (.ts) with M3U8 manifests
  2. DASH — H.264 + AAC, fragmented MP4 (.m4s) with an MPEG-DASH MPD

Both passes use a single FFmpeg invocation per format via filter_complex
split so all resolutions are encoded simultaneously.

Outputs land in LIBRARY_DIR/{recording_id}/:
  master.m3u8               HLS master playlist
  manifest.mpd              DASH manifest
  {res}/playlist.m3u8       HLS per-rendition playlist
  {res}/seg{N}.ts           HLS segments
  dash/init_{R}.mp4         DASH initialisation segments
  dash/seg_{R}_{N}.m4s      DASH media segments

Resolution profiles come from config.RESOLUTION_PROFILES.
"""

import json
import logging
import subprocess
import threading
from pathlib import Path
from typing import Callable, List, Optional

from config import (
    DASH_SEGMENT_DURATION,
    FFMPEG_BIN,
    FFPROBE_BIN,
    HLS_SEGMENT_DURATION,
    LIBRARY_DIR,
    RESOLUTION_PROFILES,
    THUMBNAIL_OFFSET,
    THUMBNAIL_WIDTH,
    THUMBNAILS_DIR,
)

logger = logging.getLogger(__name__)

# Manifest bandwidth hints (bits/s) — used in the HLS master playlist
_BW_MAP = {
    "1080p": 4_000_000,
    "720p":  2_500_000,
    "480p":  1_000_000,
    "360p":    500_000,
}

_RES_MAP = {
    "1080p": "1920x1080",
    "720p":  "1280x720",
    "480p":  "854x480",
    "360p":  "640x360",
}


# ── Media introspection ───────────────────────────────────────────────────────

def probe(file_path: str) -> dict:
    """Return ffprobe JSON output for the given file."""
    cmd = [
        FFPROBE_BIN, "-v", "quiet",
        "-print_format", "json",
        "-show_streams", "-show_format",
        str(file_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning("ffprobe failed for %s: %s", file_path, result.stderr[:200])
        return {}
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}


def get_duration(file_path: str) -> Optional[float]:
    info = probe(file_path)
    try:
        return float(info["format"]["duration"])
    except (KeyError, TypeError, ValueError):
        return None


def has_audio(file_path: str) -> bool:
    info = probe(file_path)
    return any(s.get("codec_type") == "audio" for s in info.get("streams", []))


# ── Thumbnail ─────────────────────────────────────────────────────────────────

def extract_thumbnail(input_path: str, recording_id: str) -> Optional[str]:
    """
    Extract a single JPEG frame at THUMBNAIL_OFFSET seconds.
    Returns the file path on success, None on failure.
    """
    out = THUMBNAILS_DIR / f"{recording_id}.jpg"
    duration = get_duration(input_path) or 0
    offset = min(THUMBNAIL_OFFSET, max(0, duration - 1))

    cmd = [
        FFMPEG_BIN, "-y",
        "-ss", str(offset),
        "-i", str(input_path),
        "-vframes", "1",
        "-q:v", "2",
        "-vf", f"scale={THUMBNAIL_WIDTH}:-1",
        str(out),
    ]
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode == 0 and out.exists():
        return str(out)
    logger.warning("Thumbnail extraction failed for %s", recording_id)
    return None


# ── Internal: filter_complex builder ─────────────────────────────────────────

def _build_filter_complex(profiles: list) -> str:
    """Return a filter_complex string that splits and scales to each resolution."""
    n = len(profiles)
    split_outs = "".join(f"[vraw{i}]" for i in range(n))
    parts = [f"[0:v]split={n}{split_outs}"]
    for i, p in enumerate(profiles):
        parts.append(f"[vraw{i}]scale={p['width']}:-2[v{i}]")
    return ";".join(parts)


def _run_ffmpeg(cmd: List[str], total_duration: float,
                progress_cb: Optional[Callable[[int], None]],
                label: str) -> None:
    """Run an FFmpeg command, parse stderr for progress, raise on failure."""
    logger.info("%s command: %s …", label, " ".join(cmd[:6]))

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    for line in proc.stderr:
        if progress_cb and total_duration > 0 and "time=" in line:
            try:
                raw = line.split("time=")[1].split()[0]
                h, m, s = raw.split(":")
                elapsed = float(h) * 3600 + float(m) * 60 + float(s)
                pct = min(int(elapsed / total_duration * 100), 99)
                progress_cb(pct)
            except Exception:
                pass

    proc.wait()
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)


# ── HLS transcoding ───────────────────────────────────────────────────────────

def transcode_to_hls(
    recording_id: str,
    input_path: str,
    resolution_names: List[str],
    progress_cb: Optional[Callable[[int], None]] = None,
) -> List[str]:
    """
    Single FFmpeg pass → multi-resolution HLS (H.264 + AAC, MPEG-TS segments).

    Directory layout:
        LIBRARY_DIR/{id}/{res}/playlist.m3u8
        LIBRARY_DIR/{id}/{res}/seg{N:04d}.ts
        LIBRARY_DIR/{id}/master.m3u8

    Returns list of resolution names produced.
    """
    profiles = [p for p in RESOLUTION_PROFILES if p["name"] in resolution_names]
    if not profiles:
        raise ValueError(f"No matching profiles for: {resolution_names}")

    lib_dir = LIBRARY_DIR / recording_id
    lib_dir.mkdir(parents=True, exist_ok=True)

    audio = has_audio(input_path)
    total_duration = get_duration(input_path) or 0
    n = len(profiles)

    cmd = [
        FFMPEG_BIN, "-y",
        "-i", str(input_path),
        "-filter_complex", _build_filter_complex(profiles),
    ]

    for i, p in enumerate(profiles):
        out_dir = lib_dir / p["name"]
        out_dir.mkdir(exist_ok=True)

        cmd += [
            "-map", f"[v{i}]",
            *([ "-map", "0:a?"] if audio else []),
            # Video
            "-c:v", "libx264",
            "-preset", "fast",
            "-b:v", f"{p['video_bitrate']}k",
            "-maxrate", f"{p['maxrate']}k",
            "-bufsize", f"{p['bufsize']}k",
            "-profile:v", "main",
            "-level:v", "4.0",
            "-g", str(HLS_SEGMENT_DURATION * 2),
            "-sc_threshold", "0",
            # Audio
            *(
                ["-c:a", "aac", "-b:a", p["audio_bitrate"], "-ar", "48000"]
                if audio else []
            ),
            # HLS muxer
            "-hls_time", str(HLS_SEGMENT_DURATION),
            "-hls_playlist_type", "vod",
            "-hls_segment_filename", str(out_dir / "seg%04d.ts"),
            str(out_dir / "playlist.m3u8"),
        ]

    _run_ffmpeg(cmd, total_duration, progress_cb, f"HLS {recording_id}")

    completed = [p["name"] for p in profiles]
    _write_hls_master(lib_dir, profiles)
    logger.info("HLS complete for %s: %s", recording_id, completed)
    return completed


def _write_hls_master(lib_dir: Path, profiles: list):
    lines = ["#EXTM3U", "#EXT-X-VERSION:3", ""]
    for p in profiles:
        name = p["name"]
        bw = _BW_MAP.get(name, p["video_bitrate"] * 1000)
        res = _RES_MAP.get(name, "")
        lines.append(f'#EXT-X-STREAM-INF:BANDWIDTH={bw},RESOLUTION={res},NAME="{name}"')
        lines.append(f"{name}/playlist.m3u8")
    (lib_dir / "master.m3u8").write_text("\n".join(lines) + "\n")


# ── DASH transcoding ──────────────────────────────────────────────────────────

def transcode_to_dash(
    recording_id: str,
    input_path: str,
    resolution_names: List[str],
    progress_cb: Optional[Callable[[int], None]] = None,
) -> None:
    """
    Single FFmpeg pass → multi-representation MPEG-DASH (H.264 + AAC, fMP4).

    Uses per-stream bitrate specifiers (-b:v:N) so all representations are
    encoded in one FFmpeg invocation.

    Directory layout:
        LIBRARY_DIR/{id}/manifest.mpd
        LIBRARY_DIR/{id}/dash/init_{R}.mp4      (init segments)
        LIBRARY_DIR/{id}/dash/seg_{R}_{N}.m4s   (media segments)

    R = FFmpeg's internal RepresentationID (0, 1, 2, … for video; next index for audio).
    """
    profiles = [p for p in RESOLUTION_PROFILES if p["name"] in resolution_names]
    if not profiles:
        raise ValueError(f"No matching profiles for: {resolution_names}")

    lib_dir = LIBRARY_DIR / recording_id
    dash_dir = lib_dir / "dash"
    dash_dir.mkdir(parents=True, exist_ok=True)

    audio = has_audio(input_path)
    total_duration = get_duration(input_path) or 0
    n = len(profiles)

    cmd = [
        FFMPEG_BIN, "-y",
        "-i", str(input_path),
        "-filter_complex", _build_filter_complex(profiles),
    ]

    # Map all video streams first, then a single optional audio stream.
    # Per-stream bitrates are set via -b:v:N stream specifiers after all maps.
    for i in range(n):
        cmd += ["-map", f"[v{i}]"]
    if audio:
        cmd += ["-map", "0:a?"]

    # Global video encoder settings
    cmd += [
        "-c:v", "libx264",
        "-preset", "fast",
        "-profile:v", "main",
        "-level:v", "4.0",
        "-g", str(DASH_SEGMENT_DURATION * 2),
        "-sc_threshold", "0",
    ]

    # Per-stream bitrates
    for i, p in enumerate(profiles):
        cmd += [
            f"-b:v:{i}", f"{p['video_bitrate']}k",
            f"-maxrate:v:{i}", f"{p['maxrate']}k",
            f"-bufsize:v:{i}", f"{p['bufsize']}k",
        ]

    # Audio (single track shared across all representations)
    if audio:
        cmd += ["-c:a", "aac", "-b:a", "128k", "-ar", "48000"]

    # DASH muxer — paths are relative to the MPD output file location
    adaptation_sets = "id=0,streams=v"
    if audio:
        adaptation_sets += " id=1,streams=a"

    cmd += [
        "-f", "dash",
        "-seg_duration", str(DASH_SEGMENT_DURATION),
        "-use_timeline", "1",
        "-use_template", "1",
        "-adaptation_sets", adaptation_sets,
        # Segment naming: paths relative to manifest.mpd (in lib_dir)
        "-init_seg_name", "dash/init_$RepresentationID$.mp4",
        "-media_seg_name", "dash/seg_$RepresentationID$_$Number%05d$.m4s",
        str(lib_dir / "manifest.mpd"),
    ]

    _run_ffmpeg(cmd, total_duration, progress_cb, f"DASH {recording_id}")
    logger.info("DASH complete for %s", recording_id)


# ── Combined HLS + DASH ───────────────────────────────────────────────────────

def transcode_hls_and_dash(
    recording_id: str,
    input_path: str,
    resolution_names: List[str],
    progress_cb: Optional[Callable[[int], None]] = None,
) -> List[str]:
    """
    Run HLS then DASH transcoding sequentially.

    Progress is split 0–50 % for HLS and 50–100 % for DASH so callers get
    a smooth 0→100 % update across both passes.

    Returns the list of resolution names produced (from the HLS pass).
    """

    def _hls_progress(pct: int):
        if progress_cb:
            progress_cb(pct // 2)             # maps 0-100 → 0-50

    def _dash_progress(pct: int):
        if progress_cb:
            progress_cb(50 + pct // 2)        # maps 0-100 → 50-100

    completed = transcode_to_hls(recording_id, input_path, resolution_names, _hls_progress)

    if progress_cb:
        progress_cb(50)

    transcode_to_dash(recording_id, input_path, resolution_names, _dash_progress)

    if progress_cb:
        progress_cb(100)

    return completed


# ── Background helper ─────────────────────────────────────────────────────────

def transcode_async(
    recording_id: str,
    input_path: str,
    resolution_names: List[str],
    on_progress: Callable[[int], None],
    on_complete: Callable[[List[str]], None],
    on_error: Callable[[str], None],
):
    """Run transcode_hls_and_dash in a daemon thread."""

    def _run():
        try:
            completed = transcode_hls_and_dash(
                recording_id, input_path, resolution_names, on_progress
            )
            on_complete(completed)
        except Exception as exc:
            logger.exception("Transcode failed for %s", recording_id)
            on_error(str(exc))

    t = threading.Thread(target=_run, daemon=True, name=f"transcode-{recording_id[:8]}")
    t.start()
