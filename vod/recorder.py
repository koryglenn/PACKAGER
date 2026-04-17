"""
Live-stream capture via FFmpeg.

Supports RTSP, HLS, and UDP/TS sources.  Recordings are written as raw
MPEG-TS (.ts) files to RECORDINGS_DIR/{id}/raw.ts.
"""

import logging
import subprocess
from pathlib import Path
from typing import Optional

from config import RECORDINGS_DIR, FFMPEG_BIN

logger = logging.getLogger(__name__)

# Map of recording_id → active Popen process
_active: dict[str, subprocess.Popen] = {}


def start_recording(
    recording_id: str,
    source_url: str,
    source_type: str,
    duration_seconds: Optional[int] = None,
) -> int:
    """
    Launch FFmpeg to capture a live source.

    Returns the PID of the FFmpeg process.
    Raises RuntimeError if a recording for this id is already active.
    """
    if recording_id in _active and _active[recording_id].poll() is None:
        raise RuntimeError(f"Recording {recording_id} is already active")

    out_dir = RECORDINGS_DIR / recording_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "raw.ts"

    cmd = [FFMPEG_BIN, "-y"]

    # ── Input options per source type ────────────────────────────────────────
    if source_type == "rtsp":
        cmd += ["-rtsp_transport", "tcp", "-i", source_url]
    elif source_type == "udp":
        # Expects address in host:port or multicast:port form
        url = source_url if source_url.startswith("udp://") else f"udp://{source_url}"
        cmd += ["-i", url]
    else:
        # hls or any http-accessible source
        cmd += ["-i", source_url]

    # ── Output options ────────────────────────────────────────────────────────
    # Copy streams verbatim — transcoding happens in a separate step.
    cmd += ["-c", "copy"]

    if duration_seconds and duration_seconds > 0:
        cmd += ["-t", str(duration_seconds)]

    cmd += [str(out_path)]

    logger.info("Starting recording %s: %s", recording_id, " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    _active[recording_id] = proc
    return proc.pid


def stop_recording(recording_id: str) -> bool:
    """
    Gracefully stop an active recording.

    Sends SIGTERM; if the process doesn't exit within 10 s, sends SIGKILL.
    Returns True if a process was found and stopped, False if none was active.
    """
    proc = _active.get(recording_id)
    if proc is None:
        return False

    if proc.poll() is not None:
        # Already finished on its own
        _active.pop(recording_id, None)
        return True

    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        logger.warning("FFmpeg did not exit — killing PID %d", proc.pid)
        proc.kill()
        proc.wait()

    _active.pop(recording_id, None)
    logger.info("Stopped recording %s", recording_id)
    return True


def is_active(recording_id: str) -> bool:
    proc = _active.get(recording_id)
    return proc is not None and proc.poll() is None


def recording_path(recording_id: str) -> Optional[Path]:
    """Return the raw recording path if the file exists."""
    p = RECORDINGS_DIR / recording_id / "raw.ts"
    return p if p.exists() else None
