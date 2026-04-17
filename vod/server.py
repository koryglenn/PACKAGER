"""
Local VOD System — FastAPI backend.

Endpoints
─────────
GET  /                                    → library UI
GET  /player/{id}                         → player UI

GET  /api/recordings                      → list / search
POST /api/recordings                      → create entry (no file)
GET  /api/recordings/{id}                 → details
PATCH /api/recordings/{id}               → update metadata
DELETE /api/recordings/{id}              → delete

POST /api/recordings/record              → start live capture + return id
POST /api/recordings/{id}/stop           → stop live capture → transcode
POST /api/recordings/{id}/upload         → upload file → transcode
POST /api/recordings/{id}/transcode      → re-transcode existing raw file

GET  /api/recordings/{id}/stream/master.m3u8
GET  /api/recordings/{id}/stream/{res}/playlist.m3u8
GET  /api/recordings/{id}/stream/{res}/{seg}.ts
GET  /api/recordings/{id}/thumbnail

GET  /api/stats
"""

import json
import logging
import shutil
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import func

import config
import recorder
import transcoder
from database import SessionLocal, get_db, init_db
from models import Recording
from schemas import (
    RecordingCreate,
    RecordingResponse,
    RecordingUpdate,
    StartRecordRequest,
    StatsResponse,
    TranscodeRequest,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Retention scheduler ───────────────────────────────────────────────────────

_scheduler = BackgroundScheduler()


def _run_retention():
    db = SessionLocal()
    try:
        now = datetime.utcnow()
        expired = (
            db.query(Recording)
            .filter(Recording.date_expires <= now, Recording.status == "ready")
            .all()
        )
        for rec in expired:
            _delete_files(rec.id)
            rec.status = "expired"
            rec.updated_at = datetime.utcnow()
        if expired:
            db.commit()
            logger.info("Retention: expired %d recording(s)", len(expired))
    except Exception:
        logger.exception("Retention sweep failed")
    finally:
        db.close()


@asynccontextmanager
async def _lifespan(app: FastAPI):
    init_db()
    _scheduler.add_job(
        _run_retention,
        "interval",
        seconds=config.RETENTION_CHECK_INTERVAL,
        id="retention",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("VOD server ready on %s:%d", config.SERVER_HOST, config.SERVER_PORT)
    yield
    _scheduler.shutdown(wait=False)


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="Local VOD System", version="1.0.0", lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_static_dir = config.BASE_DIR / "static"
app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _delete_files(recording_id: str):
    for d in [
        config.RECORDINGS_DIR / recording_id,
        config.LIBRARY_DIR / recording_id,
    ]:
        if d.exists():
            shutil.rmtree(d)
    thumb = config.THUMBNAILS_DIR / f"{recording_id}.jpg"
    if thumb.exists():
        thumb.unlink(missing_ok=True)


def _db_update(recording_id: str, **kwargs):
    """Thread-safe DB update used by background callbacks."""
    db = SessionLocal()
    try:
        rec = db.query(Recording).filter(Recording.id == recording_id).first()
        if rec:
            for k, v in kwargs.items():
                setattr(rec, k, v)
            rec.updated_at = datetime.utcnow()
            db.commit()
    except Exception:
        logger.exception("_db_update failed for %s", recording_id)
    finally:
        db.close()


def _to_response(rec: Recording) -> dict:
    return {
        "id": rec.id,
        "title": rec.title,
        "description": rec.description or "",
        "tags": json.loads(rec.tags or "[]"),
        "source_url": rec.source_url,
        "source_type": rec.source_type,
        "days_to_retain": rec.days_to_retain,
        "date_recorded": rec.date_recorded,
        "date_expires": rec.date_expires,
        "duration_seconds": rec.duration_seconds,
        "status": rec.status,
        "error_message": rec.error_message,
        "progress": rec.progress or 0,
        "resolutions_available": json.loads(rec.resolutions_json or "[]"),
        "file_size_bytes": rec.file_size_bytes or 0,
        "thumbnail_url": (
            f"/api/recordings/{rec.id}/thumbnail" if rec.thumbnail_path else None
        ),
        "dash_available": (config.LIBRARY_DIR / rec.id / "manifest.mpd").exists(),
        "created_at": rec.created_at,
    }


def _launch_transcode(recording_id: str, input_path: str, resolutions: List[str]):
    """Pull media info, kick off async transcode, update DB throughout."""
    duration = transcoder.get_duration(input_path)
    thumb = transcoder.extract_thumbnail(input_path, recording_id)
    updates: dict = {}
    if duration:
        updates["duration_seconds"] = duration
    if thumb:
        updates["thumbnail_path"] = thumb
    if updates:
        _db_update(recording_id, **updates)

    def _on_progress(pct: int):
        _db_update(recording_id, progress=pct)

    def _on_complete(done: List[str]):
        lib_dir = config.LIBRARY_DIR / recording_id
        size = sum(f.stat().st_size for f in lib_dir.rglob("*") if f.is_file())
        _db_update(
            recording_id,
            status="ready",
            progress=100,
            resolutions_json=json.dumps(done),
            file_size_bytes=size,
        )

    def _on_error(msg: str):
        _db_update(recording_id, status="error", error_message=msg, progress=0)

    transcoder.transcode_async(
        recording_id, input_path, resolutions, _on_progress, _on_complete, _on_error
    )


# ── Routes: UI ────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def ui_library():
    return FileResponse(str(_static_dir / "index.html"))


@app.get("/player/{recording_id}", include_in_schema=False)
async def ui_player(recording_id: str):
    return FileResponse(str(_static_dir / "player.html"))


# ── Routes: Recordings CRUD ───────────────────────────────────────────────────

@app.get("/api/recordings", response_model=None)
async def list_recordings(
    q: Optional[str] = Query(None, description="Search title, description, tags"),
    status: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    query = db.query(Recording)

    if q:
        like = f"%{q}%"
        query = query.filter(
            Recording.title.ilike(like)
            | Recording.description.ilike(like)
            | Recording.tags.ilike(like)
        )
    if status:
        query = query.filter(Recording.status == status)
    if date_from:
        query = query.filter(
            Recording.date_recorded >= datetime.fromisoformat(date_from)
        )
    if date_to:
        query = query.filter(
            Recording.date_recorded <= datetime.fromisoformat(date_to)
        )

    total = query.count()
    items = (
        query.order_by(Recording.date_recorded.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    return {"total": total, "items": [_to_response(r) for r in items]}


@app.get("/api/recordings/{recording_id}", response_model=None)
async def get_recording(recording_id: str, db: Session = Depends(get_db)):
    rec = db.query(Recording).filter(Recording.id == recording_id).first()
    if not rec:
        raise HTTPException(404, "Recording not found")
    return _to_response(rec)


@app.post("/api/recordings", status_code=201, response_model=None)
async def create_recording(body: RecordingCreate, db: Session = Depends(get_db)):
    """Create a metadata-only entry. Use /upload or /record to add content."""
    rec_id = str(uuid.uuid4())
    expires = datetime.utcnow() + timedelta(days=body.days_to_retain)
    rec = Recording(
        id=rec_id,
        title=body.title,
        description=body.description or "",
        tags=json.dumps(body.tags or []),
        source_url=body.source_url,
        source_type=body.source_type or "file",
        days_to_retain=body.days_to_retain,
        date_recorded=datetime.utcnow(),
        date_expires=expires,
        status="pending",
    )
    db.add(rec)
    db.commit()
    db.refresh(rec)
    return _to_response(rec)


@app.patch("/api/recordings/{recording_id}", response_model=None)
async def update_recording(
    recording_id: str, body: RecordingUpdate, db: Session = Depends(get_db)
):
    rec = db.query(Recording).filter(Recording.id == recording_id).first()
    if not rec:
        raise HTTPException(404, "Recording not found")

    if body.title is not None:
        rec.title = body.title
    if body.description is not None:
        rec.description = body.description
    if body.tags is not None:
        rec.tags = json.dumps(body.tags)
    if body.days_to_retain is not None:
        rec.days_to_retain = body.days_to_retain
        if rec.date_recorded:
            rec.date_expires = rec.date_recorded + timedelta(days=body.days_to_retain)

    rec.updated_at = datetime.utcnow()
    db.commit()
    return _to_response(rec)


@app.delete("/api/recordings/{recording_id}", status_code=204)
async def delete_recording(recording_id: str, db: Session = Depends(get_db)):
    rec = db.query(Recording).filter(Recording.id == recording_id).first()
    if not rec:
        raise HTTPException(404, "Recording not found")
    recorder.stop_recording(recording_id)
    _delete_files(recording_id)
    db.delete(rec)
    db.commit()


# ── Routes: Content ingestion ─────────────────────────────────────────────────

@app.post("/api/recordings/record", status_code=201, response_model=None)
async def start_live_record(
    body: StartRecordRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Create a new recording entry and immediately begin capturing from a live source.
    """
    rec_id = str(uuid.uuid4())
    now = datetime.utcnow()
    expires = now + timedelta(days=body.days_to_retain)
    resolutions = body.resolutions or config.DEFAULT_RESOLUTIONS

    rec = Recording(
        id=rec_id,
        title=body.title,
        description=body.description or "",
        tags=json.dumps(body.tags or []),
        source_url=body.source_url,
        source_type=body.source_type,
        days_to_retain=body.days_to_retain,
        date_recorded=now,
        date_expires=expires,
        status="recording",
    )
    db.add(rec)
    db.commit()

    try:
        pid = recorder.start_recording(
            rec_id, body.source_url, body.source_type, body.duration_seconds
        )
        rec.ffmpeg_pid = pid
        db.commit()
    except Exception as exc:
        rec.status = "error"
        rec.error_message = str(exc)
        db.commit()
        raise HTTPException(500, f"Failed to start FFmpeg: {exc}")

    if body.duration_seconds:
        # Auto-stop + transcode after the requested duration
        import time, threading

        def _auto_finish():
            time.sleep(body.duration_seconds + 5)
            recorder.stop_recording(rec_id)
            raw = recorder.recording_path(rec_id)
            if raw:
                _db_update(rec_id, status="transcoding", progress=0)
                _launch_transcode(rec_id, str(raw), resolutions)
            else:
                _db_update(rec_id, status="error", error_message="raw.ts not found")

        threading.Thread(target=_auto_finish, daemon=True).start()

    return _to_response(rec)


@app.post("/api/recordings/{recording_id}/stop", response_model=None)
async def stop_live_record(
    recording_id: str,
    resolutions: Optional[str] = Query(None, description="Comma-separated resolutions"),
    db: Session = Depends(get_db),
):
    """Stop an active live recording and immediately begin transcoding."""
    rec = db.query(Recording).filter(Recording.id == recording_id).first()
    if not rec:
        raise HTTPException(404, "Recording not found")
    if rec.status != "recording":
        raise HTTPException(400, f"Not currently recording (status={rec.status})")

    recorder.stop_recording(recording_id)

    raw = recorder.recording_path(recording_id)
    if not raw:
        rec.status = "error"
        rec.error_message = "No raw.ts found after stopping"
        db.commit()
        raise HTTPException(500, "Recording file missing")

    res_list = (
        [r.strip() for r in resolutions.split(",")]
        if resolutions
        else config.DEFAULT_RESOLUTIONS
    )
    rec.status = "transcoding"
    rec.progress = 0
    rec.updated_at = datetime.utcnow()
    db.commit()

    _launch_transcode(recording_id, str(raw), res_list)
    return _to_response(rec)


@app.post("/api/recordings/{recording_id}/upload", response_model=None)
async def upload_file(
    recording_id: str,
    file: UploadFile = File(...),
    resolutions: str = Form(default="1080p,720p,480p"),
    db: Session = Depends(get_db),
):
    """Upload a video file and transcode it into the VOD library."""
    rec = db.query(Recording).filter(Recording.id == recording_id).first()
    if not rec:
        raise HTTPException(404, "Recording not found")
    if rec.status not in ("pending", "error"):
        raise HTTPException(400, f"Cannot upload in status: {rec.status}")

    upload_dir = config.RECORDINGS_DIR / recording_id
    upload_dir.mkdir(parents=True, exist_ok=True)

    suffix = Path(file.filename or "video.mp4").suffix or ".mp4"
    raw_path = upload_dir / f"raw{suffix}"

    # Stream the upload to disk
    with open(raw_path, "wb") as fh:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            fh.write(chunk)

    rec.status = "transcoding"
    rec.source_type = "upload"
    rec.progress = 0
    rec.date_recorded = datetime.utcnow()
    rec.date_expires = rec.date_recorded + timedelta(days=rec.days_to_retain)
    rec.updated_at = datetime.utcnow()
    db.commit()

    res_list = [r.strip() for r in resolutions.split(",") if r.strip()]
    _launch_transcode(recording_id, str(raw_path), res_list)
    return _to_response(rec)


@app.post("/api/recordings/{recording_id}/transcode", response_model=None)
async def retranscode(
    recording_id: str,
    body: TranscodeRequest,
    db: Session = Depends(get_db),
):
    """Re-run transcoding on an existing raw file (e.g. to add resolutions)."""
    rec = db.query(Recording).filter(Recording.id == recording_id).first()
    if not rec:
        raise HTTPException(404, "Recording not found")

    raw_dir = config.RECORDINGS_DIR / recording_id
    raw = next(raw_dir.glob("raw.*"), None) if raw_dir.exists() else None
    if not raw:
        raise HTTPException(400, "No source file found — cannot transcode")

    res_list = body.resolutions or config.DEFAULT_RESOLUTIONS
    rec.status = "transcoding"
    rec.progress = 0
    rec.updated_at = datetime.utcnow()
    db.commit()

    _launch_transcode(recording_id, str(raw), res_list)
    return _to_response(rec)


# ── Routes: HLS streaming ─────────────────────────────────────────────────────

@app.get("/api/recordings/{recording_id}/stream/master.m3u8")
async def stream_master(recording_id: str):
    path = config.LIBRARY_DIR / recording_id / "master.m3u8"
    if not path.exists():
        raise HTTPException(404, "Stream not ready")
    return FileResponse(str(path), media_type="application/vnd.apple.mpegurl")


@app.get("/api/recordings/{recording_id}/stream/{resolution}/playlist.m3u8")
async def stream_playlist(recording_id: str, resolution: str):
    path = config.LIBRARY_DIR / recording_id / resolution / "playlist.m3u8"
    if not path.exists():
        raise HTTPException(404, "Playlist not found")
    return FileResponse(str(path), media_type="application/vnd.apple.mpegurl")


@app.get("/api/recordings/{recording_id}/stream/{resolution}/{segment}")
async def stream_segment(recording_id: str, resolution: str, segment: str):
    if not segment.endswith(".ts"):
        raise HTTPException(400, "Only .ts segments are served here")
    path = config.LIBRARY_DIR / recording_id / resolution / segment
    if not path.exists():
        raise HTTPException(404, "Segment not found")
    return FileResponse(str(path), media_type="video/MP2T")


# ── DASH streaming ────────────────────────────────────────────────────────────

@app.get("/api/recordings/{recording_id}/stream/manifest.mpd")
async def stream_dash_manifest(recording_id: str):
    path = config.LIBRARY_DIR / recording_id / "manifest.mpd"
    if not path.exists():
        raise HTTPException(404, "DASH manifest not ready")
    return FileResponse(str(path), media_type="application/dash+xml")


@app.get("/api/recordings/{recording_id}/stream/dash/{filename}")
async def stream_dash_segment(recording_id: str, filename: str):
    """Serve DASH init segments (.mp4) and media segments (.m4s)."""
    if "/" in filename or ".." in filename:
        raise HTTPException(400, "Invalid filename")
    if not (filename.endswith(".mp4") or filename.endswith(".m4s")):
        raise HTTPException(400, "Only .mp4 init and .m4s media segments are served here")
    path = config.LIBRARY_DIR / recording_id / "dash" / filename
    if not path.exists():
        raise HTTPException(404, "Segment not found")
    media_type = "video/mp4" if filename.endswith(".mp4") else "video/iso.segment"
    return FileResponse(str(path), media_type=media_type)


@app.get("/api/recordings/{recording_id}/thumbnail")
async def get_thumbnail(recording_id: str, db: Session = Depends(get_db)):
    rec = db.query(Recording).filter(Recording.id == recording_id).first()
    if not rec or not rec.thumbnail_path:
        raise HTTPException(404, "No thumbnail available")
    p = Path(rec.thumbnail_path)
    if not p.exists():
        raise HTTPException(404, "Thumbnail file missing")
    return FileResponse(str(p), media_type="image/jpeg")


# ── Routes: System ────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats(db: Session = Depends(get_db)):
    counts = {}
    for status in ("ready", "recording", "transcoding", "error", "expired", "pending"):
        counts[status] = (
            db.query(Recording).filter(Recording.status == status).count()
        )
    total = db.query(Recording).count()
    size = db.query(func.sum(Recording.file_size_bytes)).scalar() or 0
    return {
        "total_recordings": total,
        **counts,
        "total_size_bytes": int(size),
    }


@app.get("/api/resolutions")
async def get_resolutions():
    return [
        {"name": p["name"], "width": p["width"], "height": p["height"]}
        for p in config.RESOLUTION_PROFILES
    ]


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server:app",
        host=config.SERVER_HOST,
        port=config.SERVER_PORT,
        reload=False,
        log_level="info",
    )
