import uuid
from datetime import datetime

from sqlalchemy import Column, String, Integer, Float, DateTime, Text
from database import Base


def _new_id():
    return str(uuid.uuid4())


class Recording(Base):
    __tablename__ = "recordings"

    id = Column(String(36), primary_key=True, default=_new_id)

    # --- identity ---
    title = Column(String(255), nullable=False, index=True)
    description = Column(Text, default="")
    tags = Column(Text, default="[]")          # JSON-encoded list[str]

    # --- source ---
    source_url = Column(String(1024))
    source_type = Column(String(20))           # rtsp | hls | udp | file | upload

    # --- timing ---
    date_recorded = Column(DateTime, default=datetime.utcnow, index=True)
    date_expires = Column(DateTime, index=True)
    days_to_retain = Column(Integer, default=7)
    duration_seconds = Column(Float)

    # --- state machine ---
    # pending → recording → transcoding → ready
    #                              └──────────────→ error
    # ready → expired  (by retention job)
    status = Column(String(20), default="pending", index=True)
    error_message = Column(Text)
    progress = Column(Integer, default=0)      # 0-100 during transcoding

    # --- content ---
    resolutions_json = Column(Text, default="[]")  # JSON list of ready resolutions
    file_size_bytes = Column(Integer, default=0)
    thumbnail_path = Column(String(512))

    # --- runtime ---
    ffmpeg_pid = Column(Integer)

    # --- audit ---
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
