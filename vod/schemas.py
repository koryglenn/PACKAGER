from __future__ import annotations
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class RecordingCreate(BaseModel):
    title: str
    description: Optional[str] = ""
    tags: Optional[List[str]] = []
    days_to_retain: int = Field(default=7, ge=1, le=3650)
    source_url: Optional[str] = None
    source_type: Optional[str] = "file"


class RecordingUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    days_to_retain: Optional[int] = Field(default=None, ge=1, le=3650)


class RecordingResponse(BaseModel):
    id: str
    title: str
    description: str
    tags: List[str]
    source_url: Optional[str]
    source_type: Optional[str]
    days_to_retain: int
    date_recorded: Optional[datetime]
    date_expires: Optional[datetime]
    duration_seconds: Optional[float]
    status: str
    error_message: Optional[str]
    progress: int
    resolutions_available: List[str]
    file_size_bytes: int
    thumbnail_url: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class StartRecordRequest(BaseModel):
    title: str
    description: Optional[str] = ""
    tags: Optional[List[str]] = []
    days_to_retain: int = Field(default=7, ge=1)
    source_url: str
    source_type: str = "rtsp"              # rtsp | hls | udp
    duration_seconds: Optional[int] = None  # None = record until /stop
    resolutions: Optional[List[str]] = None


class TranscodeRequest(BaseModel):
    resolutions: Optional[List[str]] = None


class StatsResponse(BaseModel):
    total_recordings: int
    ready: int
    recording: int
    transcoding: int
    error: int
    expired: int
    total_size_bytes: int
