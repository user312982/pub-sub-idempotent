from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field


class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: dict[str, Any] = Field(default_factory=dict)


class PublishResponse(BaseModel):
    received: int
    status: str = "ok"


class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: list[str]
    uptime: float
