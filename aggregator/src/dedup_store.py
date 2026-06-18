import json
import logging
from datetime import datetime, timezone
from typing import Optional

import asyncpg

from src.models import Event

logger = logging.getLogger(__name__)


class DedupStore:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=10)
            await self._init_db()

    async def _init_db(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    topic VARCHAR(255) NOT NULL,
                    event_id VARCHAR(255) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    source VARCHAR(255) NOT NULL,
                    payload JSONB DEFAULT '{}'::jsonb,
                    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (topic, event_id)
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_topic
                ON events(topic)
            """)

    async def store_event(self, event: Event) -> bool:
        """
        Stores event if it doesn't exist. Returns True if inserted, False if duplicate.
        Uses INSERT ... ON CONFLICT DO NOTHING to ensure idempotency and atomic dedup.
        """
        async with self.pool.acquire() as conn:
            # ON CONFLICT DO NOTHING makes this operation idempotent and thread/worker-safe.
            # If two workers try to insert the exact same event at the exact same time,
            # Postgres will grant the row to one and return no row to the other.
            result = await conn.execute(
                """
                INSERT INTO events (topic, event_id, timestamp, source, payload, processed_at)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                ON CONFLICT (topic, event_id) DO NOTHING
                """,
                event.topic,
                event.event_id,
                event.timestamp,
                event.source,
                json.dumps(event.payload),
                datetime.now(timezone.utc)
            )
            # result string will be "INSERT 0 1" if successful, or "INSERT 0 0" if duplicate
            return result == "INSERT 0 1"

    async def count_unique_processed(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM events")

    async def get_topics(self) -> list[str]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT DISTINCT topic FROM events")
            return [row["topic"] for row in rows]

    async def get_events(self, topic: Optional[str] = None) -> list[Event]:
        async with self.pool.acquire() as conn:
            if topic:
                rows = await conn.fetch(
                    "SELECT topic, event_id, timestamp, source, payload FROM events WHERE topic=$1 ORDER BY processed_at",
                    topic
                )
            else:
                rows = await conn.fetch(
                    "SELECT topic, event_id, timestamp, source, payload FROM events ORDER BY processed_at"
                )
            
            events = []
            for row in rows:
                events.append(Event(
                    topic=row["topic"],
                    event_id=row["event_id"],
                    timestamp=row["timestamp"],
                    source=row["source"],
                    payload=json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"],
                ))
            return events

    async def clear(self):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM events")

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
