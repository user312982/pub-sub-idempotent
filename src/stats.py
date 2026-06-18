import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


class Stats:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=5)
            await self._init_db()

    async def _init_db(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS event_stats (
                    id INT PRIMARY KEY DEFAULT 1,
                    received BIGINT NOT NULL DEFAULT 0,
                    duplicate_dropped BIGINT NOT NULL DEFAULT 0,
                    start_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            # Initialize if not exists
            await conn.execute("""
                INSERT INTO event_stats (id, received, duplicate_dropped)
                VALUES (1, 0, 0)
                ON CONFLICT (id) DO NOTHING
            """)

    async def increment_received(self, count: int = 1):
        # Transaksional update, mencegah lost-update bila ada concurrent requests
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE event_stats SET received = received + $1 WHERE id = 1",
                count
            )

    async def increment_duplicate(self, count: int = 1):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE event_stats SET duplicate_dropped = duplicate_dropped + $1 WHERE id = 1",
                count
            )

    async def get_received(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT received FROM event_stats WHERE id = 1")

    async def get_duplicate(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT duplicate_dropped FROM event_stats WHERE id = 1")

    async def get_uptime(self) -> float:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT EXTRACT(EPOCH FROM (NOW() - start_time)) as uptime FROM event_stats WHERE id = 1")
            return float(row["uptime"]) if row else 0.0

    async def reset(self):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE event_stats SET received = 0, duplicate_dropped = 0, start_time = NOW() WHERE id = 1"
            )

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
