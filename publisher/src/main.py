import logging
import os
from contextlib import asynccontextmanager
from typing import Optional, Union

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from src.models import Event, PublishResponse, StatsResponse
from src.dedup_store import DedupStore
from src.stats import Stats
from src.consumer import Consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def create_app(
    dedup_store: Optional[DedupStore] = None,
    stats: Optional[Stats] = None,
    consumer: Optional[Consumer] = None,
):
    # Default config from environment or use test defaults
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    BROKER_URL = os.getenv("BROKER_URL", "redis://localhost:6379")
    WORKER_COUNT = int(os.getenv("WORKER_COUNT", "4"))

    if dedup_store is None:
        dedup_store = DedupStore(DATABASE_URL)
    if stats is None:
        stats = Stats(DATABASE_URL)
    if consumer is None:
        consumer = Consumer(dedup_store, stats, BROKER_URL, WORKER_COUNT)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Connect to DB and start consumers
        await dedup_store.connect()
        await stats.connect()
        await consumer.start()
        logger.info("Aggregator started")
        yield
        # Stop everything
        await consumer.stop()
        await stats.close()
        await dedup_store.close()
        logger.info("Aggregator stopped")

    app = FastAPI(title="Event Aggregator", lifespan=lifespan)

    @app.get("/health")
    async def health_check():
        return JSONResponse({"status": "ok"})

    @app.post("/publish", response_model=PublishResponse)
    async def publish(events: Union[list[Event], Event]):
        if not isinstance(events, list):
            events = [events]
        for event in events:
            await consumer.publish(event)
        return PublishResponse(received=len(events))

    @app.get("/events")
    async def get_events(topic: Optional[str] = None):
        return await dedup_store.get_events(topic)

    @app.get("/stats", response_model=StatsResponse)
    async def get_stats():
        received = await stats.get_received()
        dup_dropped = await stats.get_duplicate()
        unique = await dedup_store.count_unique_processed()
        topics = await dedup_store.get_topics()
        uptime = await stats.get_uptime()
        return StatsResponse(
            received=received,
            unique_processed=unique,
            duplicate_dropped=dup_dropped,
            topics=topics,
            uptime=uptime,
        )

    return app


app = create_app()
