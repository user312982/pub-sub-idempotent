import asyncio
import json
import logging
from typing import Optional

import redis.asyncio as redis

from src.models import Event
from src.dedup_store import DedupStore
from src.stats import Stats

logger = logging.getLogger(__name__)

STREAM_KEY = "events_stream"
GROUP_NAME = "aggregator_group"


class Consumer:
    def __init__(
        self,
        dedup_store: DedupStore,
        stats: Stats,
        redis_url: str = "redis://localhost:6379",
        worker_count: int = 1,
    ):
        self.dedup_store = dedup_store
        self.stats = stats
        self.redis_url = redis_url
        self.worker_count = worker_count
        self.redis: Optional[redis.Redis] = None
        self._tasks: list[asyncio.Task] = []
        self._stop_event = asyncio.Event()

    async def connect(self):
        if not self.redis:
            self.redis = redis.from_url(self.redis_url)
            # Create stream and group if they don't exist
            try:
                await self.redis.xgroup_create(STREAM_KEY, GROUP_NAME, mkstream=True)
                logger.info(f"Created Redis stream '{STREAM_KEY}' and group '{GROUP_NAME}'")
            except redis.exceptions.ResponseError as e:
                if "BUSYGROUP Consumer Group name already exists" not in str(e):
                    raise

    async def start(self):
        await self.connect()
        self._stop_event.clear()
        
        for i in range(self.worker_count):
            task = asyncio.create_task(self._consumer_loop(f"worker-{i}"))
            self._tasks.append(task)
            
        logger.info(f"Started {self.worker_count} consumer workers")

    async def stop(self):
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        
        if self.redis:
            await self.redis.close()
            self.redis = None

    async def publish(self, event: Event):
        # We now publish directly to redis stream instead of local queue
        await self.connect()
        payload = {"data": json.dumps(event.model_dump(mode="json"))}
        await self.redis.xadd(STREAM_KEY, payload)
        await self.stats.increment_received()

    async def _consumer_loop(self, worker_name: str):
        logger.info(f"Worker {worker_name} started")
        while not self._stop_event.is_set():
            try:
                # Block for 1 second waiting for new events
                streams = await self.redis.xreadgroup(
                    GROUP_NAME, worker_name, {STREAM_KEY: ">"}, count=10, block=1000
                )
                
                if not streams:
                    continue
                    
                for stream_name, messages in streams:
                    for message_id, message_data in messages:
                        try:
                            # Parse event
                            raw_data = message_data.get(b"data") or message_data.get("data")
                            if raw_data:
                                event_dict = json.loads(raw_data)
                                event = Event(**event_dict)
                                
                                # Process with dedup
                                is_new = await self.dedup_store.store_event(event)
                                if is_new:
                                    logger.debug(
                                        "[%s] Processed event: topic=%s event_id=%s",
                                        worker_name, event.topic, event.event_id,
                                    )
                                else:
                                    logger.warning(
                                        "[%s] Duplicate event dropped: topic=%s event_id=%s source=%s",
                                        worker_name, event.topic, event.event_id, event.source,
                                    )
                                    await self.stats.increment_duplicate()
                                    
                            # Always ACK to remove from pending list
                            await self.redis.xack(STREAM_KEY, GROUP_NAME, message_id)
                        except Exception as e:
                            logger.error("[%s] Error processing message %s: %s", worker_name, message_id, e)
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[%s] Consumer error: %s", worker_name, e)
                await asyncio.sleep(1) # Prevent tight loop on error
