import asyncio
from datetime import datetime, timezone

import pytest

from src.models import Event

@pytest.mark.asyncio
async def test_dedup_rejects_duplicate(db_deps):
    store, stats = db_deps
    event = Event(
        topic="test",
        event_id="dup-001",
        timestamp=datetime.now(timezone.utc),
        source="svc",
        payload={"data": 1},
    )
    assert await store.store_event(event) is True
    assert await store.store_event(event) is False
    assert await store.count_unique_processed() == 1


@pytest.mark.asyncio
async def test_same_event_id_different_topics(db_deps):
    store, stats = db_deps
    event_a = Event(
        topic="orders", event_id="same-id",
        timestamp=datetime.now(timezone.utc), source="svc", payload={},
    )
    event_b = Event(
        topic="payments", event_id="same-id",
        timestamp=datetime.now(timezone.utc), source="svc", payload={},
    )
    assert await store.store_event(event_a) is True
    assert await store.store_event(event_b) is True
    assert await store.count_unique_processed() == 2


@pytest.mark.asyncio
async def test_consumer_dedup_integration(db_deps, consumer):
    store, stats = db_deps
    event = Event(
        topic="test", event_id="int-001",
        timestamp=datetime.now(timezone.utc), source="svc", payload={},
    )
    await consumer.publish(event)
    await consumer.publish(event)
    
    # Wait for consumer to process message from Redis stream
    await asyncio.sleep(0.5)

    assert await store.count_unique_processed() == 1
    assert await stats.get_received() == 2
    assert await stats.get_duplicate() == 1
