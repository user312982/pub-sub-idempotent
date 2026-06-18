import asyncio
import time
from datetime import datetime, timezone

import pytest
from src.models import Event as EventModel

@pytest.mark.asyncio
async def test_stress_20000_events_with_duplicates(db_deps, consumer):
    store, st = db_deps
    
    total_unique = 20000
    duplicate_pct = 0.30
    total_duplicates = int(total_unique * duplicate_pct)

    events = []
    for i in range(total_unique):
        events.append({
            "topic": "stress",
            "event_id": f"str-{i:06d}",
            "timestamp": datetime.now(timezone.utc),
            "source": "stress-test",
            "payload": {"seq": i},
        })

    for _ in range(total_duplicates):
        events.append(events[0])

    import random
    random.shuffle(events)

    start = time.time()

    # Batch publish to speed up test execution
    batch_size = 500
    for i in range(0, len(events), batch_size):
        batch = events[i:i+batch_size]
        for raw in batch:
            await consumer.publish(EventModel(**raw))
            
    # Wait for consumer to process everything
    # Polling DB until count matches or timeout
    max_wait = 120 # seconds
    start_wait = time.time()
    while time.time() - start_wait < max_wait:
        recv = await st.get_received()
        if recv == total_unique + total_duplicates:
            # Add small buffer for DB writes to complete after redis ack
            await asyncio.sleep(2)
            break
        await asyncio.sleep(0.5)

    elapsed = time.time() - start

    unique = await store.count_unique_processed()
    recv = await st.get_received()
    dup_count = await st.get_duplicate()

    assert unique == total_unique
    assert recv == total_unique + total_duplicates
    assert dup_count == total_duplicates
    assert elapsed < 120.0, f"Stress test took {elapsed:.2f}s, exceeding 120s limit"
