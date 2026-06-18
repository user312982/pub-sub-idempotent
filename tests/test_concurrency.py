import asyncio
import time
from datetime import datetime, timezone
import pytest

from src.models import Event

@pytest.mark.asyncio
async def test_race_condition_unique_constraint(db_deps, consumer):
    """
    Menguji bahwa insert concurrent dari event yang sama 
    hanya akan berhasil satu kali.
    """
    store, stats = db_deps
    
    event = Event(
        topic="race",
        event_id="race-001",
        timestamp=datetime.now(timezone.utc),
        source="test",
        payload={}
    )
    
    # Simulate 50 concurrent inserts of the same event directly to DB
    tasks = [store.store_event(event) for _ in range(50)]
    results = await asyncio.gather(*tasks)
    
    # Hanya satu yang harusnya True (berhasil insert)
    assert results.count(True) == 1
    # Sisanya harusnya False (di-ignore karena duplicate constraint)
    assert results.count(False) == 49
    
    # Di database hanya boleh ada 1
    assert await store.count_unique_processed() == 1


@pytest.mark.asyncio
async def test_isolation_level_no_lost_update(db_deps):
    """
    Menguji bahwa update stat.received tidak mengalami lost-update 
    meski dipanggil concurrent banyak kali.
    """
    store, stats = db_deps
    
    # Simulate 100 concurrent stats increment
    tasks = [stats.increment_received(1) for _ in range(100)]
    await asyncio.gather(*tasks)
    
    # Nilai akhir harus tepat 100
    assert await stats.get_received() == 100
