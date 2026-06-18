import pytest
import pytest_asyncio
from datetime import datetime, timezone
from src.dedup_store import DedupStore
from src.models import Event

DB_URL = "postgresql://postgres:postgres@localhost:5432/postgres"

@pytest.mark.asyncio
async def test_dedup_persistence_across_reconnect():
    """
    Menguji bahwa setelah koneksi baru dibuat ke DB yang sama,
    event lama tetap ter-reject (tidak diproses ulang).
    Ini membuktikan crash recovery: dedup_store tetap konsisten
    meski aggregator restart.
    """
    event = Event(
        topic="persist-test",
        event_id="persist-pg-001",
        timestamp=datetime.now(timezone.utc),
        source="svc",
        payload={"msg": "hello-postgres"},
    )

    # Koneksi pertama — simpan event
    store1 = DedupStore(DB_URL)
    await store1.connect()
    # Bersihkan dulu kalau ada dari run sebelumnya
    await store1.clear()

    result1 = await store1.store_event(event)
    assert result1 is True  # sukses disimpan

    result2 = await store1.store_event(event)
    assert result2 is False  # duplikat di-reject

    count1 = await store1.count_unique_processed()
    assert count1 == 1
    await store1.close()

    # Simulasi "restart" — koneksi baru ke DB yang sama
    store2 = DedupStore(DB_URL)
    await store2.connect()

    # Event yang sama harus TETAP ditolak oleh DB
    result3 = await store2.store_event(event)
    assert result3 is False  # masih duplikat meski koneksi baru

    count2 = await store2.count_unique_processed()
    assert count2 == 1  # jumlah tetap 1

    await store2.clear()
    await store2.close()
