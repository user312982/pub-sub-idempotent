import asyncio
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from src.dedup_store import DedupStore
from src.stats import Stats
from src.consumer import Consumer
from src.main import create_app

DB_URL = "postgresql://user:pass@localhost:5432/pubsub"
REDIS_URL = "redis://localhost:6379"

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="function")
async def db_deps():
    # Setup
    dedup_store = DedupStore(DB_URL)
    stats = Stats(DB_URL)
    
    await dedup_store.connect()
    await stats.connect()
    
    # Clear data before test
    await dedup_store.clear()
    await stats.reset()
    
    yield dedup_store, stats
    
    # Teardown
    await dedup_store.clear()
    await dedup_store.close()
    await stats.close()

@pytest_asyncio.fixture(scope="function")
async def consumer(db_deps):
    dedup_store, stats = db_deps
    consumer = Consumer(dedup_store, stats, REDIS_URL, worker_count=4)
    
    # Clean stream
    await consumer.connect()
    try:
        await consumer.redis.xtrim("events_stream", maxlen=0)
    except:
        pass
        
    await consumer.start()
    yield consumer
    await consumer.stop()

@pytest_asyncio.fixture(scope="function")
async def app(db_deps, consumer):
    dedup_store, stats = db_deps
    return create_app(dedup_store=dedup_store, stats=stats, consumer=consumer)

@pytest_asyncio.fixture(scope="function")
async def client(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
