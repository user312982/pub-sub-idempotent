import asyncio
from datetime import datetime, timezone
import pytest


@pytest.mark.asyncio
class TestAPI:
    async def test_events_endpoint_returns_processed_events(self, client):
        event = {
            "topic": "orders",
            "event_id": "ord-001",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "svc",
            "payload": {"order": 1},
        }
        await client.post("/publish", json=event)
        await client.post("/publish", json=event)
        
        await asyncio.sleep(0.5) # Wait for consumer to process

        resp = await client.get("/events?topic=orders")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["event_id"] == "ord-001"

    async def test_events_endpoint_all_topics(self, client):
        for i in range(5):
            await client.post("/publish", json={
                "topic": f"topic-{i}",
                "event_id": f"evt-{i:03d}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "svc",
                "payload": {"i": i},
            })
            
        await asyncio.sleep(0.5)

        resp = await client.get("/events")
        assert resp.status_code == 200
        assert len(resp.json()) == 5

    async def test_stats_consistency_after_publish(self, client):
        events = [
            {"topic": "t1", "event_id": "e1", "timestamp": datetime.now(timezone.utc).isoformat(), "source": "s", "payload": {}},
            {"topic": "t1", "event_id": "e2", "timestamp": datetime.now(timezone.utc).isoformat(), "source": "s", "payload": {}},
            {"topic": "t2", "event_id": "e3", "timestamp": datetime.now(timezone.utc).isoformat(), "source": "s", "payload": {}},
        ]
        for ev in events:
            await client.post("/publish", json=ev)
            await client.post("/publish", json=ev)
            
        await asyncio.sleep(0.5)

        resp = await client.get("/stats")
        data = resp.json()
        assert data["received"] == 6
        assert data["unique_processed"] == 3
        assert data["duplicate_dropped"] == 3
        assert "t1" in data["topics"]
        assert "t2" in data["topics"]
        assert data["uptime"] > 0

    async def test_stats_empty_initial(self, client):
        resp = await client.get("/stats")
        data = resp.json()
        assert data["received"] == 0
        assert data["unique_processed"] == 0
        assert data["duplicate_dropped"] == 0
        assert data["topics"] == []
        assert data["uptime"] >= 0

    async def test_health_endpoint(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
