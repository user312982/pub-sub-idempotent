from datetime import datetime, timezone
import pytest

@pytest.mark.asyncio
class TestSchema:
    async def test_valid_event_schema(self, client):
        event = {
            "topic": "orders",
            "event_id": "ord-001",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "order-service",
            "payload": {"order_id": 123},
        }
        resp = await client.post("/publish", json=event)
        assert resp.status_code == 200
        data = resp.json()
        assert data["received"] == 1
        assert data["status"] == "ok"

    async def test_missing_topic_field(self, client):
        event = {
            "event_id": "evt-001",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test",
            "payload": {},
        }
        resp = await client.post("/publish", json=event)
        assert resp.status_code == 422

    async def test_missing_event_id_field(self, client):
        event = {
            "topic": "test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test",
            "payload": {},
        }
        resp = await client.post("/publish", json=event)
        assert resp.status_code == 422

    async def test_invalid_timestamp_format(self, client):
        event = {
            "topic": "test",
            "event_id": "evt-001",
            "timestamp": "not-a-timestamp",
            "source": "test",
            "payload": {},
        }
        resp = await client.post("/publish", json=event)
        assert resp.status_code == 422

    async def test_empty_body(self, client):
        resp = await client.post("/publish", json={})
        assert resp.status_code == 422

    async def test_batch_publish(self, client):
        events = [
            {
                "topic": "orders",
                "event_id": f"ord-{i:03d}",
                "timestamp": "2024-01-01T00:00:00Z",
                "source": "svc",
                "payload": {"seq": i},
            }
            for i in range(10)
        ]
        resp = await client.post("/publish", json=events)
        assert resp.status_code == 200
        assert resp.json()["received"] == 10
