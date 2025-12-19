import pytest

@pytest.mark.asyncio
async def test_health_check(client):
    # Test endpoint stats (sebagai health check sederhana)
    response = await client.get("/stats")
    assert response.status_code == 200
    assert "unique_processed" in response.json()

@pytest.mark.asyncio
async def test_publish_valid_event(client):
    payload = {
        "topic": "test-topic",
        "event_id": "test-id-001",
        "timestamp": "2024-01-01T10:00:00",
        "source": "pytest",
        "payload": {"key": "value"}
    }
    response = await client.post("/publish", json=payload)
    assert response.status_code == 200
    assert response.json()["status"] == "queued"