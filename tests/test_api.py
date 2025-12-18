import pytest
import uuid
from datetime import datetime
import asyncio

# Test 4: Endpoint GET /events harus list (awalnya mungkin kosong/berisi)
@pytest.mark.asyncio
async def test_get_events_structure(client):
    resp = await client.get("/events")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)

# Test 5: Filter events by topic
@pytest.mark.asyncio
async def test_get_events_filter(client):
    # Publish spesifik topic
    unique_topic = f"topic.{uuid.uuid4()}"
    payload = {
        "topic": unique_topic,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "source": "pytest",
        "payload": {}
    }
    await client.post("/publish", json=payload)
    
    # Tunggu worker memproses (async nature)
    await asyncio.sleep(1)
    
    # Filter
    resp = await client.get(f"/events?topic={unique_topic}")
    data = resp.json()
    assert len(data) >= 1
    assert data[0]["topic"] == unique_topic

# Test 6: Endpoint GET /stats struktur data
@pytest.mark.asyncio
async def test_stats_structure(client):
    resp = await client.get("/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "received" in data
    assert "unique_processed" in data
    assert "duplicate_dropped" in data
    assert "topics" in data

# Test 7: Stats increment check (sanity check)
@pytest.mark.asyncio
async def test_stats_increment(client):
    # Ambil stats awal
    initial = (await client.get("/stats")).json()
    
    # Kirim 1 event
    payload = {
        "topic": "test.stats",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "source": "pytest",
        "payload": {}
    }
    await client.post("/publish", json=payload)
    
    # Cek stats lagi
    final = (await client.get("/stats")).json()
    
    # Received harus naik
    assert final["received"] > initial["received"]