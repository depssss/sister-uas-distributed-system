import pytest
import uuid
from datetime import datetime

# Test 1: Pastikan payload valid diterima
@pytest.mark.asyncio
async def test_valid_payload(client):
    payload = {
        "topic": "test.schema",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "source": "pytest",
        "payload": {"data": "ok"}
    }
    resp = await client.post("/publish", json=payload)
    assert resp.status_code == 200
    assert resp.json()["status"] == "queued"

# Test 2: Pastikan reject jika field wajib hilang (misal: event_id)
@pytest.mark.asyncio
async def test_missing_field(client):
    payload = {
        "topic": "test.schema",
        # "event_id": MISSING,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "pytest",
        "payload": {}
    }
    resp = await client.post("/publish", json=payload)
    # FastAPI/Pydantic akan return 422 Unprocessable Entity
    assert resp.status_code == 422 

# Test 3: Pastikan reject jika format timestamp salah
@pytest.mark.asyncio
async def test_invalid_timestamp(client):
    payload = {
        "topic": "test.schema",
        "event_id": str(uuid.uuid4()),
        "timestamp": "bukan-tanggal",
        "source": "pytest",
        "payload": {}
    }
    resp = await client.post("/publish", json=payload)
    assert resp.status_code == 422