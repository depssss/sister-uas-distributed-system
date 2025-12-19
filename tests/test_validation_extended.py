import pytest
import uuid
from datetime import datetime

@pytest.mark.asyncio
async def test_empty_topic(client):
    """Test jika topic dikirim string kosong (harusnya gagal atau boleh tergantung aturan, 
    di sini kita anggap boleh tapi response tetap 200/queued karena Pydantic str min_length default 0)"""
    payload = {
        "topic": "",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "source": "pytest",
        "payload": {}
    }
    resp = await client.post("/publish", json=payload)
    assert resp.status_code == 200 # Kecuali kamu set min_length di main.py

@pytest.mark.asyncio
async def test_null_payload(client):
    """Test jika field 'payload' dikirim null"""
    payload = {
        "topic": "test",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "source": "pytest",
        "payload": None # Error karena model minta dict
    }
    resp = await client.post("/publish", json=payload)
    assert resp.status_code == 422

@pytest.mark.asyncio
async def test_missing_event_id(client):
    """Test jika event_id tidak dikirim sama sekali"""
    payload = {
        "topic": "test",
        # event_id HILANG
        "timestamp": datetime.now().isoformat(),
        "source": "pytest",
        "payload": {}
    }
    resp = await client.post("/publish", json=payload)
    assert resp.status_code == 422

@pytest.mark.asyncio
async def test_huge_payload(client):
    """Test kirim data payload sangat besar"""
    huge_data = {"data": "x" * 10000} # 10KB string
    payload = {
        "topic": "load-test",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "source": "pytest",
        "payload": huge_data
    }
    resp = await client.post("/publish", json=payload)
    assert resp.status_code == 200

@pytest.mark.asyncio
async def test_wrong_data_type(client):
    """Test kirim integer ke field yang harusnya string (source)"""
    payload = {
        "topic": "test",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "source": 12345, # Harusnya string, Pydantic akan coba convert
        "payload": {}
    }
    # Pydantic default mode biasanya auto-convert int ke string, jadi ini bisa 200
    resp = await client.post("/publish", json=payload)
    assert resp.status_code == 422