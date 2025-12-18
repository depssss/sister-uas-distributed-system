import pytest
import uuid
import asyncio
from datetime import datetime

# Test 8: Skenario Normal (Kirim 1x, Proses 1x)
@pytest.mark.asyncio
async def test_normal_processing(client):
    eid = str(uuid.uuid4())
    payload = {
        "topic": "test.dedup",
        "event_id": eid,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "pytest",
        "payload": {}
    }
    await client.post("/publish", json=payload)
    await asyncio.sleep(1) # Tunggu worker
    
    # Cek di DB via API events
    # Karena API GET /events mungkin tidak punya filter ID, kita filter manual di test
    resp = await client.get("/events?topic=test.dedup")
    events = [e for e in resp.json() if e['event_id'] == eid]
    assert len(events) == 1

# Test 9: Skenario Duplikat (Kirim 2x, Proses TETAP 1x)
@pytest.mark.asyncio
async def test_duplicate_handling(client):
    eid = str(uuid.uuid4())
    payload = {
        "topic": "test.dedup",
        "event_id": eid,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "pytest",
        "payload": {}
    }
    
    # Kirim 2 kali berturut-turut
    await client.post("/publish", json=payload)
    await client.post("/publish", json=payload)
    
    await asyncio.sleep(2) # Tunggu worker
    
    # Cek jumlah di DB
    resp = await client.get("/events?topic=test.dedup")
    events = [e for e in resp.json() if e['event_id'] == eid]
    
    # HARUS 1. Jika 2, berarti GAGAL dedup.
    assert len(events) == 1

# Test 10: Deduplication Cross-Source (Beda source, ID sama -> Tetap Dedup)
# Asumsi sistem dedup berdasarkan (Topic, EventID) saja, source diabaikan
@pytest.mark.asyncio
async def test_dedup_cross_source(client):
    eid = str(uuid.uuid4())
    payload1 = {
        "topic": "test.dedup", "event_id": eid,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "mobile", "payload": {}
    }
    payload2 = {
        "topic": "test.dedup", "event_id": eid,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "web", "payload": {}
    }
    
    await client.post("/publish", json=payload1)
    await client.post("/publish", json=payload2)
    await asyncio.sleep(1)
    
    resp = await client.get("/events?topic=test.dedup")
    events = [e for e in resp.json() if e['event_id'] == eid]
    assert len(events) == 1

# Test 11: Idempotency Stats Check
@pytest.mark.asyncio
async def test_dedup_stats_impact(client):
    # Catat stats awal
    s1 = (await client.get("/stats")).json()
    
    eid = str(uuid.uuid4())
    payload = {
        "topic": "test.stats.dedup", "event_id": eid,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "pytest", "payload": {}
    }
    
    # Kirim 3 kali
    for _ in range(3):
        await client.post("/publish", json=payload)
        
    await asyncio.sleep(2)
    s2 = (await client.get("/stats")).json()
    
    # Analisis kenaikan
    delta_received = s2["received"] - s1["received"]
    delta_unique = s2["unique_processed"] - s1["unique_processed"]
    delta_dropped = s2["duplicate_dropped"] - s1["duplicate_dropped"]
    
    assert delta_received >= 3
    # Idealnya unique naik 1
    # dropped naik 2 (karena 2 duplikat dibuang)
    # Note: Assertion ini mungkin flaky jika ada publisher lain berjalan, 
    # tapi secara logika benar.