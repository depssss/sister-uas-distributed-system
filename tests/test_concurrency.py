import pytest
import uuid
import asyncio
from datetime import datetime

# Test 12: Race Condition (Burst Request ID Sama)
@pytest.mark.asyncio
async def test_race_condition(client):
    """
    Mengirim 10 request dengan ID SAMA secara PARALEL (async gather).
    Tujuannya memaksa worker/DB crash jika locking tidak benar.
    """
    eid = str(uuid.uuid4())
    payload = {
        "topic": "test.race",
        "event_id": eid,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "pytest-race",
        "payload": {}
    }
    
    # Siapkan 10 task pengiriman bersamaan
    tasks = [client.post("/publish", json=payload) for _ in range(10)]
    
    # Jalankan "serentak"
    responses = await asyncio.gather(*tasks)
    
    # Pastikan semua diterima gateway (200 OK)
    for r in responses:
        assert r.status_code == 200
        
    await asyncio.sleep(3) # Tunggu proses
    
    # Verifikasi DB
    resp = await client.get("/events?topic=test.race")
    events = [e for e in resp.json() if e['event_id'] == eid]
    
    # Dedup store harus menang: Hanya 1 yang tersimpan
    assert len(events) == 1

# Test 13: Batch Processing Load (Kirim banyak ID beda)
@pytest.mark.asyncio
async def test_batch_load(client):
    """Kirim 50 event unik dalam waktu singkat"""
    count = 50
    tasks = []
    for _ in range(count):
        payload = {
            "topic": "test.load",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "source": "load-test",
            "payload": {}
        }
        tasks.append(client.post("/publish", json=payload))
    
    await asyncio.gather(*tasks)
    await asyncio.sleep(5) # Beri waktu worker mengejar
    
    # Cek statistik (received harus naik minimal 50 dari sebelumnya)
    # Ini tes kualitatif agar sistem tidak hang
    resp = await client.get("/events?topic=test.load")
    # Pastikan data masuk (limit default API mungkin 100, jadi aman)
    assert len(resp.json()) >= count

# Test 14: SQL Injection Safety (Basic)
@pytest.mark.asyncio
async def test_sql_injection_attempt(client):
    """Memastikan raw query tidak dieksekusi mentah"""
    eid = str(uuid.uuid4())
    payload = {
        "topic": "test.security",
        "event_id": eid,
        "timestamp": datetime.utcnow().isoformat(),
        # Mencoba inject di field source
        "source": "'; DROP TABLE processed_events; --",
        "payload": {}
    }
    
    await client.post("/publish", json=payload)
    await asyncio.sleep(1)
    
    # Cek apakah tabel masih ada dengan hit API events
    resp = await client.get("/events")
    assert resp.status_code == 200
    
# Test 15: Large Payload Handling
@pytest.mark.asyncio
async def test_large_payload(client):
    """Kirim payload agak besar (10KB)"""
    large_data = "x" * 10000
    payload = {
        "topic": "test.large",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "source": "pytest",
        "payload": {"blob": large_data}
    }
    
    resp = await client.post("/publish", json=payload)
    assert resp.status_code == 200