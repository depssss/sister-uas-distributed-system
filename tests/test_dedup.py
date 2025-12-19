import pytest

@pytest.mark.asyncio
async def test_duplicate_events_structure(client):
    """
    Test ini hanya mengecek apakah API menerima request duplikat.
    (Dedup sebenarnya terjadi di database/worker, 
    jadi di level API responsenya tetap 200 OK 'queued')
    """
    event_id = "duplicate-id-123"
    payload = {
        "topic": "test-dedup",
        "event_id": event_id,
        "timestamp": "2024-01-01T10:00:00",
        "source": "pytest",
        "payload": {}
    }

    # Kirim pertama
    res1 = await client.post("/publish", json=payload)
    assert res1.status_code == 200

    # Kirim kedua (API harusnya tetap menerima, dedup terjadi di background)
    res2 = await client.post("/publish", json=payload)
    assert res2.status_code == 200