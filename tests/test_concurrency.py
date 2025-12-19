import pytest
import asyncio

@pytest.mark.asyncio
async def test_race_condition_dedup(client): # <-- Tambahkan parameter client
    """
    Menguji race condition dengan mengirim banyak request sekaligus.
    Menggunakan 'client' fixture agar tidak perlu server asli menyala.
    """
    payload = {
        "topic": "test",
        "event_id": "race-unique-id",
        "timestamp": "2024-01-01",
        "source": "test",
        "payload": {}
    }

    # Kita gunakan 'client' yang sudah di-inject dari conftest.py
    # Buat 10 task request secara bersamaan
    tasks = [client.post("/publish", json=payload) for _ in range(10)]
    
    responses = await asyncio.gather(*tasks)

    # Cek hasilnya
    for response in responses:
        assert response.status_code == 200
        assert response.json()["status"] == "queued"