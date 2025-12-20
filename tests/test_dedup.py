def test_duplicate_events_structure(client):
    payload = {"topic": "d", "event_id": "dup-1", "timestamp": "2024", "source": "s", "payload": {}}
    client.post("/publish", json=payload)
    res2 = client.post("/publish", json=payload)
    assert res2.status_code in [200, 201, 400, 409]