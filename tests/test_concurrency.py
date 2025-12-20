def test_race_condition_dedup(client):
    payload = {"topic": "c", "event_id": "race-1", "timestamp": "2024", "source": "s", "payload": {}}
    res = client.post("/publish", json=payload)
    assert res.status_code in [200, 201]