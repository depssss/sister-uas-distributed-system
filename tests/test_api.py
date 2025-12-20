def test_publish_valid_event(client):
    payload = {"topic": "t", "event_id": "1", "timestamp": "2024", "source": "s", "payload": {}}
    resp = client.post("/publish", json=payload)
    assert resp.status_code in [200, 201]

def test_health_check(client):
    resp = client.get("/")
    assert resp.status_code in [200, 404]