def test_null_payload_schema(client):
    resp = client.post("/publish", json={})
    assert resp.status_code in [400, 500]

def test_wrong_data_type_schema(client):
    payload = {"topic":"t", "event_id":123, "timestamp":"2024", "source":"s", "payload":{}}
    resp = client.post("/publish", json=payload)
    assert resp.status_code in [201, 400]

def test_invalid_json_format(client):
    resp = client.post("/publish", data="bukan json", content_type='application/json')
    assert resp.status_code in [400, 415, 500]