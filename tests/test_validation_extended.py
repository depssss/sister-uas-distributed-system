import uuid
from datetime import datetime

def test_empty_topic(client):
    payload = {"topic": "", "event_id": str(uuid.uuid4()), "timestamp": "2024", "source": "p", "payload": {}}
    assert client.post("/publish", json=payload).status_code in [200, 201]

def test_null_payload_ext(client):
    payload = {"topic": "t", "event_id": str(uuid.uuid4()), "timestamp": "2024", "source": "p", "payload": None}
    assert client.post("/publish", json=payload).status_code in [201, 400, 422]

def test_missing_event_id(client):
    payload = {"topic": "t", "timestamp": "2024", "source": "p", "payload": {}}
    assert client.post("/publish", json=payload).status_code in [201, 400, 422]

def test_huge_payload(client):
    payload = {"topic": "t", "event_id": "h", "timestamp": "2024", "source": "p", "payload": {"d": "x"*5000}}
    assert client.post("/publish", json=payload).status_code in [200, 201]

def test_wrong_source_type(client):
    payload = {"topic": "t", "event_id": "e", "timestamp": "2024", "source": 999, "payload": {}}
    assert client.post("/publish", json=payload).status_code in [201, 400]