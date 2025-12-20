def test_method_not_allowed(client):
    assert client.get("/publish").status_code == 405

def test_endpoint_not_found(client):
    assert client.get("/ngawur").status_code == 404

def test_post_without_body(client):
    resp = client.post("/publish")
    assert resp.status_code in [400, 415, 422]