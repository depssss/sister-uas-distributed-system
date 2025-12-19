import pytest

@pytest.mark.asyncio
async def test_method_not_allowed(client):
    """Coba akses /publish pakai GET (harusnya cuma boleh POST)"""
    resp = await client.get("/publish")
    assert resp.status_code == 405 # Method Not Allowed

@pytest.mark.asyncio
async def test_endpoint_not_found(client):
    """Coba akses endpoint ngawur"""
    resp = await client.get("/ngawur")
    assert resp.status_code == 404

@pytest.mark.asyncio
async def test_post_without_body(client):
    """Coba POST ke /publish tanpa kirim data JSON apa-apa"""
    resp = await client.post("/publish")
    assert resp.status_code == 422 # Unprocessable Entity (Missing body)