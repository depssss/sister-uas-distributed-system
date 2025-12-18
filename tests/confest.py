import pytest
import asyncio
from httpx import AsyncClient

# URL Aggregator (sesuaikan jika run di lokal vs docker)
# Gunakan localhost:8080 karena Docker mem-forward port ini ke laptop Anda
BASE_URL = "http://localhost:8080"

@pytest.fixture(scope="session")
def event_loop():
    """Force session scope event loop"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def client():
    """Async Client untuk hit API"""
    # Timeout diset 10 detik jaga-jaga kalau worker sedang sibuk
    async with AsyncClient(base_url=BASE_URL, timeout=10.0) as ac:
        yield ac