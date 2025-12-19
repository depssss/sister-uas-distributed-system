import sys
import os
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from httpx import AsyncClient, ASGITransport

# --- SETUP PATH KHUSUS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
target_path = os.path.join(root_dir, "aggregator", "app")
sys.path.insert(0, target_path)

# --- IMPORT APP ---
try:
    from main import app
except ImportError as e:
    raise RuntimeError(f"FATAL: Gagal import 'main' dari {target_path}. Error: {e}")

# --- MOCK CLASSE (TIRUAN DATABASE & REDIS) ---
class FakeRedis:
    """Pura-pura jadi Redis"""
    async def rpush(self, key, value):
        return 1 # Sukses simpan
    
    async def blpop(self, key):
        # Pura-pura nunggu data (biar worker gak error loop)
        await asyncio.sleep(0.1)
        return None
    
    async def close(self):
        pass

class FakeDBConnection:
    """Pura-pura jadi Koneksi DB"""
    async def fetchval(self, query):
        return 0 # Pura-pura ada 0 data
    
    async def execute(self, query, *args):
        return "INSERT 0 1"
    
    def transaction(self):
        return MagicMock(__aenter__=AsyncMock(), __aexit__=AsyncMock())
    
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass

class FakeDBPool:
    """Pura-pura jadi Pool DB"""
    def acquire(self):
        return FakeDBConnection()
    
    async def close(self):
        pass

# --- FIXTURES ---

@pytest.fixture(scope="session")
def event_loop():
    """Membuat loop async untuk sesi tes"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="function")
async def client():
    """
    Client HTTP yang sudah disuntik dengan Database & Redis Palsu.
    Ini solusi utama agar tidak error 'AttributeError'.
    """
    # 1. Suntikkan Mock ke dalam state aplikasi
    app.state.redis = FakeRedis()
    app.state.pg_pool = FakeDBPool()
    
    # 2. Jalanin tes menggunakan client ini
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
    
    # 3. Bersihkan (Opsional)
    if hasattr(app.state, "redis"):
        await app.state.redis.close()
    if hasattr(app.state, "pg_pool"):
        await app.state.pg_pool.close()