import asyncio
import json
import os
import logging
from datetime import datetime  # <--- 1. TAMBAHKAN IMPORT INI
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel

# ... (kode import asyncpg/redis biarkan saja) ...

# ... (Logging Setup & Config biarkan saja) ...

class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime  # <--- 2. UBAH DARI 'str' MENJADI 'datetime'
    source: str
    payload: dict

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Setup Koneksi
    app.state.pg_pool = await asyncpg.create_pool(DATABASE_URL)
    app.state.redis = redis.from_url(BROKER_URL)
    
    # 2. Setup Database Schema (Idempotency Key)
    async with app.state.pg_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                event_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                source TEXT,
                payload JSONB,
                processed_at TIMESTAMP DEFAULT NOW(),
                CONSTRAINT unique_event_topic UNIQUE (event_id, topic)
            );
        """)
    
    # 3. Start Background Worker
    worker_task = asyncio.create_task(process_queue(app))
    logger.info("System Started & DB Connected")
    
    yield
    
    # 4. Cleanup
    worker_task.cancel()
    await app.state.pg_pool.close()
    await app.state.redis.close()

app = FastAPI(lifespan=lifespan)

@app.post("/publish")
async def publish_event(event: Event):
    """Menerima event dan menaruhnya di antrian Redis."""
    await app.state.redis.rpush("event_queue", event.json())
    return {"status": "queued", "event_id": event.event_id}

@app.get("/stats")
async def get_stats():
    """Melihat jumlah data unik yang berhasil disimpan."""
    async with app.state.pg_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM events")
    return {"unique_processed": count}

async def process_queue(app):
    """Worker: Ambil dari Redis -> Simpan ke Postgres (Atomic Dedup)."""
    logger.info("Worker started listening...")
    while True:
        try:
            # Blocking pop (tunggu sampai ada data)
            _, data = await app.state.redis.blpop("event_queue")
            event = json.loads(data)
            
            async with app.state.pg_pool.acquire() as conn:
                # Transaksi Database
                async with conn.transaction():
                    # TEKNIK DEDUP UTAMA: ON CONFLICT DO NOTHING
                    result = await conn.execute("""
                        INSERT INTO events (event_id, topic, source, payload)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (event_id, topic) DO NOTHING
                    """, event['event_id'], event['topic'], event['source'], json.dumps(event['payload']))
                    
                    if result == "INSERT 0 1":
                        logger.info(f"✅ Processed Unique: {event['event_id']}")
                    else:
                        logger.warning(f"♻️ Duplicate Dropped: {event['event_id']}")
                        
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in worker: {e}")
            await asyncio.sleep(1)