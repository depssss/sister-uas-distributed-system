import asyncio
import json
import logging
import os
from datetime import datetime  # <--- TAMBAHAN PENTING
from contextlib import asynccontextmanager

import redis.asyncio as redis
import uvicorn
from fastapi import FastAPI, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.exc import IntegrityError

from models import Base, ProcessedEventDB, EventSchema

# --- CONFIG ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/db")
BROKER_URL = os.getenv("BROKER_URL", "redis://localhost:6379/0")
QUEUE_NAME = "event_queue"

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("aggregator")

# --- DATABASE ENGINE ---
engine = create_async_engine(DATABASE_URL, echo=False, isolation_level="READ COMMITTED")
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

# --- REDIS CLIENT ---
redis_client = redis.from_url(BROKER_URL, decode_responses=True)

# --- WORKER PROCESS ---
async def process_queue():
    """Worker background untuk memproses queue dan insert ke DB"""
    logger.info("Worker: Started listening to Redis...")
    while True:
        try:
            # Atomic BLPOP: Tunggu item masuk queue
            task = await redis_client.blpop(QUEUE_NAME, timeout=5)
            if not task:
                continue
            
            _, event_json = task
            event_data = json.loads(event_json)
            
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    try:
                        # KONVERSI STRING KE DATETIME (Perbaikan disini)
                        timestamp_obj = datetime.fromisoformat(event_data['timestamp'])

                        # Buat object DB
                        new_event = ProcessedEventDB(
                            topic=event_data['topic'],
                            event_id=event_data['event_id'],
                            timestamp=timestamp_obj, # Gunakan object datetime, bukan string
                            source=event_data['source']
                        )
                        session.add(new_event)
                        # Flush untuk trigger Unique Constraint Check
                        await session.flush()
                        
                        logger.info(f"Worker: Processed UNIQUE {event_data['event_id']}")
                        await redis_client.incr("stats:unique_processed")
                        
                    except IntegrityError:
                        # Tangani Duplikat
                        logger.warning(f"Worker: Dropped DUPLICATE {event_data['event_id']}")
                        await session.rollback()
                        await redis_client.incr("stats:duplicate_dropped")
                    except Exception as e:
                        logger.error(f"Worker Error DB: {e}")
                        await session.rollback()
                        
        except Exception as e:
            logger.error(f"Worker Crash: {e}")
            await asyncio.sleep(1)

# --- LIFECYCLE ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Buat Table
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # Start Worker
    worker_task = asyncio.create_task(process_queue())
    
    yield
    
    # Shutdown
    worker_task.cancel()

app = FastAPI(lifespan=lifespan)

# --- API ENDPOINTS ---
@app.post("/publish")
async def publish_event(event: EventSchema):
    try:
        # Serialisasi datetime agar bisa masuk JSON Redis
        payload_json = event.model_dump_json()
        
        # Masukkan ke Redis Queue
        await redis_client.rpush(QUEUE_NAME, payload_json)
        await redis_client.incr("stats:received")
        
        return {"status": "queued", "event_id": event.event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events")
async def get_events(topic: str = None):
    async with AsyncSessionLocal() as session:
        query = select(ProcessedEventDB)
        if topic:
            query = query.where(ProcessedEventDB.topic == topic)
        query = query.limit(100)
        result = await session.execute(query)
        return result.scalars().all()

@app.get("/stats")
async def get_stats():
    received = await redis_client.get("stats:received") or 0
    unique = await redis_client.get("stats:unique_processed") or 0
    dropped = await redis_client.get("stats:duplicate_dropped") or 0
    
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(ProcessedEventDB.topic).distinct())
        topics = res.scalars().all()

    return {
        "received": int(received),
        "unique_processed": int(unique),
        "duplicate_dropped": int(dropped),
        "topics": topics
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080)