from datetime import datetime
from sqlalchemy import Column, String, DateTime, Integer, UniqueConstraint
from sqlalchemy.orm import declarative_base
from pydantic import BaseModel

Base = declarative_base()

# --- SQLALCHEMY MODEL (Database) ---
class ProcessedEventDB(Base):
    __tablename__ = "processed_events"
    
    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, nullable=False)
    event_id = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    source = Column(String)
    
    # CONSTRAINT UNIK: Mencegah duplikasi (topic + event_id)
    __table_args__ = (UniqueConstraint('topic', 'event_id', name='uq_topic_event_id'),)

# --- PYDANTIC MODEL (Validasi API) ---
class EventSchema(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: dict