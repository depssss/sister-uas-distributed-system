import sqlite3
import os

DB_PATH = os.getenv("DATABASE_PATH", "events.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT,
            event_id TEXT,
            timestamp TEXT,
            source TEXT,
            payload TEXT,
            UNIQUE(topic, event_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            key TEXT PRIMARY KEY,
            value INTEGER
        )
    """)
    for k in ["received", "unique_processed", "duplicate_dropped"]:
        conn.execute(
            "INSERT OR IGNORE INTO stats (key,value) VALUES (?,0)", (k,)
        )
    conn.commit()
    conn.close()
