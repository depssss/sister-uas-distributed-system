import os
import redis
import psycopg2
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- KONFIGURASI ---
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
DB_HOST = os.getenv("POSTGRES_HOST", "storage")
DB_NAME = os.getenv("POSTGRES_DB", "db")
DB_USER = os.getenv("POSTGRES_USER", "user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "pass")

r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def init_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                event_id TEXT UNIQUE,
                topic TEXT,
                timestamp TIMESTAMP,
                source TEXT,
                payload JSONB
            );
        ''')
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Database initialized.")
    except Exception as e:
        print(f"❌ DB Init Error: {e}")

@app.route('/publish', methods=['POST'])
def publish_event():
    data = request.json
    if not data: return jsonify({"error": "No data"}), 400
    
    event_id = data.get("event_id")
    # Increment counter 'received' setiap ada request masuk
    r.incr("stats_received")

    if r.exists(f"dedup:{event_id}"):
        r.incr("stats_duplicate")
        return jsonify({"status": "ignored", "reason": "duplicate"}), 200

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO events (event_id, topic, timestamp, source, payload) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (event_id, data.get('topic'), data.get('timestamp'), data.get('source'), json.dumps(data.get('payload')))
        )
        conn.commit()
        cur.close()
        conn.close()

        r.setex(f"dedup:{event_id}", 3600, "1")
        return jsonify({"status": "success", "event_id": event_id}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- ENDPOINT BARU: /stats ---
@app.route('/stats', methods=['GET'])
def get_stats():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Hitung jumlah unik di DB
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT topic) FROM events")
        unique_count, topic_count = cur.fetchone()
        
        # Ambil daftar topic
        cur.execute("SELECT DISTINCT topic FROM events")
        topics = [row[0] for row in cur.fetchall()]
        
        cur.close()
        conn.close()

        received = int(r.get("stats_received") or 0)
        duplicate = int(r.get("stats_duplicate") or 0)

        return jsonify({
            "received": received,
            "unique_processed": unique_count,
            "duplicate_dropped": duplicate,
            "topics": topics,
            "status": "active"
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- ENDPOINT BARU: /events ---
@app.route('/events', methods=['GET'])
def get_events():
    topic = request.args.get('topic')
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        if topic:
            cur.execute("SELECT event_id, topic, timestamp, source, payload FROM events WHERE topic = %s", (topic,))
        else:
            cur.execute("SELECT event_id, topic, timestamp, source, payload FROM events")
        
        rows = cur.fetchall()
        events = []
        for r_item in rows:
            events.append({
                "event_id": r_item[0], "topic": r_item[1], 
                "timestamp": r_item[2].isoformat(), "source": r_item[3], "payload": r_item[4]
            })
        cur.close()
        conn.close()
        return jsonify(events), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/', methods=['GET'])
def index():
    return "Aggregator Service is Running!", 200

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=8080)