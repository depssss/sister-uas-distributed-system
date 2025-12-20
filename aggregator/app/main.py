import os
import redis
import psycopg2
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- KONFIGURASI (Disamakan persis dengan docker-compose.yml) ---
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
DB_HOST = os.getenv("POSTGRES_HOST", "storage")
DB_NAME = os.getenv("POSTGRES_DB", "db")
DB_USER = os.getenv("POSTGRES_USER", "user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "pass")

# Inisialisasi Redis
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

# Inisialisasi Tabel Database
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
        print("✅ Database initialized successfully.")
    except Exception as e:
        print(f"❌ Failed to initialize database: {e}")

@app.route('/publish', methods=['POST'])
def publish_event():
    data = request.json
    event_id = data.get("event_id")

    # 1. CEK DUPLIKASI DI REDIS
    if r.exists(event_id):
        print(f"[DUPLICATE] Event {event_id} ignored.")
        return jsonify({"status": "ignored", "reason": "duplicate"}), 200

    try:
        # 2. SIMPAN KE POSTGRES
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO events (event_id, topic, timestamp, source, payload) VALUES (%s, %s, %s, %s, %s)",
            (event_id, data['topic'], data['timestamp'], data['source'], json.dumps(data['payload']))
        )
        conn.commit()
        cur.close()
        conn.close()

        # 3. CATAT DI REDIS (Expiry 1 jam)
        r.setex(event_id, 3600, "processed")

        print(f"[SUCCESS] Event {event_id} saved to DB.")
        return jsonify({"status": "success", "event_id": event_id}), 201

    except Exception as e:
        print(f"[ERROR] {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/', methods=['GET'])
def index():
    return "Aggregator Service is Running!", 200

if __name__ == '__main__':
    # Tunggu sebentar agar DB siap (opsional tapi disarankan)
    init_db()
    app.run(host='0.0.0.0', port=8080)