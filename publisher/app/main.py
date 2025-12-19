import time
import uuid
import random
import requests
import datetime
import os

AGGREGATOR_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")

def generate_event(id_override=None):
    return {
        "topic": "orders",
        "event_id": id_override or str(uuid.uuid4()),
        "timestamp": datetime.datetime.now().isoformat(),
        "source": "publisher-auto",
        "payload": {"amount": random.randint(10, 500)}
    }

def run_simulation():
    try:
        # 1. Kirim Event Unik
        print("--- Sending Unique Events ---")
        for _ in range(3):
            requests.post(AGGREGATOR_URL, json=generate_event())
        
        # 2. Kirim Duplikat (Untuk Demo Idempotency)
        dup_id = f"DUP-{random.randint(1000,9999)}"
        print(f"--- Sending DUPLICATES for ID: {dup_id} ---")
        for _ in range(4):
            # Mengirim ID yang sama 4 kali
            requests.post(AGGREGATOR_URL, json=generate_event(id_override=dup_id))
            
    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    print("Publisher started. Waiting for aggregator...")
    time.sleep(5) # Tunggu service lain
    while True:
        run_simulation()
        time.sleep(5)