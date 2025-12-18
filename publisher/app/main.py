import time
import uuid
import random
import requests
import os
from datetime import datetime

TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")

def generate_event(repeat_id=None):
    event_id = repeat_id if repeat_id else str(uuid.uuid4())
    return {
        "topic": random.choice(["order.created", "payment.success", "user.login"]),
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "publisher-sim",
        "payload": {"amount": random.randint(10, 1000)}
    }

def main():
    print(f"Publisher starting... Target: {TARGET_URL}")
    time.sleep(8) # Tunggu Aggregator & DB siap
    
    recent_ids = []

    while True:
        try:
            # 30% kemungkinan kirim duplikat dari history
            should_duplicate = (random.random() < 0.3) and (len(recent_ids) > 0)
            
            if should_duplicate:
                ev_id = random.choice(recent_ids)
                payload = generate_event(repeat_id=ev_id)
                print(f"Sending DUPLICATE: {ev_id}")
            else:
                payload = generate_event()
                recent_ids.append(payload["event_id"])
                # Simpan 50 ID terakhir untuk duplikasi
                if len(recent_ids) > 50: recent_ids.pop(0)
                print(f"Sending NEW: {payload['event_id']}")

            resp = requests.post(TARGET_URL, json=payload, timeout=2)
            
            # Simulasi delay acak
            time.sleep(random.uniform(0.1, 0.5))
            
        except Exception as e:
            print(f"Error publishing: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()