import requests
import time
import uuid
import random

TARGET_URL = "http://aggregator:8080/publish"

def send_events():
    # Cukup 5000 - 10.000 saja kalau 20.000 terlalu berat
    for i in range(10000): 
        event_id = str(uuid.uuid4())
        
        # Simulasi duplikasi (20% kemungkinan)
        if random.random() < 0.2:
            event_id = "duplicate-id-123" 

        payload = {
            "topic": "sensor_suhu",
            "event_id": event_id,
            "timestamp": "2024-01-01T10:00:00",
            "source": "publisher-sim",
            "payload": {"temp": random.randint(20, 35)}
        }
        
        try:
            requests.post(TARGET_URL, json=payload, timeout=5)
            if i % 500 == 0:
                print(f"✅ Sent {i} events...")
        except:
            pass
        
        # KUNCI AGAR TIDAK CRASH: Kasih jeda sangat kecil
        time.sleep(0.001) 

if __name__ == "__main__":
    print("🚀 Starting publisher...")
    send_events()
    print("🏁 Publisher finished.")