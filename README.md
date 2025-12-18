NAMA : DEWI PURNAMASARI 
NIM : 11221087

# UAS – Distributed Systems  
## Pub-Sub Log Aggregator dengan Idempotency & Deduplication

### Deskripsi Singkat
Project ini membangun sistem **Publish–Subscribe Log Aggregator** berbasis **Docker Compose** yang terdiri dari beberapa service internal. Sistem dirancang untuk menangani **duplikasi event**, **idempotency**, serta **konkurensi** dengan pendekatan transaksi yang aman dan konsisten.

Sistem berjalan sepenuhnya di jaringan lokal Docker Compose tanpa ketergantungan layanan eksternal publik.

---

## Arsitektur Sistem

### Services
- **aggregator**
  - REST API (FastAPI)
  - Menerima event dari publisher
  - Memproses deduplication dan penyimpanan event
- **publisher**
  - Simulator/generator event
  - Mengirim event duplikat (at-least-once delivery)
- **broker**
  - Redis (internal message broker)
- **storage**
  - SQLite (file-based) dengan Docker volume

---

## Model Event

```json
{
  "topic": "string",
  "event_id": "string-unik",
  "timestamp": "ISO8601",
  "source": "string",
  "payload": { "key": "value" }
}
