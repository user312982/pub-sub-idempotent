# Panduan Demo Video Manual — UAS Sistem Terdistribusi

> **Durasi target:** 25–30 menit  
> **Format rekaman:** Fullscreen terminal + browser (split atau sequential)

---

## Persiapan Sebelum Rekaman

### Kebutuhan Sistem
- Docker + Docker Compose terinstall.
- Terminal dengan font besar (min. 14pt) agar command mudah dibaca.
- Browser terbuka di repository GitHub Anda (bagian README Arsitektur).

Anda akan menjalankan langkah-langkah di bawah ini secara **manual satu per satu** sambil menjelaskan. Command yang ada di kotak `bash` bisa Anda copy-paste ke terminal saat merekam.

---

## Struktur Demo (25 Menit)

| Segmen | Durasi | Fokus |
|---|---|---|
| 1. Intro & Arsitektur | 3 menit | Penjelasan sistem, diagram Mermaid di GitHub |
| 2. Build & Run | 3 menit | Menjalankan stack Docker Compose |
| 3. Publish Event | 4 menit | Test API: single, batch, duplikat |
| 4. Query & Stats | 3 menit | Cek data via `/events` dan `/stats` |
| 5. Schema Validation | 2 menit | Request tidak valid → Error 422 |
| 6. Stress Test | 4 menit | Publisher simulator 20k+ events |
| 7. Bukti Konkurensi | 3 menit | Memperlihatkan log worker, duplikat di-drop |
| 8. Unit Tests | 3 menit | Menjalankan 18 tests (semua passed) |

---

## Panduan Langkah & Skrip Naratif

*(Teks miring adalah panduan apa yang diucapkan. Teks di dalam kotak hitam adalah command yang dijalankan di terminal)*

### Segmen 1 — Intro & Arsitektur (3 menit)

> *"Selamat datang di demo UAS Sistem Terdistribusi. Proyek ini adalah Pub-Sub Log Aggregator yang dibangun menggunakan Python, FastAPI, Redis Stream, dan PostgreSQL — semua dijalankan lewat Docker Compose.*
>
> *Sistem ini dirancang untuk memastikan sebuah event hanya diproses tepat satu kali, meski dikirim berkali-kali oleh publisher yang berbeda. Mari kita lihat arsitekturnya di GitHub README..."*

**[Tampilkan browser → GitHub README → Diagram Mermaid]**

> *"Ada 4 service utama: Publisher sebagai simulator, Aggregator sebagai API dan consumer, Redis sebagai message broker, dan PostgreSQL sebagai penyimpanan persisten.*
>
> *Kunci idempotency ada di constraint PRIMARY KEY PostgreSQL dengan INSERT ON CONFLICT DO NOTHING. Sementara distribusi workload paralel menggunakan Redis Stream Consumer Group."*

---

### Segmen 2 — Build & Run (3 menit)

**[Buka Terminal]**

> *"Sekarang kita akan menjalankan semua layanannya menggunakan Docker Compose."*

```bash
docker compose up -d --build aggregator broker storage
```

> *"Kita bisa lihat docker compose menjalankan 3 service: aggregator, broker (Redis), dan storage (Postgres). Tidak ada port yang diekspos ke publik kecuali port 8080 untuk API lokal ini."*

Cek apakah sistem sudah berjalan:
```bash
curl http://localhost:8080/health
```
*(Tunggu sampai mengembalikan `{"status":"ok"}`)*

---

### Segmen 3 — Publish Event (4 menit)

> *"Sekarang kita coba mengirim event ke aggregator."*

**1. Kirim Single Event:**
```bash
curl -s -X POST http://localhost:8080/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"orders","event_id":"ord-001","timestamp":"2024-06-01T10:00:00Z","source":"order-service","payload":{"amount":150000}}' \
  | python3 -m json.tool
```
> *"Status OK, event masuk ke antrean Redis Stream dan langsung diproses."*

**2. Kirim Duplikat (event_id sama persis):**
```bash
curl -s -X POST http://localhost:8080/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"orders","event_id":"ord-001","timestamp":"2024-06-01T10:00:00Z","source":"order-service","payload":{"amount":150000}}' \
  | python3 -m json.tool
```
> *"Sistem membalas 200 OK karena API menerima pengiriman ini (At-least-once delivery). Namun nanti event ini tidak akan masuk ke database lagi karena deduplikasi."*

**3. Kirim Batch Event:**
```bash
curl -s -X POST http://localhost:8080/publish \
  -H 'Content-Type: application/json' \
  -d '[
    {"topic":"payments","event_id":"pay-001","timestamp":"2024-06-01T10:01:00Z","source":"payment-svc","payload":{}},
    {"topic":"logs","event_id":"log-001","timestamp":"2024-06-01T10:01:02Z","source":"auth-svc","payload":{}}
  ]' | python3 -m json.tool
```
> *"Kita juga bisa mengirim beberapa event sekaligus dalam format batch JSON."*

---

### Segmen 4 — Query & Stats (3 menit)

> *"Kita akan buktikan bahwa deduplikasi benar-benar terjadi di level database."*

**Lihat statistik sistem:**
```bash
curl -s http://localhost:8080/stats | python3 -m json.tool
```
> *"Bisa dilihat di sini: received=4, tapi unique_processed=3 dan duplicate_dropped=1. Duplikat yang tadi kita kirim berhasil dideteksi dan dibuang."*

**Cek data aslinya:**
```bash
curl -s http://localhost:8080/events | python3 -m json.tool
```

---

### Segmen 5 — Schema Validation (2 menit)

> *"FastAPI secara otomatis memvalidasi struktur data dengan Pydantic. Mari kita coba mengirim event yang salah format."*

```bash
curl -s -X POST http://localhost:8080/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"test","event_id":"bad","timestamp":"bukan-iso8601","source":"test","payload":{}}' \
  | python3 -m json.tool
```

> *"Karena format timestamp tidak valid, sistem langsung menolak dengan error 422 Unprocessable Entity."*

---

### Segmen 6 — Stress Test & Konkurensi (4 menit)

> *"Untuk membuktikan ketahanan sistem, kita akan jalankan publisher simulator yang menembakkan ribuan event secara bersamaan."*

```bash
# Menjalankan stress test ringan (1000 event)
docker compose run --rm -e TOTAL_EVENTS=1000 publisher
```

> *"Ini akan menjalankan script publisher. Sambil menunggu, kita bisa cek live statusnya."*

**(Buka terminal tab baru, atau split terminal):**
```bash
curl -s http://localhost:8080/stats | python3 -m json.tool
```
> *"Stats akan terus bertambah. Nantinya total unique + duplicate_dropped harus sama persis dengan total received."*

---

### Segmen 7 — Bukti Konkurensi dari Log (3 menit)

> *"Di balik layar, ada 4 worker yang memproses ini paralel secara aman tanpa lost-update."*

```bash
docker compose logs aggregator --tail=100 | grep -E "worker|Started|Duplicate" | head -20
```

> *"Terlihat ada worker-0, worker-1, worker-2, worker-3 yang membaca dari Redis Stream bersamaan. Setiap kali mendeteksi duplikat, sistem mencatat 'Duplicate' dengan level WARNING sebagai audit trail. Dan PostgreSQL mengamankan race condition via transaksi row-level lock."*

---

### Segmen 8 — Unit Tests & Penutup (3 menit)

> *"Terakhir, kita akan jalankan automated tests untuk memastikan semuanya bebas bug."*

**(Pastikan Postgres & Redis lokal menyala. Jika tidak, Anda bisa bahas file test-nya saja di editor)*

```bash
source .venv/bin/activate
pytest tests/ -v --ignore=tests/test_stress.py
```

> *"Ada 18 pengujian mulai dari schema validation, API, deduplikasi, hingga pembuktian bahwa sistem terhindar dari anomali lost-update dan race condition.*
> 
> *Demikian demo ini, semuanya berjalan sesuai spesifikasi dari desain sistem terdistribusi. Terima kasih."*

---

**Jangan lupa jalankan perintah ini jika ingin mereset/menghapus data setelah latihan merekam:**
```bash
docker compose down -v
```
