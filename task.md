# UAS Sistem Terdistribusi (Take-Home, 1 Minggu)

> **Tema:** Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi (Docker Compose Wajib)

## Deskripsi Singkat
Bangun sistem Pub-Sub log aggregator multi-service yang berjalan dengan Docker Compose. Sistem harus mendukung **idempotency** (consumer tidak memproses ulang event yang sama), **deduplication** kuat, serta **transaksi/konkurensi** yang mencegah *race condition* dan memastikan konsistensi data. Semua layanan berjalan pada jaringan lokal Compose, tanpa akses layanan eksternal publik.

*Catatan: Contoh penggunaan K6 tersedia di GitHub.*

---

## Ketentuan Umum
* **Sifat:** Individu, *take-home* 1 minggu.
* **Bahasa Pengantar:** Bahasa Indonesia (dengan istilah teknis Inggris bila relevan).
* **Cakupan Teori:** Bab 1–13 dari buku utama (*DISTRIBUTED SYSTEMS Concepts and Design - Fifth Edition*). Penekanan pada *Transactions/Concurrency Control*.
* **Bahasa Pemrograman:** Rust atau Python (pilih salah satu).
* **Infrastruktur:** Docker Compose **Wajib**. Boleh menggunakan *image* open-source tambahan sebagai internal service (mis. `redis`, `nats`, `postgres`).
* **Jaringan:** Hanya lokal dalam Compose (tanpa akses ke layanan eksternal publik).
* **Persistensi Data:** Harus aman meski container dihapus (gunakan *named volumes* atau *bind mounts*).
* **Video Demo:** Wajib (YouTube *unlisted* atau *public*), durasi minimal 25 menit.
* **Pengujian:** Unit/Integration Tests tingkatkan menjadi 12–20 tests.
* **Pengumpulan:** Submit via link GitHub + laporan (PDF/MD) menggunakan format sitasi APA 7th berbahasa Indonesia.

---

## Tujuan Pembelajaran
* **Bab 1–2:** Karakteristik sistem terdistribusi dan arsitektur (*publish–subscribe*, *microservices*).
* **Bab 3–4:** Komunikasi antar komponen dan penamaan (`topic`, `event_id`).
* **Bab 5:** Waktu dan *ordering* (*logical/monotonic ordering*, toleransi *event out-of-order*).
* **Bab 6:** Toleransi kegagalan (duplikasi, *crash*, *retry*, *backoff*, *graceful restart*).
* **Bab 7:** Konsistensi dan replikasi (*eventual/causal*; idempotency + dedup untuk konsistensi).
* **Bab 8–9:** Transaksi dan kontrol konkurensi (ACID, *isolation*, *conflict resolution*, *idempotent upsert*).
* **Bab 10–11:** Keamanan dan sistem berkas/penyimpanan terdistribusi (akses lokal, isolasi jaringan Compose, persistensi).
* **Bab 12–13:** Sistem berbasis web dan koordinasi (*orchestration*, *observability*, *readiness/liveness*, koordinasi antar service).

---

## Struktur Tugas

### Bagian Teori (30%)
Jawab ringkas (150–250 kata/poin) dan sertakan sitasi APA 7th. Soroti Bab 8–9 (transaksi/konkurensi) disertai contoh dari rancangan Anda.

1.  **T1 (Bab 1):** Karakteristik sistem terdistribusi dan *trade-off* desain Pub-Sub aggregator.
2.  **T2 (Bab 2):** Kapan memilih arsitektur *publish–subscribe* dibanding *client–server*? Alasan teknis.
3.  **T3 (Bab 3):** *At-least-once* vs *exactly-once delivery*; peran *idempotent consumer*.
4.  **T4 (Bab 4):** Skema penamaan `topic` dan `event_id` (unik, *collision-resistant*) untuk dedup.
5.  **T5 (Bab 5):** Ordering praktis (*timestamp* + *monotonic counter*); batasan dan dampaknya.
6.  **T6 (Bab 6):** Failure modes dan mitigasi (*retry*, *backoff*, *durable dedup store*, *crash recovery*).
7.  **T7 (Bab 7):** Eventual consistency pada aggregator; peran idempotency + dedup.
8.  **T8 (Bab 8):** Desain transaksi: ACID, *isolation level*, dan strategi menghindari *lost-update*.
9.  **T9 (Bab 9):** Kontrol konkurensi: *locking/unique constraints/upsert*; *idempotent write pattern*.
10. **T10 (Bab 10–13):** Orkestrasi Compose, keamanan jaringan lokal, persistensi (volume), *observability*.

### Bagian Implementasi (70%)
Bangun sistem multi-service dengan Docker Compose dan pilihan bahasa Python atau Rust.

#### a. Arsitektur Layanan (Compose)
* **aggregator:** API untuk *publish* dan akses *event*; consumer internal memproses *queue*.
* **publisher:** Generator/simulator event (termasuk duplikasi) ke broker/aggregator.
* **broker** *(Opsional namun disarankan)*: Message broker internal (mis. `redis`, `nats`).
* **storage:** Database persisten (mis. `postgres:16-alpine`) atau *file-based* (SQLite) dengan volume.
* **Jaringan Compose:** Default network internal; tidak ada port keluar ke layanan eksternal selain *expose* untuk demo lokal.

#### b. Model Event & API
* **Event JSON Minimal:** `{ "topic": "string", "event_id": "string-unik", "timestamp": "ISO8601", "source": "string", "payload": { ... } }`
* **POST `/publish`:** Menerima single/batch event; validasi skema.
* **GET `/events?topic=...`:** Daftar event unik yang telah diproses.
* **GET `/stats`:** `received`, `unique_processed`, `duplicate_dropped`, `topics`, `uptime`.

#### c. Idempotency & Deduplication (Persisten)
* **Dedup Store Persisten:** Postgres (tabel dengan *constraint* unik `(topic, event_id)`) atau SQLite dengan file di volume.
* **Idempotency:** Event `(topic, event_id)` yang sama hanya diproses sekali walau diterima berkali-kali.
* **Logging:** Harus jelas untuk deteksi duplikasi (*audit log* disarankan).

#### d. Transaksi & Konkurensi
* Terapkan transaksi saat *insert/processing* untuk mencegah *race condition*.
* Gunakan *upsert/unique constraints* untuk dedup atomik (contoh: `INSERT ... ON CONFLICT DO NOTHING`).
* **Uji Konkurensi:** Jalankan beberapa worker/threads consumer; buktikan tidak ada *double-process*.
* Jelaskan *isolation level* yang dipilih (mis. `READ COMMITTED` atau `SERIALIZABLE`) dan alasannya.

> **Contoh Kasus Transaksi (Disarankan):**
> * **Dedup berbasis constraint unik (Wajib):** Saat memproses event, lakukan insert ke tabel `processed_events` dengan *constraint* unik di dalam satu transaksi. Bukti: dua worker paralel tidak menghasilkan duplicate processing.
> * **Outbox + upsert (Opsional):** Tulis event ke tabel `outbox` dalam transaksi yang sama saat aggregator menyimpan event, lalu proses outbox terpisah.
> * **Batch atomic (Opsional):** `POST /publish` menerima batch; seluruh batch berhasil atau gagal secara atomik.
> * **Konsistensi statistik (Opsional):** Update stats secara transaksional agar bebas *lost-update* (contoh: `UPDATE ... SET count = count + 1 WHERE ...`).
> * **Isolation level (Wajib dijelaskan):** Pilih *isolation* sesuai kebutuhan; jelaskan *trade-off* (*phantom reads*, *write skew*) dan mitigasi.

#### e. Reliability & Ordering
* **At-least-once delivery:** Publisher mengirim duplikat; sistem tetap konsisten.
* **Crash tolerance:** Setelah restart/container recreate, dedup store mencegah reprocessing.
* **Ordering:** Jelaskan apakah total ordering diperlukan; sediakan strategi praktis.

#### f. Performa Minimum
* Proses **≥ 20.000 event** (≥ 30% duplikasi) tetap responsif.
* Sertakan metrik throughput/latency/duplicate rate di laporan.

#### g. Docker & Compose (Wajib)
* Sediakan `Dockerfile` untuk layanan aplikasi dan `docker-compose.yml`.
* **Python:** Rekomendasi base `python:3.11-slim`, non-root user, `requirements.txt`.
* **Rust:** Rekomendasi base `rust:1.72-slim` (build), runtime image minimal (`debian:bookworm-slim`).
* **Persistensi:** Gunakan *named volumes/bind mounts*; dokumentasikan lokasi data.

**Contoh skeleton `docker-compose.yml`:**
```yaml
services:
  aggregator:
    build: ./aggregator
    image: uts-aggregator:latest
    depends_on: [storage, broker]
    environment:
      - DATABASE_URL=postgres://user:pass@storage:5432/db
      - BROKER_URL=redis://broker:6379
    ports:
      - "8080:8080"
    volumes:
      - aggregator_data:/var/lib/aggregator

  publisher:
    build: ./publisher
    image: uts-publisher:latest
    depends_on: [broker]
    environment:
      - TARGET_URL=http://aggregator:8080/publish

  broker:
    image: redis:7-alpine
    ports: []  # tetap internal
    volumes:
      - broker_data:/data

  storage:
    image: postgres:16-alpine
    environment:
      - POSTGRES_DB=db
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
    volumes:
      - pg_data:/var/lib/postgresql/data

volumes:
  pg_data:
  broker_data:
  aggregator_data: