# Laporan UAS Sistem Terdistribusi
**Topik:** Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi

---

## Bagian Teori

### T1 (Bab 1): Karakteristik Sistem Terdistribusi dan Trade-off
Sistem terdistribusi memiliki karakteristik utama yaitu *concurrency*, ketiadaan *global clock*, dan potensi *independent failures* (Coulouris et al., 2011). Pada Pub-Sub aggregator, komponen-komponen (publisher, broker Redis, aggregator API, dan database Postgres) berjalan secara konkuren. Trade-off desain yang muncul adalah antara performa (throughput) dan keandalan (consistency). Untuk mencapai performa tinggi, kita menggunakan arsitektur event-driven asynchronous dengan *Redis Stream*. Namun, hal ini menyebabkan potensi duplikasi pesan (*at-least-once delivery*), sehingga diperlukan mekanisme *idempotency* dan sinkronisasi transaksional pada storage untuk menjaga konsistensi data.

### T2 (Bab 2): Arsitektur Publish-Subscribe vs Client-Server
Arsitektur *publish-subscribe* dipilih dibandingkan *client-server* sinkron karena memberikan dekapling spasial dan temporal (Coulouris et al., 2011). Secara teknis, publisher (penghasil log) tidak perlu mengetahui keberadaan atau status aggregator secara langsung; mereka hanya mengirim ke broker (Redis Stream). Jika aggregator mengalami *downtime*, pesan tetap tersimpan di broker dan dapat diproses nanti (*temporal decoupling*). Ini sangat penting untuk sistem agregator log yang menerima beban (*burst*) tinggi, mencegah efek *bottleneck* langsung pada layanan database utama.

### T3 (Bab 3): At-least-once vs Exactly-once Delivery
Jaringan tidak dapat diandalkan, menyebabkan masalah *Two Generals Problem* di mana kepastian pengiriman (*exactly-once*) sangat sulit dan mahal untuk dicapai (Coulouris et al., 2011). Oleh karena itu, kita memilih jaminan *at-least-once delivery* (pesan pasti sampai minimal sekali) yang sering menyebabkan duplikasi. Peran *idempotent consumer* adalah menetralisir efek duplikasi ini. Dengan desain idempoten, proses memproses pesan yang sama beberapa kali akan memberikan hasil akhir sistem yang sama seperti jika diproses sekali.

### T4 (Bab 4): Skema Penamaan `topic` dan `event_id`
Penamaan dalam sistem terdistribusi berfungsi untuk mengidentifikasi entitas secara unik (Coulouris et al., 2011). Kombinasi `topic` (string representasional aliran data) dan `event_id` (UUID v4 yang sangat *collision-resistant*) bertindak sebagai *composite key* unik. Desain deduplikasi mengandalkan keunikan identifier ini di *database constraint* (PRIMARY KEY pada tabel `events`). Hal ini mencegah fenomena di mana log duplikat dari publisher direkam ganda, menjaga integritas agregasi.

### T5 (Bab 5): Waktu dan Ordering
Tanpa *global clock* yang presisi, pengurutan absolut waktu antar node sulit dicapai (Coulouris et al., 2011). Dalam desain ini, kita menggunakan *timestamp* pragmatis berbasis waktu UTC (sebagai *wall-clock time*) pada saat pesan dibuat, dipadukan dengan *monotomic logical sequence* (berasal dari urutan `id` auto-increment pada Postgres Stream, atau timestamp saat *processed_at*). Batasannya adalah kejadian di publisher berbeda mungkin memiliki timestamp *out-of-order* akibat *clock drift*. Dampaknya pada sistem ini ditoleransi dengan tidak mewajibkan *strict temporal ordering*; data diagregasi berdasarkan waktu kedatangan (*eventual consistency*) dan keunikannya.

### T6 (Bab 6): Toleransi Kegagalan
Sistem terdistribusi rentan terhadap *crash* dan *omission failures* (Coulouris et al., 2011). Mitigasi yang diterapkan pada sistem ini meliputi:
1. **Durable Broker:** Menggunakan Redis (Stream) dan bukan in-memory API queue agar event tidak hilang saat container restart.
2. **Durable Dedup Store:** Database Postgres menyimpan event secara persisten ke disk (lewat Docker Volume). Jika container aggregator *crash*, setelah *graceful restart*, ia dapat membaca `event_id` yang sudah ada dari database, mencegah *reprocessing* event yang sama (*crash recovery*).

### T7 (Bab 7): Eventual Consistency
Data di sistem terdistribusi dengan *asynchronous replication* atau pemrosesan umumnya memiliki sifat *eventual consistency* (Coulouris et al., 2011). Pada sistem ini, *delay* antara saat log di-publish dan saat muncul di endpoint `/stats` bisa terjadi (karena antrean di Redis). Peran *idempotency* dan *deduplication* memastikan sistem pada akhirnya akan mencapai status konsisten (tepat dengan satu representasi log) tanpa memedulikan seberapa lambat atau seberapa banyak duplikat yang diterima.

### T8 (Bab 8): Desain Transaksi dan ACID
Transaksi merupakan mekanisme untuk mengeksekusi sekumpulan operasi secara atomik (ACID) agar konsisten meskipun terjadi *crash* (Coulouris et al., 2011). Untuk menghindari anomali *lost-update* (dua worker menimpa hasil penambahan metrik satu sama lain), kita menerapkan transaksi `UPDATE event_stats SET received = received + 1` langsung di database Postgres. Isolation level yang digunakan adalah `READ COMMITTED` (default Postgres), yang sudah cukup aman dari anomali ini karena `UPDATE` akan mengambil *row-level lock* sebelum modifikasi.

### T9 (Bab 9): Kontrol Konkurensi
Kontrol konkurensi diperlukan agar akses paralel tidak merusak data (Coulouris et al., 2011). Strategi *idempotent write pattern* diimplementasikan dengan `INSERT INTO ... ON CONFLICT (topic, event_id) DO NOTHING`. Saat beberapa *worker* konsumen secara kompetitif mencoba memasukkan `event_id` yang sama, mekanisme ini memaksa penyelesaian *conflict* (race condition) secara deterministik: hanya satu pekerja yang barisnya tercatat (rowcount=1), sisanya diabaikan (rowcount=0) dengan aman tanpa membatalkan transaksi.

### T10 (Bab 10–13): Keamanan Jaringan dan Persistensi
Isolasi keamanan dibangun melalui *virtual network* pada Docker Compose; layanan internal seperti Postgres dan Redis tidak mengekspos *port* publik. Sistem penyimpanan (*storage*) menggunakan pendekatan *persistent data layer* melalui *named volumes* (`pg_data` dan `broker_data`) agar *lifetime* data terpisah dari *lifetime container* statis. Orkestrasi ditangani Docker Compose dengan mekanisme *healthcheck*, memastikan urutan *startup* (aggregator tidak *live* sebelum storage siap) (*observability* & *readiness/liveness probes*).

---

## Arsitektur & Implementasi

Sistem diimplementasikan di atas Python (FastAPI & asyncio) dengan tumpukan teknologi:
- **FastAPI / Uvicorn**: Aggregator log, terekspos di `localhost:8080`.
- **Redis 7**: *Message Broker* untuk antrean streaming yang bisa diserap paralel (`XREADGROUP`).
- **Postgres 16**: Basis data relasional dengan pool koneksi (`asyncpg`) untuk mencatat log unik dan metrik terpusat.
- **Publisher**: Modul Python asinkron yang mampu menyuntikkan lebih dari 25.000 log (*stress testing*).

### Konkurensi & Bukti No Race Condition
1. Redis stream didesain dengan konsep Consumer Group (`aggregator_group`), sehingga banyak worker (`WORKER_COUNT=4`) membagi beban secara adil.
2. Jika ada publisher nakal mengirim 5 event persis sama nyaris dalam 1 milidetik, Redis mungkin melempar event ini ke 4 worker berbeda.
3. Namun saat 4 worker itu mengeksekusi `INSERT ... ON CONFLICT DO NOTHING`, Postgres menahan *row-lock*. Hanya 1 yang akan sukses. Terbukti lewat `/stats` tidak akan merekam metrik `unique_processed` berlebih.

---

### Metrik Performa (Stress Test)

Hasil pengujian stres dengan **26.000 event (20.000 unik + 6.000 duplikat / 30%)**:

| Metrik | Nilai |
|---|---|
| Total event dikirim | 26.000 |
| Unique event tersimpan | 20.000 |
| Duplikat di-drop | 6.000 |
| Duplicate rate | 30% |
| Waktu total (local test) | ~117 detik |
| Throughput publish ke Redis | ~222 events/detik |
| Worker count | 4 paralel |
| Konsistensi data akhir | ✅ `unique + dropped == received` |

> Throughput dipengaruhi jumlah worker dan latensi I/O ke Postgres. Pada deployment Docker Compose dengan Postgres di container terpisah, throughput lebih tinggi karena network lebih stabil.

---

### Persistensi Data — Lokasi

Seluruh data persisten dikelola via **Docker named volumes**:

| Volume | Lokasi di Container | Isi |
|---|---|---|
| `pg_data` | `/var/lib/postgresql/data` | Semua tabel Postgres: `events` dan `event_stats` |
| `broker_data` | `/data` | Redis AOF/RDB persistence (backup stream) |

Data tidak hilang meski container dihapus dan dibuat ulang (`docker compose down && docker compose up`). Untuk menghapus data sepenuhnya gunakan `docker compose down -v`.

---
**Referensi:**
Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2011). *Distributed Systems: Concepts and Design* (5th ed.). Addison-Wesley.
