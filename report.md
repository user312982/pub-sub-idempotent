# Laporan UAS Sistem Terdistribusi
**Topik:** Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi

---

## Bagian Teori

### T1 (Bab 1): Karakteristik Sistem Terdistribusi dan Trade-off
Sistem terdistribusi memiliki karakteristik utama yaitu *concurrency*, ketiadaan *global clock*, dan kerentanan terhadap *independent failures* pada komponen maupun jaringan (Coulouris et al., 2011; van Steen & Tanenbaum, 2023). Pada sistem *Pub-Sub Log Aggregator* yang dirancang, karakteristik *concurrency* terlihat sangat jelas: beberapa entitas *publisher* mengirimkan log secara asinkron, sementara beberapa instans *worker aggregator* memproses pesan tersebut dari Redis secara paralel. Ketiadaan *global clock* diatasi secara pragmatis dengan penggunaan zona waktu UTC saat event dibentuk. Kegagalan independen dimitigasi dengan memisahkan arsitektur ke dalam beberapa kontainer yang terisolasi. Dalam desain sistem seperti ini, selalu ada *trade-off* antara performa (*throughput* dan *latency*) berhadapan dengan keandalan (*consistency*). Untuk mendapatkan performa tinggi dalam menyerap ribuan event, arsitektur ini menggunakan pendekatan *event-driven* melalui antrean *Redis Stream*, sehingga membebaskan *publisher* dari keharusan menunggu database selesai menulis (*decoupling*). Namun, *trade-off* dari keputusan ini adalah potensi pengiriman ganda ketika jaringan bermasalah (*at-least-once delivery*), sehingga memaksa sistem di sisi agregator untuk memikul beban pemrosesan *idempotency* dan *deduplication* menggunakan sinkronisasi transaksional agar data akhir tetap konsisten.

### T2 (Bab 2): Arsitektur Publish-Subscribe vs Client-Server
Arsitektur *publish-subscribe* (Pub-Sub) sangat direkomendasikan dan dipilih dibandingkan arsitektur *client-server* sinkron tradisional karena kemampuannya memberikan *spatial decoupling* dan *temporal decoupling* (van Steen & Tanenbaum, 2023). Secara teknis, *spatial decoupling* berarti *publisher* (sebagai penghasil log) tidak perlu mengetahui alamat IP, keberadaan, atau jumlah agregator (sebagai *subscriber*) secara langsung. Mereka cukup mengirimkan *payload* ke satu titik pusat yaitu broker (Redis Stream). Sementara itu, *temporal decoupling* berarti *publisher* dan agregator tidak perlu aktif secara bersamaan. Jika layanan agregator atau database mengalami *downtime* atau sedang *restart*, pesan yang dikirim oleh *publisher* tidak akan hilang, melainkan tetap tersimpan dengan aman di dalam antrean persisten broker. Ketika agregator kembali hidup, ia dapat melanjutkan memproses pesan dari titik terakhir yang belum dibaca. Pendekatan asinkron ini sangat vital untuk sistem pencatatan log yang sering kali menghadapi lonjakan lalu lintas secara tiba-tiba (*burst*), mencegah efek *bottleneck* langsung yang dapat mematikan database utama apabila diakses menggunakan arsitektur *client-server* yang ketat (Coulouris et al., 2011).

### T3 (Bab 3): At-least-once vs Exactly-once Delivery
Dalam jaringan yang tidak dapat diandalkan, masalah teoritis seperti *Two Generals Problem* menunjukkan bahwa menjamin kepastian pengiriman tepat satu kali (*exactly-once delivery*) tanpa konfirmasi yang tiada henti adalah hal yang mustahil untuk dicapai dengan murah (van Steen & Tanenbaum, 2023). Kegagalan jaringan atau terputusnya koneksi di tengah transmisi membuat pengirim tidak tahu apakah pesannya sudah diproses atau belum. Oleh karena itu, *trade-off* teknis yang dipilih pada sistem ini adalah jaminan *at-least-once delivery*, di mana pesan dipastikan sampai minimal satu kali melalui mekanisme pengiriman ulang (*retry*) jika gagal. Konsekuensi dari pendekatan ini adalah sistem penerima akan sering mendapatkan pesan duplikat. Di sinilah peran *idempotent consumer* menjadi sangat esensial. Konsumen yang idempoten dirancang untuk secara aman memproses pesan yang sama berkali-kali tanpa mengubah status (*state*) sistem melebihi perubahan yang terjadi pada pemrosesan pertama (Coulouris et al., 2011). Desain ini mengalihkan kompleksitas dari lapisan transport jaringan ke logika bisnis aplikasi, memastikan integritas hasil agregasi log.

### T4 (Bab 4): Skema Penamaan `topic` dan `event_id`
Penamaan (*naming*) di dalam sistem terdistribusi berfungsi vital untuk mengidentifikasi sebuah entitas secara unik, sehingga resource dapat dikelola dan dibedakan tanpa ambiguitas (Coulouris et al., 2011). Pada sistem agregator log ini, skema penamaan berbasis *flat naming* digunakan melalui kombinasi antara `topic` (kategori atau asal log, misal "auth", "payment") dan `event_id`. Identifier `event_id` ini direpresentasikan menggunakan format UUID v4 (Universally Unique Identifier) yang memiliki sifat sangat *collision-resistant*, artinya probabilitas dua *event* berbeda memiliki UUID yang sama secara acak adalah sangat mendekati nol (van Steen & Tanenbaum, 2023). Desain deduplikasi pada arsitektur ini secara penuh mengandalkan keunikan penggabungan nilai `(topic, event_id)` tersebut dengan menjadikannya sebagai *composite key* pada *database constraint* (PRIMARY KEY di tabel `events`). Hal ini sangat krusial karena *constraint* tersebut bertindak sebagai garis pertahanan absolut di level basis data untuk mencegah log yang sama dari dikirim ulang (*retry*) dicatat lebih dari satu kali.

### T5 (Bab 5): Waktu dan Ordering
Mengurutkan event yang berasal dari berbagai *node* di dalam sistem terdistribusi merupakan tantangan besar karena tidak adanya *global clock* yang mutlak, serta adanya fenomena *clock drift* pada masing-masing mesin pengirim (Coulouris et al., 2011). Pengurutan logis seperti algoritma Lamport Clocks dapat digunakan, tetapi dalam agregator log praktis, pendekatan tersebut menambah kompleksitas *overhead* yang terlalu besar. Sebagai gantinya, sistem ini mengandalkan *timestamp* pragmatis berbasis waktu riil UTC dari *publisher* saat pesan dibuat, dipadukan dengan *monotonic counter* yang dihasilkan oleh basis data PostgreSQL (melalui urutan `id` auto-increment atau tanggal simpan `processed_at`) (van Steen & Tanenbaum, 2023). Batasannya adalah ketidakakuratan mutlak: event dari dua *publisher* berbeda dapat tercatat secara tidak berurutan (*out-of-order*) jika jam salah satu mesin lebih lambat. Dampak ini ditoleransi karena agregasi log umumnya lebih mementingkan konsistensi keberadaan data (*eventual consistency*) dan keunikan pesan, dibandingkan *strict temporal ordering*.

### T6 (Bab 6): Toleransi Kegagalan
Sistem terdistribusi selalu rentan terhadap berbagai bentuk kegagalan, terutama *crash failures* (komponen mati seketika) dan *omission failures* (pesan hilang di jaringan) (van Steen & Tanenbaum, 2023). Untuk menjaga ketahanan sistem (*fault tolerance*), desain ini mengimplementasikan beberapa lapisan mitigasi. Pertama, mitigasi kegagalan komunikasi antara *publisher* dan broker diatasi dengan mekanisme *retry* dan *exponential backoff* untuk mencegah *network flooding*. Kedua, *Durable Broker* diterapkan dengan menggunakan Redis yang mengaktifkan fitur penyimpanan AOF/RDB secara persisten melalui volume, sehingga pesan belum terproses tidak akan lenyap ketika terjadi *restart* pada kontainer Redis. Ketiga, *Durable Dedup Store* diterapkan di sisi agregator dengan menyimpan setiap *event_id* yang berhasil diproses ke dalam basis data PostgreSQL berbasis disk. Ketika *container* agregator tiba-tiba mati dan hidup kembali (*crash recovery*), ia tidak perlu mengingat pesan apa saja yang sudah diproses di memori, melainkan cukup melempar duplikat ke PostgreSQL yang akan langsung menolaknya.

### T7 (Bab 7): Eventual Consistency
Replikasi asinkron dan pemrosesan terdistribusi yang terdekapling umumnya menghasilkan sifat *eventual consistency* dibandingkan *strong consistency* (van Steen & Tanenbaum, 2023). Pada rancangan agregator ini, *eventual consistency* sangat terasa pada *delay* propagasi data: ketika *publisher* sukses menembakkan log ke Redis (mendapat HTTP 200 OK), data tersebut tidak serta merta langsung dapat dilihat pada endpoint `/stats` atau `/events`. Ada latensi pemrosesan di dalam *consumer group* yang menarik antrean secara asinkron. Peran utama kombinasi *idempotency* dan mekanisme *deduplication* adalah memastikan bahwa terlepas dari seberapa lama data tertunda, dan terlepas dari seberapa banyak duplikat pesan yang dikirimkan karena sistem *retry* publisher, keseluruhan *state* sistem pada akhirnya akan *converge* ke satu kondisi akhir yang valid. Artinya, apabila aliran pesan telah berhenti, sistem menjamin jumlah log unik dan kalkulasi metrik agregat akan tepat dan presisi (Coulouris et al., 2011).

### T8 (Bab 8): Desain Transaksi dan ACID
Transaksi menyediakan mekanisme fundamental untuk mengeksekusi sekumpulan operasi secara atomik (ACID), sehingga sistem tetap konsisten meski terjadi *crash* atau intervensi konkuren (Coulouris et al., 2011). Dalam rancangan agregator log ini, contoh paling kritis dari penerapan transaksi adalah pembaruan statistik global, di mana sistem harus memitigasi anomali *lost-update*. *Lost-update* terjadi jika dua *worker* membaca nilai `received` yang sama (misal: 10), masing-masing menambahkannya menjadi 11 di memori, dan menyimpannya kembali, sehingga hasil akhirnya adalah 11, padahal seharusnya 12. Untuk mencegahnya, desain kami menghindari pembaruan di lapisan aplikasi (Python) dan mendelegasikan kalkulasi langsung ke dalam basis data dengan query transaksional: `UPDATE event_stats SET received = received + 1`. PostgreSQL menjalankan isolasi transaksi `READ COMMITTED` yang memicu perlindungan *row-level lock* pesimistik. Ini menjamin bahwa berapapun jumlah *worker* asinkron yang memperbarui metrik secara paralel, operasi akan dieksekusi secara antre oleh database dan tidak ada perhitungan yang hilang.

### T9 (Bab 9): Kontrol Konkurensi
Kontrol konkurensi adalah prasyarat mutlak dalam memastikan bahwa akses dan manipulasi paralel terhadap *shared resource* tidak merusak integritas data (van Steen & Tanenbaum, 2023). Strategi konkurensi sentral yang digunakan dalam desain ini adalah *idempotent write pattern* berbasis instruksi *Upsert* atau *Do Nothing* di level basis data pesimistik. Contoh penerapannya pada rancangan aplikasi kami adalah eksekusi query `INSERT INTO events ... ON CONFLICT (topic, event_id) DO NOTHING`. Ketika 4 buah *worker* asinkron mendeteksi *event_id* yang sama dan mencoba memasukkannya secara paralel ke database, PostgreSQL akan menempatkan *lock* pada indeks unik tersebut untuk transaksi tercepat. Ketiga *worker* lainnya akan secara deterministik mendeteksi konflik (*constraint violation*). Daripada melempar *fatal error*, klausa `DO NOTHING` menginstruksikan database untuk meredam konflik secara elegan tanpa membatalkan koneksi. Dengan demikian, *race condition* dapat dinetralisir tanpa memerlukan mekanisme penguncian eksternal (*distributed lock*) yang rumit.

### T10 (Bab 10–13): Keamanan Jaringan dan Persistensi
Mengelola banyak komponen terdistribusi menuntut infrastruktur orkestrasi dan observabilitas yang memadai (Coulouris et al., 2011). Proyek ini memanfaatkan *Docker Compose* untuk membungkus dan mengkoreografi seluruh layanan ke dalam satu ekosistem internal. Keamanan jaringan lokal ditegakkan dengan *virtual network bridge* yang mencegah layanan vital seperti basis data dan broker diekspos ke *port* publik *host*, membatasi celah serangan. Untuk mengatasi sifat *stateless container*, prinsip persistensi dipertahankan dengan mendefinisikan *named volumes* (`pg_data` dan `broker_data`), sehingga *lifetime* data dipisahkan dari siklus hidup *container*. Selain itu, sistem mengadopsi prinsip observabilitas yang tangguh melalui penentuan urutan inisiasi berbasis `healthcheck`. Layanan agregator tidak akan menerima lalu lintas apa pun sebelum basis data PostgreSQL diidentifikasi siap menerima instruksi secara transaksional.

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
van Steen, M., & Tanenbaum, A. S. (2023). *Distributed Systems* (4th ed.).

