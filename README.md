# Event Aggregator — UAS Pub-Sub System

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green?logo=fastapi)](https://fastapi.tiangolo.com)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker)](https://docker.com)
[![Tests](https://img.shields.io/badge/tests-18%20passed-brightgreen)](tests/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

Sistem **Pub-Sub Log Aggregator** terdistribusi yang dibangun dengan Python, FastAPI, Redis Stream, dan PostgreSQL. Mendukung **idempotent consumer**, **deduplication persisten**, dan **transaksi/kontrol konkurensi** sepenuhnya melalui Docker Compose.

---

## Table of Contents

- [Arsitektur](#arsitektur)
- [Fitur](#fitur)
- [Tech Stack](#tech-stack)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Performa](#performa)
- [Design Decisions](#design-decisions)
- [Persistensi Data](#persistensi-data)
- [Video Demo](#video-demo)

---

## Arsitektur

```mermaid
flowchart TD
    PUB(["Publisher\nSimulator / Client"])

    subgraph AGG["Aggregator Service"]
        direction TB
        API["FastAPI\nPOST /publish\nGET /events\nGET /stats\nGET /health"]
    end

    subgraph BROKER["Broker"]
        STREAM[("Redis Stream\nevents_stream")]
    end

    subgraph CG["Consumer Group  x4 workers"]
        direction LR
        W0["worker-0"] ~~~ W1["worker-1"]
        W1 ~~~ W2["worker-2"]
        W2 ~~~ W3["worker-3"]
    end

    subgraph STORAGE["Storage"]
        direction LR
        PG[("PostgreSQL\nevents\nPRIMARY KEY\ntopic, event_id")]
        ST[("PostgreSQL\nevent_stats\nreceived\nduplicate_dropped")]
    end

    PUB -->|"POST /publish\nsingle / batch"| AGG
    AGG -->|"XADD"| BROKER
    BROKER -->|"XREADGROUP"| CG
    CG -->|"INSERT ON CONFLICT\nDO NOTHING"| PG
    CG -->|"UPDATE SET\ncount = count + 1"| ST

    style AGG fill:#1e3a5f,stroke:#4a9eff,color:#fff
    style BROKER fill:#c0392b,stroke:#e74c3c,color:#fff
    style CG fill:#1a5c38,stroke:#27ae60,color:#fff
    style STORAGE fill:#4a235a,stroke:#9b59b6,color:#fff
    style PUB fill:#2c3e50,stroke:#7f8c8d,color:#fff
```

**Flow:**
1. **Publisher** mengirim event(s) ke `POST /publish` (single atau batch)
2. **FastAPI** memvalidasi skema event via Pydantic, lalu push ke **Redis Stream** (`XADD`)
3. **Consumer workers** (4 paralel) membaca stream dengan `XREADGROUP`
4. Setiap worker mencoba `INSERT ... ON CONFLICT (topic, event_id) DO NOTHING` ke PostgreSQL
5. Postgres menjamin hanya satu worker yang berhasil insert — race condition aman
6. Stats diperbarui secara transaksional: `UPDATE event_stats SET received = received + 1`

---

## Fitur

- ✓ **Idempotent Deduplication** — `(topic, event_id)` unik dijamin oleh PRIMARY KEY PostgreSQL
- ✓ **Multi-worker Concurrent Consumer** — 4 worker paralel via Redis Stream Consumer Group
- ✓ **Persistent Storage** — PostgreSQL dengan named Docker Volume (`pg_data`)
- ✓ **Batch & Single Ingestion** — Menerima satu event atau array event per request
- ✓ **At-Least-Once Delivery** — Duplikat diterima di API, di-drop oleh consumer
- ✓ **Real-Time Statistics** — received, unique, duplicates, topics, uptime (persisten di DB)
- ✓ **Schema Validation** — Otomatis 422 untuk event yang tidak valid
- ✓ **Crash Tolerance** — Setelah restart container, dedup state tetap di PostgreSQL
- ✓ **No Race Condition** — Dibuktikan oleh test: 50 concurrent insert → hanya 1 berhasil
- ✓ **Audit Logging** — Setiap event duplicate di-log dengan level WARNING
- ✓ **Health Check** — Endpoint `/health` untuk readiness probe Docker
- ✓ **Non-root Docker** — Container berjalan sebagai user `appuser`

---

## Tech Stack

| Komponen | Teknologi |
|---|---|
| **Framework** | [FastAPI](https://fastapi.tiangolo.com) 0.104+ |
| **Runtime** | Python 3.11+ dengan asyncio |
| **Dedup Store** | PostgreSQL 16 (`asyncpg` pool, `ON CONFLICT DO NOTHING`) |
| **Message Broker** | Redis 7 (Stream + Consumer Group) |
| **Stats Store** | PostgreSQL (transaksional `UPDATE SET count = count + 1`) |
| **Validation** | Pydantic v2 |
| **Server** | Uvicorn (ASGI) |
| **Testing** | pytest + pytest-asyncio + httpx AsyncClient |
| **Container** | Docker (python:3.11-slim, non-root user) |

---

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) + Docker Compose
- (Opsional) Python 3.11+ untuk development lokal

### Docker Compose (Direkomendasikan)

Menjalankan seluruh stack: aggregator, publisher, Redis broker, dan PostgreSQL storage.

```bash
docker compose up --build
```

Setelah semua service healthy, cek di terminal lain:

```bash
# Health check
curl http://localhost:8080/health

# Statistik deduplikasi
curl http://localhost:8080/stats

# Semua event unik
curl http://localhost:8080/events

# Filter by topic
curl http://localhost:8080/events?topic=orders
```

Untuk menghentikan (data tetap tersimpan di volume):
```bash
docker compose down
```

Untuk menghentikan dan hapus semua data:
```bash
docker compose down -v
```

### Lokal (Tanpa Docker) — Untuk Testing

Pastikan Postgres dan Redis berjalan di localhost, lalu:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Set environment variables
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres
export BROKER_URL=redis://localhost:6379

uvicorn src.main:app --reload --port 8080
```

---

## API Reference

### GET /health

Liveness/readiness check untuk Docker healthcheck.

**Response** `200 OK`:
```json
{"status": "ok"}
```

---

### POST /publish

Ingest satu atau lebih event ke sistem.

**Request Body** (single event):
```json
{
  "topic": "orders",
  "event_id": "ord-001",
  "timestamp": "2024-01-01T00:00:00Z",
  "source": "order-service",
  "payload": {"order_id": 123, "amount": 49.99}
}
```

**Request Body** (batch array):
```json
[
  {"topic": "orders", "event_id": "ord-001", "timestamp": "2024-01-01T00:00:00Z", "source": "order-svc", "payload": {}},
  {"topic": "payments", "event_id": "pay-001", "timestamp": "2024-01-01T00:00:01Z", "source": "pay-svc", "payload": {}}
]
```

**Response** `200 OK`:
```json
{"received": 2, "status": "ok"}
```

**Schema Event:**

| Field | Type | Required | Deskripsi |
|---|---|---|---|
| `topic` | string | ✓ | Kategori event (e.g. "orders", "payments") |
| `event_id` | string | ✓ | Identifier unik dalam topic (UUID direkomendasikan) |
| `timestamp` | string (ISO 8601) | ✓ | Waktu event terjadi |
| `source` | string | ✓ | Nama service pengirim |
| `payload` | object | ✗ | Data arbitrer event (default: `{}`) |

> **Note:** Event duplikat (same `topic` + `event_id`) diterima di API level tapi **di-drop** oleh consumer. Tidak ada error yang dikembalikan.

---

### GET /events

Ambil daftar event unik yang telah diproses, opsional filter by topic.

**Parameters:**

| Parameter | Type | Required | Deskripsi |
|---|---|---|---|
| `topic` | string | ✗ | Filter by topic. Kosong = semua event. |

**Response** `200 OK`:
```json
[
  {
    "topic": "orders",
    "event_id": "ord-001",
    "timestamp": "2024-01-01T00:00:00Z",
    "source": "order-service",
    "payload": {}
  }
]
```

---

### GET /stats

Statistik real-time sistem (persisten di PostgreSQL, tidak hilang setelah restart).

**Response** `200 OK`:
```json
{
  "received": 26000,
  "unique_processed": 20000,
  "duplicate_dropped": 6000,
  "topics": ["orders", "payments", "logs", "analytics", "notifications"],
  "uptime": 12.34
}
```

| Field | Deskripsi |
|---|---|
| `received` | Total event diterima (termasuk duplikat) |
| `unique_processed` | Event yang berhasil disimpan (unik) |
| `duplicate_dropped` | Duplikat yang terdeteksi dan di-drop |
| `topics` | Daftar topic berbeda yang pernah masuk |
| `uptime` | Detik sejak service terakhir dimulai |

---

## Project Structure

```
pub-sub/
├── aggregator/
│   └── Dockerfile              # Image aggregator (non-root user)
├── publisher/
│   └── Dockerfile              # Image publisher (non-root user)
├── src/
│   ├── __init__.py
│   ├── main.py                 # FastAPI app, routes, lifespan
│   ├── models.py               # Pydantic models (Event, PublishResponse, StatsResponse)
│   ├── dedup_store.py          # PostgreSQL deduplication store (asyncpg)
│   ├── stats.py                # Persistent stats di PostgreSQL (transaksional)
│   ├── consumer.py             # Multi-worker consumer via Redis Stream
│   └── publisher.py            # Stress-test simulator (25k events, concurrent)
├── tests/
│   ├── __init__.py
│   ├── conftest.py             # pytest async fixtures (DB + Redis)
│   ├── test_api.py             # API endpoint & stats consistency (5 tests)
│   ├── test_concurrency.py     # Race condition & lost-update proof (2 tests)
│   ├── test_dedup.py           # Deduplication + consumer integration (3 tests)
│   ├── test_persistence.py     # Crash recovery via reconnect (1 test)
│   ├── test_schema.py          # Schema validation (6 tests)
│   └── test_stress.py          # 20k events, 30% dup stress test (1 test)
├── docker-compose.yml          # 4 services: aggregator, publisher, broker, storage
├── requirements.txt
├── laporan.md                  # Laporan teori T1-T10 dengan sitasi APA 7th
└── README.md
```

---

## Testing

Proyek memiliki **18 unit/integration tests** yang mencakup semua area fungsional.

### Setup test dependencies

```bash
# Start test Postgres & Redis
docker run -d --name test_postgres \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres \
  -p 5432:5432 postgres:16-alpine

docker run -d --name test_redis -p 6379:6379 redis:7-alpine

# Aktifkan venv dan install dependencies
source .venv/bin/activate
pip install -r requirements.txt
```

### Jalankan tests

```bash
# Semua tests (kecuali stress test yang berat)
pytest tests/ -v --ignore=tests/test_stress.py

# Semua tests termasuk stress test (butuh ~2 menit)
pytest tests/ -v
```

| File | Tests | Cakupan |
|---|---|---|
| `test_api.py` | 5 | Endpoint `/events`, `/stats`, `/health` |
| `test_concurrency.py` | 2 | Race condition (50 concurrent insert), lost-update |
| `test_dedup.py` | 3 | Deduplikasi, consumer integration |
| `test_persistence.py` | 1 | Crash recovery — reconnect tolak event lama |
| `test_schema.py` | 6 | Validasi skema, missing fields, batch |
| `test_stress.py` | 1 | 20.000 event + 30% duplikat (≤120 detik) |

---

## Performa

Hasil stress test: **26.000 event (20.000 unik + 6.000 duplikat / 30%)**:

| Metrik | Nilai |
|---|---|
| Total event dikirim | 26.000 |
| Unique event tersimpan | 20.000 |
| Duplikat di-drop | 6.000 |
| Duplicate rate | 30% |
| Waktu total (local test) | ~117 detik |
| Worker count | 4 paralel |
| Konsistensi data akhir | ✓ `unique + dropped == received` |

---

## Design Decisions

### Idempotency & Deduplication
- `INSERT INTO events ... ON CONFLICT (topic, event_id) DO NOTHING` — atomik di level database
- PRIMARY KEY `(topic, event_id)` sebagai constraint — jaminan level storage, bukan aplikasi
- Dua worker yang insert event sama secara bersamaan: Postgres beri row-lock ke satu, yang lain mendapat rowcount=0

### Concurrency Control
- **Redis Stream + Consumer Group**: load balancing message ke N workers secara adil
- **Isolation Level: READ COMMITTED** (default Postgres) — cukup karena INSERT row-level lock sudah atomik
- Trade-off: READ COMMITTED bisa ada *phantom reads* pada SELECT, tapi tidak relevan untuk insert-only dedup pattern

### Stats Update (No Lost-Update)
- `UPDATE event_stats SET received = received + $1` — Postgres mengambil row-lock saat UPDATE
- Tidak ada lost-update meski 100 coroutine update bersamaan (dibuktikan oleh `test_isolation_level_no_lost_update`)

### Ordering
- Total ordering **tidak diperlukan** — setiap event independen
- Partial ordering diberikan oleh `processed_at` timestamp di PostgreSQL

### At-Least-Once Delivery
- Publisher boleh kirim duplikat — consumer tidak protes, hanya di-drop
- `XACK` di Redis dikirim setelah berhasil proses (termasuk duplikat yang di-ignore) — mencegah pesan terjebak di pending list

---

## Persistensi Data

Semua data persisten dikelola via **Docker named volumes**:

| Volume | Lokasi di Container | Isi |
|---|---|---|
| `pg_data` | `/var/lib/postgresql/data` | Tabel `events` (log unik) dan `event_stats` (metrik) |
| `broker_data` | `/data` | Redis AOF/RDB (backup stream) |

Data **tidak hilang** meski container dihapus dan dibuat ulang:
```bash
docker compose down && docker compose up  # data tetap ada
docker compose down -v                    # hapus semua data (reset total)
```

---

## Video Demo

[![YouTube Demo](https://img.shields.io/badge/YouTube-Watch-red?logo=youtube)]()

<!-- TODO: Replace with actual YouTube link -->
*Demo minimal 25 menit mencakup:*

- Menjalankan stack lengkap dengan Docker Compose
- Mempublish event (single dan batch)
- Verifikasi deduplikasi dengan event duplikat
- Mengecek statistics via `/stats`
- Query events via `/events`
- Menjalankan 18 unit tests
- Stress test (20k+ events)
- Penjelasan arsitektur dan design decisions
