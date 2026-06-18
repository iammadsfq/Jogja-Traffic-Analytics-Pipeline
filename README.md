# Jogja Traffic Analytics Pipeline

Scalable real-time traffic analytics pipeline untuk memproses metadata event deteksi objek dari banyak kamera secara event-driven.

Fokus repo ini adalah **data engineering**: Kafka → Spark Structured Streaming → TimescaleDB → Grafana, di-scale ke 100 kamera lewat mock ingestion. Sumber event di sini bersifat **mock/simulasi** (event sintetis dengan skema `traffic.raw`). Inference computer-vision dari stream CCTV nyata (YOLO, HLS, direction tracking) **di luar scope** repo ini dan dipindahkan ke proyek terpisah `../jogja-cctv-cv/`. Kedua proyek berbagi satu kontrak data: skema event `traffic.raw` di bawah.

## 1) Project Summary

Project ini berfokus pada 3 hal utama:

- Skalabilitas pemrosesan event real-time dari banyak kamera.
- Fault-tolerance dengan message buffering.
- Penyimpanan time-series yang efisien untuk query operasional dan historis.

Use case utama: monitoring volume kendaraan/orang (in/out) secara near real-time untuk dashboard operasional.

## 2) Feasibility Check (Teknis, Data, Biaya)

Status: **Feasible** untuk portfolio/prototipe di laptop standar.

Alasan:

- Arsitektur memakai komponen open-source yang mature: Kafka, PySpark, PostgreSQL + TimescaleDB, Grafana.
- Beban 100+ kamera bisa disimulasikan dengan mock ingestion tanpa menjalankan 100 model AI sekaligus.
- Data CCTV publik DIY tersedia di portal resmi: https://jogjaprov.go.id/cctv (diobservasi dapat diakses publik).
- Seluruh stack bisa dijalankan lokal via Docker Compose tanpa biaya lisensi.

Catatan realistis performa:

- Target awal throughput 500 events/detik dan latency dashboard < 3 detik masih realistis untuk mesin pengembangan yang cukup (SSD, RAM memadai).
- Jika target > 1.000 events/detik stabil, lakukan tuning batch interval Spark, partisi Kafka, dan index/hypertable TimescaleDB.

## 3) Legal & Ethics Check

Repo ini hanya memakai **event sintetis/mock** (tidak ada stream CCTV nyata, tidak ada rekaman video, tidak ada PII), sehingga tidak ada isu legal sumber data.

Catatan: jika nanti pipeline ini disambung ke sumber CCTV nyata (lihat proyek `../jogja-cctv-cv/`), guardrail legal (akses publik, larangan scraping, simpan metadata non-PII saja, kill switch) wajib ditinjau di sisi proyek tersebut sebelum scale-up.

## 4) Zero-Cost Architecture

Komponen utama:

1. **Event Source Layer (Producer)**
	- Producer Python menghasilkan event metadata deteksi objek (mock/sintetis) sesuai skema `traffic.raw`.
	- Publish event JSON ke Kafka topic `traffic.raw`.
	- Catatan: produksi event dari inference CCTV nyata ada di proyek terpisah `../jogja-cctv-cv/` dan memakai skema event yang sama.
2. **Ingestion Layer (Broker)**
	- Apache Kafka sebagai durable buffer.
3. **Stream Processing Layer**
	- PySpark Structured Streaming untuk window aggregation (contoh 1 menit).
4. **Storage Layer**
	- PostgreSQL + TimescaleDB untuk tabel time-series.
5. **Visualization Layer**
	- Grafana untuk dashboard real-time dan historical trend.

Semua komponen di atas tersedia open-source dan dapat dijalankan lokal tanpa biaya cloud.

## 5) Data Contract

Topic raw event: `traffic.raw`

```json
{
  "event_id": "uuid",
  "camera_id": "string",
  "timestamp": "iso8601_timestamp",
  "object_type": "string",
  "direction": "string",
  "confidence": 0.0,
  "metadata": {
	 "location": "string",
	 "lane_id": 1
  }
}
```

Keterangan:

- `object_type`: `car`, `motorcycle`, `person`, `bus`, `truck`
- `direction`: `in` atau `out`
- `confidence`: rentang `0.0` sampai `1.0`

## 6) Database Design

Tabel utama: `fact_traffic_events`

| Kolom | Tipe | Deskripsi |
|---|---|---|
| time | timestamptz | Waktu kejadian (time partition key) |
| camera_id | varchar | ID unik kamera |
| object_type | varchar | Kategori objek |
| direction | varchar | Arah (`in`/`out`) |
| event_count | integer | Jumlah event (umumnya 1 untuk raw) |

Rekomendasi implementasi TimescaleDB:

- Ubah tabel menjadi hypertable pada kolom `time`.
- Tambah index komposit `(camera_id, time DESC)`.
- Pertimbangkan continuous aggregate untuk metrik per menit/jam.

## 7) Scaling Strategy (Mock Ingestion)

Untuk mengatasi keterbatasan perangkat saat tidak mungkin menjalankan 100 model AI:

1. Jalankan deteksi asli dari 1-2 video sumber.
2. Simpan event metadata mentah.
3. Duplikasi event dengan mengganti `camera_id` acak ke 100 kamera virtual.
4. Atur rate kirim (misal 500-1.000 events/detik) untuk stress test Spark dan DB.

Hasil: arsitektur bisa diuji bottleneck-nya secara realistis tanpa biaya hardware tinggi.

## 8) Development Roadmap

### Phase 1 - MVP

- Docker Compose: Kafka, PostgreSQL(+TimescaleDB), Grafana.
- Producer Python sederhana untuk satu stream/video.
- Simpan event mentah ke database.

### Phase 2 - Streaming & Aggregation

- PySpark baca dari Kafka.
- Agregasi per menit (total in/out per kamera, per object).
- Dashboard Grafana real-time.

### Phase 3 - Simulation & Scaling

- Implementasi mock ingestion untuk 100 kamera virtual.
- Optimasi DB (index, hypertable tuning, retention policy).
- Health check status kamera dan monitoring pipeline.

## 9) Success Metrics

- **Latency**: data muncul di dashboard < 3 detik setelah event.
- **Throughput**: minimal 500 events/detik pada laptop standar.
- **Reliability**: at-least-once — consumer commit offset Kafka hanya setelah row durably ter-commit ke DB, sehingga tidak ada data hilang saat komponen selain Kafka restart sementara (duplikat mungkin terjadi saat crash di antara commit DB dan commit offset; dedup via `event_id` = future work).

## 10) Sumber Data

Repo ini memakai **event sintetis/mock**:

- `src/producer/event_producer.py` — generate event single-stream sesuai skema `traffic.raw`.
- `src/producer/mock_duplicator.py` — duplikasi event seed ke N kamera virtual untuk uji skala.

Untuk produksi event dari stream CCTV nyata (OpenCV + YOLO + direction tracking), gunakan proyek terpisah `../jogja-cctv-cv/` yang menulis ke topic `traffic.raw` dengan skema yang sama, sehingga seluruh komponen downstream pipeline ini tidak perlu berubah.

## 11) Minimal Run Plan (No Cost)

1. Jalankan infrastruktur lokal via Docker Compose.
2. Start producer (single stream/video) -> kirim ke Kafka.
3. Start Spark streaming job -> tulis ke TimescaleDB.
4. Hubungkan Grafana ke PostgreSQL -> buat panel latency, throughput, in/out.
5. Aktifkan mock duplicator untuk uji skala.

Catatan Phase 2 (Spark sink):

- Job `src/spark/stream_aggregate.py` melakukan agregasi 1 menit dan upsert ke tabel `fact_traffic_agg_1m`.
- Saat menjalankan `spark-submit`, sertakan paket Kafka connector:
	- `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/spark/stream_aggregate.py`

### Scaffold yang sudah disiapkan

- `docker-compose.yml`
- `.env.example`
- `requirements.txt`
- `requirements-runtime.txt`
- `infra/postgres/init/001_init.sql`
- `src/producer/event_producer.py`
- `src/producer/mock_duplicator.py`
- `src/consumer/raw_to_postgres.py`
- `src/spark/stream_aggregate.py`
- `scripts/smoke_test_e2e.ps1`
- `scripts/run_spark_agg.ps1`
- `scripts/scale_test_100cams.ps1`

### Quickstart lokal (MVP)

Prerequisite:

- Docker Desktop
- Python 3.10-3.12 direkomendasikan (3.14 sering terkendala wheel untuk paket CV/ML)

1. Siapkan environment file:
	- copy `.env.example` menjadi `.env`
2. Jalankan infrastruktur:
	- `docker compose --env-file .env up -d`
3. Install dependency Python:
	- core pipeline (termasuk Spark): `pip install -r requirements.txt`
	- runtime minimal producer/consumer: `pip install -r requirements-runtime.txt`
4. Jalankan producer event sample:
	- `python src/producer/event_producer.py`
5. Jalankan consumer Kafka ke PostgreSQL (terminal baru):
	- `python src/consumer/raw_to_postgres.py`
6. Opsional simulasi skala kamera (terminal baru):
	- `python src/producer/mock_duplicator.py`
7. Buka Grafana di `http://localhost:3000` dan koneksikan data source PostgreSQL.

Catatan kompatibilitas Windows:

- Pada Python 3.13+ di Windows, Kafka client Python tertentu dapat bermasalah.
- Gunakan script smoke test berbasis container Linux untuk verifikasi cepat:
	- `powershell -ExecutionPolicy Bypass -File scripts/smoke_test_e2e.ps1`

### Menjalankan Spark aggregate (1 perintah)

- `powershell -ExecutionPolicy Bypass -File scripts/run_spark_agg.ps1`

Script ini otomatis:

- Menjalankan Spark di container Linux.
- Memasang dependency Python `psycopg` di container.
- Menambahkan Kafka connector package untuk Structured Streaming.

### Scale test 100 kamera (throughput + latency)

- `powershell -ExecutionPolicy Bypass -File scripts/scale_test_100cams.ps1`

Script ini otomatis:

- Menjalankan seed producer (`traffic.seed`) + mock duplicator 100 kamera + consumer DB.
- Mengukur rows ingested, throughput (events/s), ingestion lag, dan active camera count.

> Ingestion dari stream CCTV nyata (HLS + YOLO + direction tracking) tidak ada di repo ini — lihat proyek terpisah `../jogja-cctv-cv/`. Output proyek tersebut memakai skema event `traffic.raw` yang sama sehingga langsung kompatibel dengan pipeline ini.

### Query cek cepat di PostgreSQL

Contoh validasi data masuk:

- `SELECT COUNT(*) FROM fact_traffic_events;`
- `SELECT * FROM fact_traffic_events ORDER BY time DESC LIMIT 10;`

## 12) Disclaimer

- Project ini ditujukan untuk pembelajaran dan portfolio engineering.
- Penggunaan data CCTV harus mematuhi aturan sumber data dan hukum yang berlaku.
- Untuk deployment produksi, lakukan legal review formal dan security hardening.
