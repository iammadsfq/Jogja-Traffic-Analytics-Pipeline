# Jogja Traffic Analytics Pipeline

Scalable real-time traffic and crowd analytics pipeline untuk memproses metadata deteksi objek dari banyak stream CCTV secara event-driven.

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

Status: **Bisa dibuat legal-compliant**, dengan guardrail berikut.

Penting: dokumen ini bukan nasihat hukum. Untuk produksi, tetap verifikasi kebijakan resmi sumber data.

Prinsip kepatuhan:

- Hanya simpan **metadata agregat** dan event analitik (bukan rekaman video mentah).
- Jangan simpan informasi identitas personal.
- Terapkan rate limit dan jadwal pengambilan data yang wajar.
- Hormati ketentuan penggunaan situs/sumber data. Jika syarat penggunaan tidak jelas, mintakan izin tertulis ke pengelola data.
- Sediakan kill switch untuk mematikan ingestion jika ada permintaan penghentian.

Checklist go/no-go sebelum running live ingestion:

- [ ] Halaman/sumber data menyatakan akses publik untuk pemantauan.
- [ ] Tidak ada larangan eksplisit scraping/restreaming pada terms/disclaimer.
- [ ] Pipeline hanya menyimpan metadata non-PII.
- [ ] Ada contact path untuk klarifikasi izin penggunaan data.

Jika satu saja gagal, gunakan mode simulasi/offline dataset sampai legal clear.

## 4) Zero-Cost Architecture

Komponen utama:

1. **Inference Layer (Producer)**
	- Python (OpenCV + YOLOv8) ekstraksi metadata objek per frame/interval.
	- Publish event JSON ke Kafka topic `traffic.raw`.
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
- **Reliability**: tidak ada data hilang saat komponen selain Kafka restart sementara.

## 10) Getting Data (Praktis & Aman)

Sumber utama (target):

- Portal CCTV Pemda DIY: https://jogjaprov.go.id/cctv

Fallback jika ada kendala akses/izin:

- Gunakan video publik/open dataset lalu jalankan pipeline dalam mode simulasi.
- Tetap pakai skema event yang sama agar komponen downstream tidak berubah.

Best practice ingestion:

- Sampling frame seperlunya (misal 1-2 FPS) agar ringan.
- Simpan hanya metadata event.
- Tambahkan retry + dead-letter strategy di producer/consumer.

## 11) Minimal Run Plan (No Cost)

1. Jalankan infrastruktur lokal via Docker Compose.
2. Start producer (single stream/video) -> kirim ke Kafka.
3. Start Spark streaming job -> tulis ke TimescaleDB.
4. Hubungkan Grafana ke PostgreSQL -> buat panel latency, throughput, in/out.
5. Aktifkan mock duplicator untuk uji skala.

### Scaffold yang sudah disiapkan

- `docker-compose.yml`
- `.env.example`
- `requirements.txt`
- `requirements-inference.txt`
- `infra/postgres/init/001_init.sql`
- `src/producer/event_producer.py`
- `src/producer/mock_duplicator.py`
- `src/consumer/raw_to_postgres.py`
- `src/spark/stream_aggregate.py`

### Quickstart lokal (MVP)

Prerequisite:

- Docker Desktop
- Python 3.10-3.12 direkomendasikan (3.14 sering terkendala wheel untuk paket CV/ML)

1. Siapkan environment file:
	- copy `.env.example` menjadi `.env`
2. Jalankan infrastruktur:
	- `docker compose --env-file .env up -d`
3. Install dependency Python:
	- `pip install -r requirements.txt`
	- opsional model inference: `pip install -r requirements-inference.txt`
4. Jalankan producer event sample:
	- `python src/producer/event_producer.py`
5. Jalankan consumer Kafka ke PostgreSQL (terminal baru):
	- `python src/consumer/raw_to_postgres.py`
6. Opsional simulasi skala kamera (terminal baru):
	- `python src/producer/mock_duplicator.py`
7. Buka Grafana di `http://localhost:3000` dan koneksikan data source PostgreSQL.

### Query cek cepat di PostgreSQL

Contoh validasi data masuk:

- `SELECT COUNT(*) FROM fact_traffic_events;`
- `SELECT * FROM fact_traffic_events ORDER BY time DESC LIMIT 10;`

## 12) Disclaimer

- Project ini ditujukan untuk pembelajaran dan portfolio engineering.
- Penggunaan data CCTV harus mematuhi aturan sumber data dan hukum yang berlaku.
- Untuk deployment produksi, lakukan legal review formal dan security hardening.
