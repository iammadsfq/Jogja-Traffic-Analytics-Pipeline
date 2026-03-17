param(
    [int]$DurationSeconds = 60,
    [int]$SeedEventsPerSecond = 6,
    [int]$MockCameraCount = 100,
    [int]$DuplicateFactor = 8,
    [int]$MaxOutEps = 800
)

$ErrorActionPreference = "Stop"

$network = "jogja-traffic-analytics-pipeline_default"
$consumerName = "jtap-consumer-scale"
$seedName = "jtap-producer-seed"
$dupName = "jtap-duplicator-scale"

Write-Host "[1/6] Ensuring core services are up..."
docker compose --env-file .env up -d | Out-Null

Write-Host "[2/6] Cleaning previous scale-test containers..."
$ErrorActionPreference = "Continue"
docker rm -f $consumerName $seedName $dupName 2>$null | Out-Null
$ErrorActionPreference = "Stop"

$countBefore = [int64](docker exec jtap-postgres psql -U traffic_user -d traffic_analytics -t -A -c "SELECT COUNT(*) FROM fact_traffic_events;")
Write-Host "Rows before test: $countBefore"

Write-Host "[3/6] Starting consumer, seed producer, and 100-camera duplicator..."
docker run -d --rm --name $consumerName `
  --network $network `
  -v "${PWD}:/app" -w /app `
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 `
  -e KAFKA_TOPIC_RAW=traffic.raw `
  -e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 `
  -e POSTGRES_DB=traffic_analytics -e POSTGRES_USER=traffic_user `
  -e POSTGRES_PASSWORD=traffic_password -e DB_WRITE_BATCH_SIZE=200 `
  python:3.13-slim sh -c "pip install -q -r requirements-runtime.txt; python src/consumer/raw_to_postgres.py" | Out-Null

docker run -d --rm --name $seedName `
  --network $network `
  -v "${PWD}:/app" -w /app `
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 `
  -e KAFKA_TOPIC_RAW=traffic.seed `
  -e CAMERA_ID=seed_001 -e CAMERA_LOCATION=seed-source -e CAMERA_LANE_ID=1 `
  -e EVENTS_PER_SECOND=$SeedEventsPerSecond `
  python:3.13-slim sh -c "pip install -q -r requirements-runtime.txt; python src/producer/event_producer.py" | Out-Null

docker run -d --rm --name $dupName `
  --network $network `
  -v "${PWD}:/app" -w /app `
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 `
  -e KAFKA_TOPIC_SOURCE=traffic.seed `
  -e KAFKA_TOPIC_TARGET=traffic.raw `
  -e MOCK_CAMERA_COUNT=$MockCameraCount `
  -e DUPLICATE_FACTOR=$DuplicateFactor `
  -e MAX_OUT_EPS=$MaxOutEps `
  -e MOCK_CAMERA_PREFIX=cam `
  python:3.13-slim sh -c "pip install -q -r requirements-runtime.txt; python src/producer/mock_duplicator.py" | Out-Null

Write-Host "[4/6] Running load for $DurationSeconds seconds..."
Start-Sleep -Seconds $DurationSeconds

Write-Host "[5/6] Collecting metrics..."
$countAfter = [int64](docker exec jtap-postgres psql -U traffic_user -d traffic_analytics -t -A -c "SELECT COUNT(*) FROM fact_traffic_events;")
$delta = $countAfter - $countBefore
$throughput = if ($DurationSeconds -gt 0) { [math]::Round($delta / $DurationSeconds, 2) } else { 0 }

$lagSec = docker exec jtap-postgres psql -U traffic_user -d traffic_analytics -t -A -c "SELECT COALESCE(ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(time)))::numeric, 3), 0) FROM fact_traffic_events;"
$activeCameras = docker exec jtap-postgres psql -U traffic_user -d traffic_analytics -t -A -c "SELECT COUNT(DISTINCT camera_id) FROM fact_traffic_events WHERE time >= NOW() - INTERVAL '$DurationSeconds seconds';"

Write-Host ""
Write-Host "=== Scale Test Result ==="
Write-Host "Duration               : $DurationSeconds s"
Write-Host "Rows before            : $countBefore"
Write-Host "Rows after             : $countAfter"
Write-Host "Rows ingested          : $delta"
Write-Host "Approx throughput      : $throughput events/s"
Write-Host "Ingestion latency (lag): $lagSec s"
Write-Host "Active cameras window  : $activeCameras"

Write-Host "[6/6] Cleaning test containers..."
$ErrorActionPreference = "Continue"
docker rm -f $consumerName $seedName $dupName 2>$null | Out-Null
$ErrorActionPreference = "Stop"

Write-Host "Done."
