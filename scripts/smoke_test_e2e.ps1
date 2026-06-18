$ErrorActionPreference = "Stop"

Write-Host "[1/4] Starting core infrastructure..."
docker compose --env-file .env up -d | Out-Null

Write-Host "[2/4] Running temporary consumer and producer containers..."
$oldErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
docker rm -f jtap-consumer-test jtap-producer-test *> $null
$ErrorActionPreference = $oldErrorActionPreference

$workspace = (Get-Location).Path

docker run -d --name jtap-consumer-test `
  --network jogja-traffic-analytics-pipeline_default `
  -v "${workspace}:/app" -w /app `
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 `
  -e KAFKA_TOPIC_RAW=traffic.raw `
  -e POSTGRES_HOST=postgres `
  -e POSTGRES_PORT=5432 `
  -e POSTGRES_DB=traffic_analytics `
  -e POSTGRES_USER=traffic_user `
  -e POSTGRES_PASSWORD=traffic_password `
  -e DB_WRITE_BATCH_SIZE=10 `
  python:3.13-slim sh -c "pip install -q -r requirements-runtime.txt; python src/consumer/raw_to_postgres.py" | Out-Null

docker run -d --name jtap-producer-test `
  --network jogja-traffic-analytics-pipeline_default `
  -v "${workspace}:/app" -w /app `
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 `
  -e KAFKA_TOPIC_RAW=traffic.raw `
  -e EVENTS_PER_SECOND=30 `
  python:3.13-slim sh -c "pip install -q -r requirements-runtime.txt; python src/producer/event_producer.py" | Out-Null

Write-Host "[3/4] Waiting for events to be ingested (polling up to 90s)..."
# Containers pip-install before the python scripts start, so a fixed sleep is
# unreliable. Poll the row count instead: fast when ready, no false negative.
$oldEap = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$deadline = (Get-Date).AddSeconds(90)
$count = 0
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 3
    $raw = docker exec jtap-postgres psql -U traffic_user -d traffic_analytics -t -A -c "SELECT COUNT(*) FROM fact_traffic_events;"
    if ($raw -match '^\d+$') { $count = [int]$raw }
    Write-Host "  ...rows so far: $count"
    if ($count -gt 0) { break }
}
$ErrorActionPreference = $oldEap

Write-Host "[4/4] Result"
if ($count -gt 0) {
    Write-Host "PASS: total_events = $count"
} else {
    Write-Host "FAIL: no events ingested within 90s timeout"
}

Write-Host "Cleaning temporary test containers..."
docker rm -f jtap-producer-test jtap-consumer-test | Out-Null

if ($count -le 0) { exit 1 }
Write-Host "Done."
