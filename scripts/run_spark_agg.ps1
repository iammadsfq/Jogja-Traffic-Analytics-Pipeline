param(
    [string]$ContainerName = "jtap-spark-agg",
    [string]$NetworkName = "jogja-traffic-analytics-pipeline_default",
    [string]$KafkaServers = "kafka:29092",
    [string]$KafkaTopic = "traffic.raw",
    [string]$PostgresHost = "postgres",
    [string]$PostgresPort = "5432",
    [string]$PostgresDb = "traffic_analytics",
    [string]$PostgresUser = "traffic_user",
    [string]$PostgresPassword = "traffic_password"
)

$ErrorActionPreference = "Stop"

Write-Host "Starting Spark aggregate job container: $ContainerName"

$ErrorActionPreference = "Continue"
docker rm -f $ContainerName 2>$null | Out-Null
$ErrorActionPreference = "Stop"

$cmd = @"
pip install -q psycopg[binary]==3.2.13; /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/spark/stream_aggregate.py
"@

docker run -d --user 0 --name $ContainerName `
  --network $NetworkName `
  -v "${PWD}:/app" -w /app `
  -e KAFKA_BOOTSTRAP_SERVERS=$KafkaServers `
  -e KAFKA_TOPIC_RAW=$KafkaTopic `
  -e POSTGRES_HOST=$PostgresHost `
  -e POSTGRES_PORT=$PostgresPort `
  -e POSTGRES_DB=$PostgresDb `
  -e POSTGRES_USER=$PostgresUser `
  -e POSTGRES_PASSWORD=$PostgresPassword `
  apache/spark:3.5.1-scala2.12-java17-python3-r-ubuntu /bin/bash -lc $cmd | Out-Null

Start-Sleep -Seconds 6

docker ps --filter "name=$ContainerName" --format "table {{.Names}}`t{{.Status}}" | Write-Host
Write-Host "Use: docker logs -f $ContainerName"
