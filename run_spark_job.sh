#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$SCRIPT_DIR/docker"
ENV_FILE="$DOCKER_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE"
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

cd "$DOCKER_DIR"

echo "[GitPulse] Starting Docker services..."
docker compose up -d postgres minio spark-master spark-worker

echo "[GitPulse] Waiting for Spark master to be ready..."
for _ in $(seq 1 30); do
  if docker compose ps spark-master | grep -q "Up"; then
    break
  fi
  sleep 2
done

INPUT_FILE="${1:-}"
if [[ -z "$INPUT_FILE" ]]; then
  mapfile -t matches < <(find "$SCRIPT_DIR/data/gharchive" -maxdepth 1 -type f \( -name "*.json.gz" -o -name "*.json" \) | sort)
  if [[ ${#matches[@]} -eq 0 ]]; then
    echo "No sample GHArchive files found in $SCRIPT_DIR/data/gharchive"
    echo "Place a .json.gz file there or pass one as an argument."
    exit 1
  fi
  INPUT_FILE="${matches[0]}"
fi

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE"
  exit 1
fi

INPUT_BASENAME="$(basename "$INPUT_FILE")"
CONTAINER_INPUT_PATH="/tmp/gharchive/$INPUT_BASENAME"
CONTAINER_REPOS_PATH="/tmp/curated_repos.txt"

for container in gitpulse-spark-master gitpulse-spark-worker; do
  docker exec "$container" mkdir -p /tmp/gharchive
  docker cp "$INPUT_FILE" "$container:$CONTAINER_INPUT_PATH"
  docker cp "$SCRIPT_DIR/curated_repos.txt" "$container:$CONTAINER_REPOS_PATH"
done

echo "[GitPulse] Running Spark job for $INPUT_FILE"
docker compose exec -T -e HOME=/tmp/spark-home \
  -e PYTHONPATH=/tmp/spark-home/.python-packages \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY="${MINIO_ROOT_USER}" \
  -e MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD}" \
  -e PG_HOST=postgres \
  -e PG_PORT=5432 \
  -e PG_DB="${PG_DB}" \
  -e PG_USER="${PG_USER}" \
  -e PG_PASSWORD="${PG_PASSWORD}" \
  -e GHARCHIVE_INPUT_PATH="$CONTAINER_INPUT_PATH" \
  -e CURATED_REPOS_PATH="$CONTAINER_REPOS_PATH" \
  spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/spark-home/.ivy2 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 \
  /opt/spark_jobs/spark_job.py

echo "[GitPulse] Spark job finished."
echo "[GitPulse] Open http://localhost:8501 for the dashboard."
