# Airflow + ClickHouse + MinIO Taxi Pipeline

## Overview

This project orchestrates monthly NYC TLC taxi data loads with Apache Airflow:

- Download taxi CSVs from the public TLC dataset
- Upload files to MinIO (S3-compatible object store)
- Create external S3 tables and load data into ClickHouse

## Requirements

- Docker and Docker Compose
- Python 3.9+ (only needed for local development)

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/....git
   cd airflow-clickhouse-s3-pipeline
   ```

2. Create the shared Docker network:
   ```bash
   docker network create ny-taxi-network
   ```

3. Start MinIO and ClickHouse:
   ```bash
   docker compose up -d
   ```

4. Start Airflow:
   ```bash
   cd airflow
   docker compose -f docker-compose.yaml up -d
   ```

5. Open the Airflow UI:
   ```
   http://localhost:8081
   ```
   - Login: `airflow`
   - Password: `airflow`

## Services and Ports

- Airflow UI: `http://localhost:8081`
- MinIO console: `http://localhost:9001`
- MinIO S3 endpoint: `http://localhost:9002`
- ClickHouse HTTP: `http://localhost:8123`
- ClickHouse native: `tcp://localhost:9000`
- Postgres (Airflow metadata): `localhost:5445`

Default credentials:

- MinIO: `minio-user` / `minio-password`
- ClickHouse: `default` / `clickhouse_password`
- Airflow: `airflow` / `airflow`

## Available DAGs

- `minio_taxi_pipeline`: one-off load of a single taxi file
- `minio_taxi_scheduled`: monthly scheduled load (all taxi types)
- `green_taxi_scheduled`: monthly load for green taxis (2020)
- `yellow_taxi_scheduled`: monthly load for yellow taxis (2020)

## Configuration

Defaults are embedded in the DAGs and can be overridden via Airflow Variables:

- `ACCESS_KEY_ID` / `SECRET_KEY_ID` / `ENDPOINT_URL`
- `BUCKET_NAME` / `DATASET`
- `REGION`

ClickHouse connection is configured in `airflow/docker-compose.yaml` via
`AIRFLOW_CONN_CLICKHOUSE_DEFAULT`.

## Backfill Example

```bash
docker exec -it airflow-standalone bash

airflow dags backfill minio_taxi_scheduled \
  --start-date "2020-02-01" \
  --end-date "2020-12-01"
```

## Logs

```bash
docker compose -f airflow/docker-compose.yaml logs -f airflow-standalone
```

## Additional Resources

- Airflow docs: https://airflow.apache.org/docs/
- ClickHouse docs: https://clickhouse.com/docs/
- MinIO docs: https://min.io/docs/