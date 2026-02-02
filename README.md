# Airflow for Coinbase Realtime Analytics

## Description

This directory contains the Apache Airflow configuration for the DE Zoomcamp module "orchestration". Airflow is used for orchestrating workflows, including:

- ETL processes for NY_taxi data data
- System monitoring
- Report generation and analytics

## Requirements

- Docker and Docker Compose
- Python 3.9+

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/....git
   cd airflow-clickhouse-s3-pipeline
   ```

2. Create necessary directories:
   ```bash
   mkdir -p orchestration/{dags,plugins,logs}
   ```

3. Start Airflow:
   ```bash
   cd docker/airflow
   docker-compose up -d
   ```

4. Open the Airflow interface:
   ```
   http://localhost:8081
   ```
   - Login: `airflow`
   - Password: `airflow`

## Available DAGs

1. **coinbase_market_data_etl**
   - Schedule: daily
   - Description: ETL process for Coinbase market data

2. **infrastructure_management**
   - Schedule: weekly
   - Description: Infrastructure management with Terraform

3. **system_monitoring**
   - Schedule: hourly
   - Description: System monitoring

## Configuration

All settings can be changed in the `.env` file. Key parameters:

- `AIRFLOW_UID` - Airflow user ID
- `POSTGRES_USER`, `POSTGRES_PASSWORD` - PostgreSQL credentials
- `CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` - ClickHouse connection settings

## Creating Custom DAGs

1. Create a new Python file in the `orchestration/dags/` directory
2. Use the standard Airflow DAG format
3. The DAG will automatically appear in the Airflow interface

## Debugging

To view logs, use:

```bash
docker-compose logs -f airflow-standalone
```

## Additional Information

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Providers](https://airflow.apache.org/docs/apache-airflow-providers/) 