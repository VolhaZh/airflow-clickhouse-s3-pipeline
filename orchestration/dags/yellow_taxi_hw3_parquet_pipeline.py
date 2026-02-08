"""
Scheduled MinIO + ClickHouse taxi pipeline (HW3).

Loads Yellow Taxi Parquet data for January - June 2024.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import boto3
import pendulum
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import get_current_context

DATA_DIR = Path("/opt/airflow/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_ARGS = {
    "owner": "data_engineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _minio_config() -> dict:
    return {
        "access_key": Variable.get("ACCESS_KEY_ID", default_var="minio-user"),
        "secret_key": Variable.get("SECRET_KEY_ID", default_var="minio-password"),
        "region": Variable.get("REGION", default_var="us-east-1"),
        "endpoint": Variable.get("ENDPOINT_URL", default_var="http://object-store:9000"),
        "bucket": Variable.get("BUCKET_NAME", default_var="zoomcamp-hw3"),
        "dataset": Variable.get("DATASET", default_var="zoomcamp"),
    }


with DAG(
    dag_id="yellow_taxi_hw3_parquet",
    default_args=DEFAULT_ARGS,
    description="Monthly load of Yellow Taxi Parquet data (Jan-Jun 2024) to MinIO and ClickHouse",
    schedule="0 0 1 * *",
    catchup=True,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2024, 7, 1, tz="UTC"),
    tags=["zoomcamp", "taxi", "minio", "clickhouse", "scheduled", "hw3"],
) as dag:

    @task
    def build_context() -> dict:
        context = get_current_context()
        logical_date = context["logical_date"]

        if logical_date.year != 2024 or logical_date.month > 6:
            raise ValueError("yellow_taxi_hw3_parquet supports only Jan-Jun 2024 runs.")

        taxi = "yellow"
        year = logical_date.year
        month = f"{logical_date.month:02d}"
        file_name = f"{taxi}_tripdata_{year}-{month}.parquet"

        return {
            "taxi": taxi,
            "year": year,
            "month": month,
            "file_name": file_name,
            "source_url": (
                "https://d37ci6vzurychx.cloudfront.net/trip-data/"
                f"{file_name}"
            ),
        }

    @task
    def download_file(meta: dict) -> str:
        parquet_path = DATA_DIR / meta["file_name"]

        with requests.get(meta["source_url"], stream=True, timeout=60) as response:
            response.raise_for_status()
            with open(parquet_path, "wb") as parquet_file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        parquet_file.write(chunk)

        return str(parquet_path)

    @task
    def ensure_bucket() -> str:
        cfg = _minio_config()
        client = boto3.client(
            "s3",
            endpoint_url=cfg["endpoint"],
            aws_access_key_id=cfg["access_key"],
            aws_secret_access_key=cfg["secret_key"],
            region_name=cfg["region"],
        )

        bucket = cfg["bucket"]
        try:
            client.head_bucket(Bucket=bucket)
        except Exception:
            client.create_bucket(Bucket=bucket)
        return bucket

    @task
    def upload_to_minio(meta: dict, bucket: str, parquet_path: str) -> str:
        cfg = _minio_config()
        client = boto3.client(
            "s3",
            endpoint_url=cfg["endpoint"],
            aws_access_key_id=cfg["access_key"],
            aws_secret_access_key=cfg["secret_key"],
            region_name=cfg["region"],
        )

        minio_key = f"{bucket}_{meta['file_name']}"
        client.upload_file(parquet_path, bucket, minio_key)
        return minio_key

    @task
    def create_clickhouse_tables(meta: dict, minio_key: str) -> None:
        cfg = _minio_config()
        conn = BaseHook.get_connection("clickhouse_default")

        from clickhouse_driver import Client

        client = Client(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            database=conn.schema or "default",
        )

        dataset = cfg["dataset"]
        table_base = f"{dataset}.{meta['taxi']}_tripdata_{meta['year']}_{meta['month']}_hw3"
        s3_path = f"{cfg['endpoint']}/{cfg['bucket']}/{minio_key}"

        client.execute(f"CREATE DATABASE IF NOT EXISTS {dataset}")

        create_ext_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_base}_ext (
            VendorID Nullable(Int64),
            tpep_pickup_datetime Nullable(DateTime64),
            tpep_dropoff_datetime Nullable(DateTime64),
            passenger_count Nullable(Int64),
            trip_distance Nullable(Float64),
            RatecodeID Nullable(Int64),
            store_and_fwd_flag Nullable(String),
            PULocationID Nullable(Int64),
            DOLocationID Nullable(Int64),
            payment_type Nullable(Int64),
            fare_amount Nullable(Float64),
            extra Nullable(Float64),
            mta_tax Nullable(Float64),
            tip_amount Nullable(Float64),
            tolls_amount Nullable(Float64),
            improvement_surcharge Nullable(Float64),
            total_amount Nullable(Float64),
            congestion_surcharge Nullable(Float64),
            Airport_fee Nullable(Float64)
        )
        ENGINE = S3(
            '{s3_path}',
            '{cfg["access_key"]}',
            '{cfg["secret_key"]}',
            'Parquet'
        )
        SETTINGS input_format_parquet_allow_missing_columns = 1
        """

        create_final_sql = f"""
        CREATE TABLE IF NOT EXISTS {dataset}.yellow_tripdata_2024_all_hw3 (
            VendorID Nullable(Int64),
            tpep_pickup_datetime Nullable(DateTime64),
            tpep_dropoff_datetime Nullable(DateTime64),
            passenger_count Nullable(Int64),
            trip_distance Nullable(Float64),
            RatecodeID Nullable(Int64),
            store_and_fwd_flag Nullable(String),
            PULocationID Nullable(Int64),
            DOLocationID Nullable(Int64),
            payment_type Nullable(Int64),
            fare_amount Nullable(Float64),
            extra Nullable(Float64),
            mta_tax Nullable(Float64),
            tip_amount Nullable(Float64),
            tolls_amount Nullable(Float64),
            improvement_surcharge Nullable(Float64),
            total_amount Nullable(Float64),
            congestion_surcharge Nullable(Float64),
            Airport_fee Nullable(Float64)
        )
        ENGINE = MergeTree
        ORDER BY (tpep_pickup_datetime, tpep_dropoff_datetime)
        SETTINGS allow_nullable_key = 1
        """

        insert_sql = f"""
        INSERT INTO {dataset}.yellow_tripdata_2024_all_hw3 (
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            RatecodeID,
            store_and_fwd_flag,
            PULocationID,
            DOLocationID,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            Airport_fee
        )
        SELECT
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            RatecodeID,
            store_and_fwd_flag,
            PULocationID,
            DOLocationID,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            Airport_fee
        FROM {table_base}_ext
        """

        client.execute(create_ext_sql)
        client.execute(create_final_sql)
        client.execute(insert_sql)

    meta = build_context()
    parquet_path = download_file(meta)
    bucket = ensure_bucket()
    minio_key = upload_to_minio(meta, bucket, parquet_path)
    create_clickhouse_tables(meta, minio_key)


