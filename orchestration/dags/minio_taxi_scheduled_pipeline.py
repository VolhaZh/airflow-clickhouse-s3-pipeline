"""
Scheduled MinIO + ClickHouse taxi pipeline.

Runs on the 1st day of the month:
- 09:00 UTC: green taxi
- 10:00 UTC: yellow taxi
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
import gzip
import shutil

import boto3
import pendulum
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.models.param import Param
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
    "start_date": pendulum.datetime(2020, 1, 1, 0, 0, tz="UTC"),
}


def _minio_config() -> dict:
    return {
        "access_key": Variable.get("ACCESS_KEY_ID", default_var="minio-user"),
        "secret_key": Variable.get("SECRET_KEY_ID", default_var="minio-password"),
        "region": Variable.get("REGION", default_var="us-east-1"),
        "endpoint": Variable.get("ENDPOINT_URL", default_var="http://object-store:9000"),
        "bucket": Variable.get("BUCKET_NAME", default_var="zoomcamp"),
        "dataset": Variable.get("DATASET", default_var="zoomcamp"),
    }


with DAG(
    dag_id="minio_taxi_scheduled",
    default_args=DEFAULT_ARGS,
    description="Monthly scheduled load of NYC TLC data to MinIO and ClickHouse",
    schedule="0 0 1 * *",
    catchup=True,
    end_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    tags=["zoomcamp", "taxi", "minio", "clickhouse", "scheduled"],
    params={
        "taxi": Param("auto", enum=["auto", "yellow", "green"]),
    },
) as dag:

    @task
    def build_context() -> dict:
        context = get_current_context()
        params = context["params"]
        logical_date = context["logical_date"]

        if logical_date.year <  passenger_countendulum.datetime(2021, 1, 1, tz="UTC") or logical_date >= pendulum.datetime(2021, 8, 1, tz="UTC"):
            raise ValueError("minio_taxi_scheduled supports only 2021-03 through 2021-07 runs.")

        taxi = params["taxi"]
        if taxi == "auto":
            taxi = "green" if logical_date.hour == 9 else "yellow"

        year = logical_date.year
        month = f"{logical_date.month:02d}"
        file_name = f"{taxi}_tripdata_{year}-{month}.csv"

        return {
            "taxi": taxi,
            "year": year,
            "month": month,
            "file_name": file_name,
            "source_url": (
                "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
                f"{taxi}/{file_name}.gz"
            ),
        }

    @task
    def download_file(meta: dict) -> str:
        gz_path = DATA_DIR / f"{meta['file_name']}.gz"
        csv_path = DATA_DIR / meta["file_name"]

        with requests.get(meta["source_url"], stream=True, timeout=60) as response:
            response.raise_for_status()
            with open(gz_path, "wb") as gz_file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        gz_file.write(chunk)

        with gzip.open(gz_path, "rb") as gz_file, open(csv_path, "wb") as csv_file:
            shutil.copyfileobj(gz_file, csv_file)

        gz_path.unlink(missing_ok=True)
        return str(csv_path)

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
    def upload_to_minio(meta: dict, bucket: str, csv_path: str) -> str:
        cfg = _minio_config()
        client = boto3.client(
            "s3",
            endpoint_url=cfg["endpoint"],
            aws_access_key_id=cfg["access_key"],
            aws_secret_access_key=cfg["secret_key"],
            region_name=cfg["region"],
        )

        minio_key = f"{bucket}_{meta['file_name']}"
        client.upload_file(csv_path, bucket, minio_key)
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
        table_base = f"{dataset}.{meta['taxi']}_tripdata_{meta['year']}_{meta['month']}"
        s3_path = f"{cfg['endpoint']}/{cfg['bucket']}/{minio_key}"

        client.execute(f"CREATE DATABASE IF NOT EXISTS {dataset}")

        if meta["taxi"] == "yellow":
            create_ext_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_base}_ext (
                VendorID String,
                tpep_pickup_datetime DateTime64,
                tpep_dropoff_datetime DateTime64,
                passenger_count UInt8,
                trip_distance Float64,
                RatecodeID String,
                store_and_fwd_flag String,
                PULocationID String,
                DOLocationID String,
                payment_type UInt8,
                fare_amount Float32,
                extra Float32,
                mta_tax Float32,
                tip_amount Float32,
                tolls_amount Float32,
                improvement_surcharge Float32,
                total_amount Float32,
                congestion_surcharge Float32
            )
            ENGINE = S3(
                '{s3_path}',
                '{cfg["access_key"]}',
                '{cfg["secret_key"]}',
                'CSVWithNames'
            )
            SETTINGS input_format_with_names_use_header = 1
            """
            merge_sql = f"""
            CREATE TABLE IF NOT EXISTS {dataset}.yellow_tripdata
            ENGINE = Merge('{dataset}', '^yellow_tripdata')
            """
        else:
            create_ext_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_base}_ext (
                VendorID String,
                lpep_pickup_datetime DateTime64,
                lpep_dropoff_datetime DateTime64,
                store_and_fwd_flag String,
                RatecodeID String,
                PULocationID String,
                DOLocationID String,
                passenger_count UInt8,
                trip_distance Float64,
                fare_amount Float32,
                extra Float32,
                mta_tax Float32,
                tip_amount Float32,
                tolls_amount Float32,
                ehail_fee String,
                improvement_surcharge Float32,
                total_amount Float32,
                payment_type UInt8,
                trip_type UInt8,
                congestion_surcharge Float32
            )
            ENGINE = S3(
                '{s3_path}',
                '{cfg["access_key"]}',
                '{cfg["secret_key"]}',
                'CSVWithNames'
            )
            SETTINGS input_format_with_names_use_header = 1
            """
            merge_sql = f"""
            CREATE TABLE IF NOT EXISTS {dataset}.green_tripdata
            ENGINE = Merge('{dataset}', '^green_tripdata')
            """

        client.execute(create_ext_sql)
        client.execute(merge_sql)

    meta = build_context()
    csv_path = download_file(meta)
    bucket = ensure_bucket()
    minio_key = upload_to_minio(meta, bucket, csv_path)
    create_clickhouse_tables(meta, minio_key)

