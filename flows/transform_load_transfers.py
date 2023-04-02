from prefect import task, flow
from pathlib import Path
import os
import duckdb

from prefect_gcp import GcpCredentials
from google.cloud.bigquery import LoadJobConfig, SourceFormat, WriteDisposition

GCP_CRED_BLOCK = GcpCredentials.load("decamp-gcp-cred")
RAW_DATA_PATH = Path("transfers/raw")
CLEANED_DATA_PATH = Path("transfers/cleaned")

BUCKET = os.environ.get("BUCKET")
GOOGLE_CLOUD_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")


def get_bucket():
    gcs_client = GCP_CRED_BLOCK.get_cloud_storage_client()
    return gcs_client.get_bucket(BUCKET)


def get_bq_client():
    bq_client = GCP_CRED_BLOCK.get_bigquery_client()
    return bq_client


@task(name="Retrieve `transfers` data")
def retrieve_raw() -> duckdb.DuckDBPyRelation:
    """
    Retrieve data using DuckDB
    """
    return duckdb.sql(f'SELECT * FROM "./data/{RAW_DATA_PATH}/*/*.csv"')


@task(name="Write `transfers` data to local")
def write_local(validated_data: duckdb.DuckDBPyRelation) -> Path:
    """
    Write to local
    """
    validated_data.write_parquet(
        f"./data/{CLEANED_DATA_PATH}/cleaned_transfers.parquet"
    )
    return os.path.join(CLEANED_DATA_PATH, "cleaned_transfers.parquet")


@task(retries=3, log_prints=True, name="Write `transfers` data to gcs")
def write_gcs(sub_path: Path) -> None:
    """Write data from local to GCS"""

    bucket = get_bucket()

    from_path = os.path.join("./data", sub_path)
    to_path = os.path.join("eu_football", sub_path)
    bucket.blob(to_path).upload_from_filename(from_path)


@task(log_prints=True, name="Load `transfers` data to BQ")
def load_to_bq(table: str):
    bq_client = get_bq_client()

    try:
        table_id = f"{GOOGLE_CLOUD_PROJECT}.src_eu_football.{table}"
        gcs_uri = f"gs://{BUCKET}/eu_football/{CLEANED_DATA_PATH}/*"

        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            source_format=SourceFormat.PARQUET,
        )

    except Exception as e:
        print(e)

    finally:
        print("Load data from storage to table")
        load_job = bq_client.load_table_from_uri(
            source_uris=gcs_uri, destination=table_id, job_config=job_config
        )
        load_job.result()
        dml_result = bq_client.get_table(table_id)
        print(f"Loaded {dml_result.num_rows} records to {table_id}")


@flow
def transform_load_transfers():
    raw_data = retrieve_raw()
    sub_path = write_local(raw_data)
    write_gcs(sub_path)
    load_to_bq("transfers")


if __name__ == "__main__":
    transform_load_transfers()
