from prefect import task, flow
from prefect_gcp import GcpCredentials

from time import perf_counter
from datetime import timedelta
from pathlib import Path
import pandas as pd
import os

from google.cloud.bigquery import LoadJobConfig, SourceFormat, WriteDisposition


RAW_DATA_PATH = Path("matches/raw")
RAW_FULL_DATA_PATH = os.path.join("./data/", RAW_DATA_PATH)
CLEANED_DATA_PATH = Path("matches/cleaned")
CLEANED_FULL_DATA_PATH = os.path.join("./data/", CLEANED_DATA_PATH)

GCP_CRED_BLOCK = GcpCredentials.load("decamp-gcp-cred")
GOOGLE_CLOUD_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")
BUCKET = os.environ.get("BUCKET")


def is_cached():
    return True


def fetch_from_gcs():
    pass


def get_season_dirs():
    return list(filter(lambda x: "-" in x, os.listdir()))


def make_dirs(season_dirs: list):
    for season_dir in season_dirs:
        path = os.path.join(CLEANED_FULL_DATA_PATH, season_dir)
        if not os.path.exists(path):
            os.mkdir(path)


def read_csv_from_local(season_dirs: list) -> list[tuple]:
    raw_data = []
    paths = []
    for season_dir in season_dirs:
        season_path = os.path.join(RAW_FULL_DATA_PATH, season_dir)
        for league_csv in os.listdir(season_path):
            path = os.path.join(season_path, league_csv)
            league_name = league_csv.split(".")[:2]

            raw_data.append((league_name, pd.read_csv(path)))
            paths.append(
                os.path.join(season_dir, league_csv)
            )  # e.g. "1993-1994/be.1.csv"

    return paths, raw_data


def get_bucket():
    gcs_client = GCP_CRED_BLOCK.get_cloud_storage_client()
    return gcs_client.get_bucket(BUCKET)


def get_bq_client():
    bq_client = GCP_CRED_BLOCK.get_bigquery_client()
    return bq_client


@task(retries=3, log_prints=True)
def retrieve_raw():
    if is_cached():
        season_dirs = list(filter(lambda x: "-" in x, os.listdir(RAW_FULL_DATA_PATH)))
        paths, raw_data = read_csv_from_local(season_dirs)
        return season_dirs, paths, raw_data

    else:
        return fetch_from_gcs()


@task(log_prints=True)
def transform(raw_data: list[tuple]) -> list[pd.DataFrame]:
    league_dfs: list[pd.DataFrame] = []
    columns = ["match_date", "team_1", "fulltime", "halftime", "team_2"]
    for league_name, league_df in raw_data:
        country_code, tier = league_name

        league_df = league_df.replace("?", None)  # Replace "?" value to None
        league_df.columns = columns
        league_df["match_date"] = pd.to_datetime(league_df["match_date"])
        league_df["country_code"] = country_code
        league_df["tier"] = tier

        league_dfs.append(league_df)

    return league_dfs


@task(retries=3, log_prints=True)
def write_local(paths: list, cleaned_data: list[pd.DataFrame]):
    for path, df in zip(paths, cleaned_data):
        local_path = os.path.join(CLEANED_FULL_DATA_PATH, path)
        df.to_csv(local_path, index=False)

        # sub_paths.append( os.path.join("cleaned", path) )
    # return sub_paths


@task(retries=3, log_prints=True)
def write_gcs(sub_paths: Path) -> None:
    """Write data from local to GCS"""
    start = perf_counter()

    bucket = get_bucket()

    for sub_path in sub_paths:
        from_path = os.path.join(CLEANED_FULL_DATA_PATH, sub_path)
        to_path = os.path.join("eu_football", CLEANED_DATA_PATH, sub_path)
        print(from_path, to_path)
        bucket.blob(to_path).upload_from_filename(from_path)

    end = perf_counter()
    print(f"Time taken: {end - start:.2f} seconds")


@task(log_prints=True, name="Load data from GCS to BQ")
def load_to_bq(table: str):
    start = perf_counter()

    bq_client = get_bq_client()

    try:
        table_id = f"{GOOGLE_CLOUD_PROJECT}.src_eu_football.{table}"
        gcs_uri = f"gs://{BUCKET}/eu_football/{CLEANED_DATA_PATH}/*.csv"

        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            source_format=SourceFormat.CSV,
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

    end = perf_counter()
    print(f"Time taken: {end - start:.2f} seconds")


@flow(name="Transform and Load matches to BQ")
def transform_load_matches():
    season_dirs, paths, raw_data = retrieve_raw()  # task
    # return season_dirs, paths, raw_data
    cleaned_data = transform(raw_data)  # task

    make_dirs(season_dirs)
    write_local(paths, cleaned_data)  # task
    write_gcs(paths)  # task
    # load_to_bq(table="matches") #task


if __name__ == "__main__":
    transform_load_matches()
