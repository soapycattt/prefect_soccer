from time import perf_counter
from pathlib import Path
import os
import httpx
import asyncio
import shutil

from prefect import flow, task
from prefect_gcp import GcpCredentials


MATCH_URL = "https://api.github.com/repos/footballcsv/cache.footballdata/contents"
TRANSFER_URL = (
    "https://api.github.com/repos/emordonez/transfermarkt-transfers/contents/data"
)

RAW_MATCH_DATA_PATH = Path("matches/raw")
RAW_TRANSFER_DATA_PATH = Path("transfers/raw")


BUCKET = os.getenv("BUCKET")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}


class Crawler:
    def __init__(self, client: httpx.AsyncClient, dimension: str):
        self.client = client
        self.seasons = []
        self.dimension = dimension
        self.URL = self.get_url()
        self.RAW_PATH = self.get_raw_path()
        self.RAW_LOCAL_PATH = os.path.join("data", self.RAW_PATH)

    def get_raw_path(self):
        if self.dimension == "matches":
            return RAW_MATCH_DATA_PATH
        elif self.dimension == "transfers":
            return RAW_TRANSFER_DATA_PATH

    def get_url(self):
        if self.dimension == "matches":
            return MATCH_URL
        elif self.dimension == "transfers":
            return TRANSFER_URL

    def get_season(self, response):
        if self.dimension == "matches":
            return [
                record["name"]
                for record in response
                if (record["type"] == "dir") and ("-" in record["name"])
            ]
        elif self.dimension == "transfers":
            return [record["name"] for record in response if (record["type"] == "dir")]

    def make_dirs(self, seasons: list = None):
        seasons = seasons or self.seasons
        for season in seasons:
            path = os.path.join(self.RAW_LOCAL_PATH, season)
            if not os.path.exists(path):
                os.mkdir(path)

    def clean_dirs(self, seasons: list = None):
        seasons = seasons or self.seasons
        for season in seasons:
            path = os.path.join(self.RAW_LOCAL_PATH, season)
            if os.path.exists(path):
                shutil.rmtree(path)

    def extract_download_links(
        self, season_responses_list: list[dict], seasons: list = None
    ):
        seasons = seasons or self.seasons
        season_leagues = []
        for season, leagues in zip(seasons, season_responses_list):
            for league in leagues.json():
                download_url = league["download_url"]
                season_leagues.append((season, download_url))
        return season_leagues

    async def fetch_league(self, download_tuple: tuple):
        season, download_url = download_tuple
        response = await self.client.get(download_url)

        filename = os.path.basename(download_url)

        path = os.path.join(
            "data", self.RAW_PATH, season, filename
        )  # data/transfers/raw/1993/something.csv
        with open(path, "wb") as f:
            f.write(response.content)

        return os.path.join(self.RAW_PATH, season, filename)

    async def fetch_multiple_leagues(self, download_tuples: list[tuple]):
        reqs = [self.fetch_league(download_tuple) for download_tuple in download_tuples]
        return await asyncio.gather(*reqs)

    async def fetch_seasons(self):
        reqs = [self.client.get(f"{self.URL}/{season}") for season in self.seasons]
        resutls = await asyncio.gather(*reqs, return_exceptions=True)
        return resutls

    async def fetch_repo(self):
        response = await self.client.get(self.URL)
        return response.json()

    async def run(self):
        # Fetch repo diretocries
        response = await self.fetch_repo()

        # Extract seasons and filter out non-eu leagues
        self.seasons = self.get_season(response)

        self.clean_dirs()
        # Fetch seasons
        season_responses_list = await self.fetch_seasons()

        # Create directories
        self.make_dirs()

        # Extract download link + its season
        downloads_tuple = self.extract_download_links(season_responses_list)
        # Fetch leagues csv to local | Input params = list of leagues
        print("Proceeding to download csv file")
        sub_paths = await self.fetch_multiple_leagues(downloads_tuple)
        print(
            f"Completed downloading {len(downloads_tuple)} csv files from {len(self.seasons)} seasons"
        )

        return sub_paths


def get_paths(sub_paths: list, dimension: str):
    from_paths = [os.path.join("data", path) for path in sub_paths]
    to_paths = [os.path.join("eu_football", path) for path in sub_paths]
    return from_paths, to_paths


async def get_bucket():
    gcp_cred_block = await GcpCredentials.load("decamp-gcp-cred")
    gcs_client = gcp_cred_block.get_cloud_storage_client()
    return gcs_client.get_bucket(BUCKET)


@task(log_prints=True)
async def fetch_data(dimension: str):
    start = perf_counter()

    sub_paths = []
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    async with httpx.AsyncClient(headers=headers, limits=limits) as client:
        crawler = Crawler(client, dimension)
        sub_paths = await crawler.run()

    end = perf_counter()
    print(f"Time taken: {end - start:.2f} seconds")
    return sub_paths


async def write_gcs(bucket, from_path: Path, to_path: Path) -> None:
    """Write data from local to GCS"""
    bucket.blob(to_path).upload_from_filename(from_path)


@task(log_prints=True)
async def async_write_gcs(bucket, from_paths: list[Path], to_paths: list[Path]):
    print("Loading to GCS")
    start = perf_counter()

    loads = [
        write_gcs(bucket, from_path, to_path)
        for from_path, to_path in zip(from_paths, to_paths)
    ]
    await asyncio.gather(*loads)

    end = perf_counter()
    print("Completed loading to GCS")
    print(f"Time taken: {end - start:.2f} seconds")


# def build_extraction(name):
@flow(name="Extract")
async def extract_to_gcs(dimension: str):
    sub_paths = await fetch_data(dimension)  # task transfer/raw/1993/something.csv

    # TODO: Test compability of transfers data to the rest of the tasks
    bucket = await get_bucket()
    from_paths, to_paths = get_paths(sub_paths, dimension)
    # return from_paths, to_paths
    await async_write_gcs(bucket, from_paths, to_paths)
    # gcs_loads = [write_gcs(bucket, from_path, to_path) for from_path, to_path in zip(from_paths, to_paths)]
    # await asyncio.gather(*gcs_loads) #multiple tasks


# return extract_to_gcs


if __name__ == "__main__":
    # response = asyncio.run(extract_to_gcs("transfers"))
    pass
