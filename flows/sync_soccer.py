from prefect import flow
from extract_to_gcs import extract_to_gcs
from transform_load_matches import transform_load_matches
from transform_load_transfers import transform_load_transfers


@flow(name="Sync Transfers")
def sync_transfers():
    extract_to_gcs("transfers")
    transform_load_transfers()


@flow(name="Sync Matches")
def sync_matches():
    extract_to_gcs("matches")
    transform_load_matches()


if __name__ == "__main__":
    # asyncio.run(sync_soccer())
    pass
