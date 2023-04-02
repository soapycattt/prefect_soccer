from prefect.blocks.system import Secret, String
from prefect_gcp import GcpCredentials, GcsBucket
import os

gcp_cred_path = os.getenv("GCP_CRED_PATH")
bucket = os.getenv("BUCKET")

GcpCredentials(service_account_file=gcp_cred_path).save(
    "decamp-gcp-cred", overwrite=True
)

gcp_credential = GcpCredentials.load("decamp-gcp-cred")

GcsBucket(
    bucket=bucket,
    gcp_credentials=gcp_credential,
).save(name="decamp-bucket", overwrite=True)
