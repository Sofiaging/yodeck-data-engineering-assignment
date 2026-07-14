import boto3
import pandas as pd
import dlt
from io import StringIO


# -----------------------
# S3 configuration
# -----------------------

BUCKET_NAME = "yodeck-landing-zone"

S3_KEY = (
    "postgres_production_folder/"
    "accounts/"
    "accounts_2026-06-09.csv"
)


# -----------------------
# Read CSV from S3
# -----------------------

s3_client = boto3.client("s3")

response = s3_client.get_object(
    Bucket=BUCKET_NAME,
    Key=S3_KEY
)

csv_content = response["Body"].read().decode("utf-8")

df = pd.read_csv(
    StringIO(csv_content)
)


print("CSV loaded from S3:")
print(df.head())


# -----------------------
# Add ingestion metadata
# -----------------------

df["_source_file"] = S3_KEY
df["_ingested_at"] = pd.Timestamp.utcnow()


# -----------------------
# dlt pipeline
# -----------------------

pipeline = dlt.pipeline(
    pipeline_name="yodeck_ingestion",
    destination="postgres",
    dataset_name="raw"
)


load_info = pipeline.run(
    df.to_dict(orient="records"),
    table_name="accounts",
)


print(load_info)
