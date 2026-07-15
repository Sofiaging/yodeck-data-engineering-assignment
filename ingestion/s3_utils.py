import hashlib
import re
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd
import os


# Create S3 client using credentials from AWS CLI
s3_client = boto3.client("s3")


def list_files(bucket: str, prefix: str) -> list:
    """
    List all objects under an S3 prefix.
    """

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )

    return response.get("Contents", [])


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    Reads a CSV file from S3 into a pandas DataFrame.
    """

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    return pd.read_csv(BytesIO(response["Body"].read()))


def calculate_checksum(bucket: str, key: str) -> str:
    """
    Calculate SHA256 checksum of an S3 object.
    Used for duplicate detection.
    """

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    file_bytes = response["Body"].read()

    return hashlib.sha256(file_bytes).hexdigest()


def parse_s3_key(s3_key: str):
    """
    Parses an S3 object key.

    Example:

        postgres_production_folder/accounts/accounts_2026-06-09.csv

    Returns:

        (
            "accounts_2026-06-09.csv",
            "accounts"
        )
    """

    filename = os.path.basename(s3_key)

    table_name = os.path.basename(
        os.path.dirname(s3_key)
    )

    return filename, table_name


def extract_snapshot_date(filename: str, pattern: str):
    """
    Extract snapshot date from filename using regex.

    Example:
        accounts_2026-06-09.csv

    returns

        datetime.date(2026, 6, 9)
    """

    match = re.match(pattern, filename)

    if not match:
        return None

    return datetime.strptime(
        match.group(1),
        "%Y-%m-%d"
    ).date()


def select_files(
    files: list,
    mode: str,
    start_date=None,
    end_date=None
):
    """
    Selects candidate files for ingestion.

    all   -> return every discovered file

    range -> return files whose snapshot_date
             falls within the supplied date range

    new   -> return every discovered file.
             Duplicate detection is performed
             later in load_s3_csvs.py after the
             file checksum has been calculated.
    """

    if mode == "all":
        return files

    if mode == "range":

        selected = []

        for file in files:

            snapshot_date = file.get("snapshot_date")

            if start_date <= snapshot_date <= end_date:
                selected.append(file)

        return selected

    if mode == "new":
        return files

    raise ValueError(
        f"Unknown ingestion mode: {mode}"
    )


def resolve_duplicate_snapshots(files: list) -> list:
    """
    If multiple files exist for the same
    table + snapshot_date, keep the latest upload.

    Example:

        accounts_2026-06-09.csv
        accounts_2026-06-09_resend.csv

    -> keep the latest LastModified object.
    """

    latest = {}
    duplicates = []

    for file in files:

        key = (
            file["table_name"],
            file["snapshot_date"]
        )

        if key not in latest:
            latest[key] = file
            continue

        if file["LastModified"] > latest[key]["LastModified"]:

            duplicates.append(latest[key]["filename"])

            latest[key] = file

        else:

            duplicates.append(file["filename"])

    if duplicates:

        print("\nDuplicate snapshots detected:")

        for filename in duplicates:

            print(f"  - {filename}")

        print()    

    return list(latest.values())


def validate_snapshot_completeness(files: list):
    """
    Returns the snapshot dates that are incomplete.
    """

    expected_tables = {
        "accounts",
        "subscriptions",
        "invoices",
        "exchange_rates"
    }

    snapshots = {}

    for file in files:
        snapshots.setdefault(
            file["snapshot_date"],
            set()
        ).add(file["table_name"])

    incomplete_dates = set()

    for snapshot_date, tables in snapshots.items():

        missing = expected_tables - tables

        if missing:

            print(
                f"\nWARNING: Incomplete snapshot {snapshot_date}"
            )

            print(
                f"Missing tables: {', '.join(sorted(missing))}"
            )

            incomplete_dates.add(snapshot_date)

    return incomplete_dates