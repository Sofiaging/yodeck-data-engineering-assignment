import argparse
from datetime import datetime

import yaml

from s3_utils import (
    list_files,
    select_files,
    resolve_duplicate_snapshots,
    read_csv_from_s3,
    calculate_checksum,
    parse_s3_key,
    extract_snapshot_date
)

from validators import (
    validate_filename,
    validate_primary_key,
    validate_datatypes
)

from dlt_db_utils import (
    prepare_dataframe,
    load_table,
    write_ingestion_log,
    file_already_ingested
)

from datetime import UTC

# ---------------------------------------------------------
# Load configuration
# ---------------------------------------------------------

with open("config/pipeline_config.yml", "r") as f:
    pipeline_config = yaml.safe_load(f)

with open("config/tables.yml", "r") as f:
    table_config = yaml.safe_load(f)


# ---------------------------------------------------------
# Parse CLI arguments
# ---------------------------------------------------------

parser = argparse.ArgumentParser(
    description="Load CSV snapshots from S3 into PostgreSQL."
)

parser.add_argument(
    "--mode",
    choices=["all", "range", "new"],
    required=True,
    help="Ingestion mode."
)

parser.add_argument(
    "--start-date",
    required=False,
    help="Start snapshot date (YYYY-MM-DD)."
)

parser.add_argument(
    "--end-date",
    required=False,
    help="End snapshot date (YYYY-MM-DD)."
)

args = parser.parse_args()

mode = args.mode

start_date = (
    datetime.strptime(args.start_date, "%Y-%m-%d").date()
    if args.start_date
    else None
)

end_date = (
    datetime.strptime(args.end_date, "%Y-%m-%d").date()
    if args.end_date
    else None
)


# ---------------------------------------------------------
# S3 configuration
# ---------------------------------------------------------

bucket = pipeline_config["s3"]["bucket"]

prefix = pipeline_config["s3"]["prefix"]


# ---------------------------------------------------------
# Discover files
# ---------------------------------------------------------

print("\nDiscovering files in S3...\n")

files = list_files(
    bucket=bucket,
    prefix=prefix
)

folder_placeholders = sum(
    1 for file in files
    if file["Key"].endswith("/")
)

csv_files = len(files) - folder_placeholders

print(f"Found {csv_files} CSV snapshot files.")


# ---------------------------------------------------------
# Build ingestion plan
# ---------------------------------------------------------

enriched_files = []

for file in files:

    key = file["Key"]

    # Ignore folder placeholders
    if key.endswith("/"):
        continue

    filename, table_name = parse_s3_key(key)

    pattern = pipeline_config["tables"][table_name]["snapshot_pattern"]

    snapshot_date = extract_snapshot_date(
        filename,
        pattern
    )

    enriched_files.append({

        "Key": key,

        "filename": filename,

        "table_name": table_name,

        "snapshot_date": snapshot_date,

        "LastModified": file["LastModified"]

    })


# ---------------------------------------------------------
# Resolve duplicate snapshots
# ---------------------------------------------------------

enriched_files = resolve_duplicate_snapshots(
    enriched_files
)


# ---------------------------------------------------------
# Filter files according to ingestion mode
# ---------------------------------------------------------

enriched_files = select_files(

    enriched_files,

    mode,

    start_date,

    end_date

)

print(

    f"{len(enriched_files)} files selected "

    f"for ingestion.\n"

)


# ---------------------------------------------------------
# Counters
# ---------------------------------------------------------

successful = 0

failed = 0


# ---------------------------------------------------------
# Process every snapshot
# ---------------------------------------------------------

for file in enriched_files:

    started_at = datetime.now(UTC)

    try:

        filename = file["filename"]

        key = file["Key"]

        table_name = file["table_name"]

        snapshot_date = file["snapshot_date"]

        print(

            f"Processing "

            f"{filename}"

        )

        # -------------------------------------------------
        # Validate filename
        # -------------------------------------------------

        pattern = pipeline_config["tables"][table_name][
            "snapshot_pattern"
        ]

        validate_filename(
            filename,
            pattern
        )

        # -------------------------------------------------
        # Calculate checksum
        # -------------------------------------------------

        checksum = calculate_checksum(
            bucket,
            key
        )

        # -------------------------------------------------
        # Incremental ingestion
        # -------------------------------------------------

        if mode == "new":

            if file_already_ingested(
                filename,
                checksum
            ):

                print(

                    f"Skipping already ingested file: "

                    f"{filename}"

                )

                continue

            print(

                f"New file detected: "

                f"{filename}"

            )

        # -------------------------------------------------
        # Read CSV
        # -------------------------------------------------

        df = read_csv_from_s3(
            bucket,
            key
        )

        # -------------------------------------------------
        # Validate datatypes
        # -------------------------------------------------

        expected_schema = table_config["tables"][table_name][
            "columns"
        ]

        validate_datatypes(
            df,
            expected_schema
        )

        # -------------------------------------------------
        # Validate primary key
        # -------------------------------------------------

        primary_keys = table_config["tables"][table_name][
            "primary_key"
        ]

        validate_primary_key(
            df,
            primary_keys
        )

        # -------------------------------------------------
        # Add ingestion metadata
        # -------------------------------------------------

        df = prepare_dataframe(
            df,
            source_file=filename,
            snapshot_date=snapshot_date,
            checksum=checksum
        )

        # -------------------------------------------------
        # Load into PostgreSQL
        # -------------------------------------------------

        load_info = load_table(
            df,
            table_name
        )

        completed_at = datetime.now(UTC)

        # -------------------------------------------------
        # Extract dlt load id
        # -------------------------------------------------

        load_id = str(load_info)

        # -------------------------------------------------
        # Update ingestion log
        # -------------------------------------------------

        write_ingestion_log(

            file_name=filename,

            snapshot_date=snapshot_date,

            table_name=table_name,

            row_count=len(df),

            checksum=checksum,

            started_at=started_at,

            completed_at=completed_at,

            status="SUCCESS",

            error_message=None,

            schema_change=False,

            load_id=load_id

        )

        successful += 1

        print(

            f"✓ Successfully loaded "

            f"{filename}"

        )


    except Exception as e:

        completed_at = datetime.now(UTC)

        failed += 1

        print(

            f"✗ Failed to load "

            f"{filename}"

        )

        print(e)

        try:

            write_ingestion_log(

                file_name=filename,

                snapshot_date=snapshot_date,

                table_name=table_name,

                row_count=0,

                checksum=checksum if "checksum" in locals() else None,

                started_at=started_at,

                completed_at=completed_at,

                status="FAILED",

                error_message=str(e),

                schema_change=False,

                load_id=None

            )

        except Exception:

            pass


# ---------------------------------------------------------
# Summary
# ---------------------------------------------------------

print("\n----------------------------------------")

print("Ingestion completed.")

print("----------------------------------------")

print(f"Successful files : {successful}")

print(f"Failed files     : {failed}")

print("----------------------------------------")

