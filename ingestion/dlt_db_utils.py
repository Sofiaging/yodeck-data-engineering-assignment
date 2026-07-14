import dlt
import pandas as pd
import psycopg2
import toml
import yaml

# ---------------------------------------------------------
# Load pipeline configuration
# ---------------------------------------------------------

with open("config/pipeline_config.yml", "r") as f:
    pipeline_config = yaml.safe_load(f)

# ---------------------------------------------------------
# DLT Pipeline
# ---------------------------------------------------------

pipeline = dlt.pipeline(

    pipeline_name=pipeline_config["pipeline"]["name"],

    destination=pipeline_config["pipeline"]["destination"],

    dataset_name=pipeline_config["pipeline"]["dataset"]

)


# ---------------------------------------------------------
# Add ingestion metadata
# ---------------------------------------------------------

def prepare_dataframe(
    df: pd.DataFrame,
    source_file: str,
    snapshot_date,
    checksum: str
):
    """
    Adds ingestion metadata columns before loading.
    """

    df = df.copy()

    metadata = pipeline_config["metadata_columns"]

    df[metadata["source_file"]] = source_file

    df[metadata["snapshot_date"]] = snapshot_date

    df[metadata["ingested_at"]] = pd.Timestamp.now(tz="UTC")

    df[metadata["checksum"]] = checksum

    return df


# ---------------------------------------------------------
# Load business table
# ---------------------------------------------------------

def load_table(
    df: pd.DataFrame,
    table_name: str
):
    """
    Loads a dataframe into the raw schema.

    Returns:
        dlt LoadInfo object.
    """

    load_info = pipeline.run(
        df.to_dict(orient="records"),
        table_name=table_name
    )

    return load_info


# ---------------------------------------------------------
# Write ingestion log
# ---------------------------------------------------------

def write_ingestion_log(
    file_name,
    snapshot_date,
    table_name,
    row_count,
    checksum,
    started_at,
    completed_at,
    status,
    error_message,
    schema_change,
    load_id
):
    """
    Writes one row into raw.ingestion_log.
    """

    log_df = pd.DataFrame(
        [
            {
                "file_name": file_name,
                "snapshot_date": snapshot_date,
                "table_name": table_name,
                "row_count": row_count,
                "checksum": checksum,
                "started_at": started_at,
                "completed_at": completed_at,
                "status": status,
                "error_message": error_message,
                "schema_change": schema_change,
                "load_id": load_id
            }
        ]
    )

    pipeline.run(
        log_df.to_dict(orient="records"),
        table_name="ingestion_log"
    )

# ---------------------------------------------------------
# Postgres connection
# ---------------------------------------------------------

def get_postgres_connection():
    """
    Creates a PostgreSQL connection using
    credentials from .dlt/secrets.toml.
    """

    secrets = toml.load(".dlt/secrets.toml")

    credentials = secrets["destination"]["postgres"]["credentials"]

    return psycopg2.connect(

        host=credentials["host"],

        port=credentials["port"],

        database=credentials["database"],

        user=credentials["username"],

        password=credentials["password"]

    )

# -------------------------------------------------------------
# For ingestion_mode='new', check if file was already ingested
# -------------------------------------------------------------

def file_already_ingested(
    file_name: str,
    checksum: str
):
    """
    Returns True if the exact same file
    (same filename + checksum)
    has already been ingested.
    """

    conn = get_postgres_connection()

    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT 1
        FROM raw.ingestion_log
        WHERE file_name = %s
          AND checksum = %s
        LIMIT 1
        """,
        (
            file_name,
            checksum
        )
    )

    exists = cursor.fetchone() is not None

    cursor.close()

    conn.close()

    return exists

