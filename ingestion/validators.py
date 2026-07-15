import re

import pandas as pd


def validate_filename(
    filename: str,
    pattern: str
):
    """
    Validates that a filename follows the expected pattern
    defined in pipeline_config.yml.

    Examples of valid filenames:

        accounts_2026-06-09.csv
        accounts_2026-06-09_resend.csv

    Raises:
        ValueError if filename does not match.
    """

    if not re.match(pattern, filename):

        raise ValueError(

            f"Invalid filename '{filename}'. "
            f"Expected pattern '{pattern}'."
        )


def validate_primary_key(
    df: pd.DataFrame,
    primary_keys: list
):
    """
    Checks that:

    1. All primary key columns exist.
    2. There are no duplicate primary keys
       within the current snapshot.

    Raises:
        ValueError if validation fails.
    """

    # Check that PK columns exist
    for pk in primary_keys:

        if pk not in df.columns:

            raise ValueError(

                f"Primary key column '{pk}' "
                "not found in source file."
            )

    # Check duplicates
    duplicated = df.duplicated(
        subset=primary_keys,
        keep=False
    )

    if duplicated.any():

        duplicate_rows = df.loc[
            duplicated,
            primary_keys
        ]

        raise ValueError(

            "Duplicate primary keys detected:\n\n"

            f"{duplicate_rows}"
        )


def validate_datatypes(
    df: pd.DataFrame,
    expected_schema: dict
):
    """
    Validates dataframe column datatypes according
    to config/tables.yml.

    Supported datatypes:

        string
        float
        date
        timestamp

    Raises:
        ValueError if validation fails.
    """

    for column, datatype in expected_schema.items():

        # Check column exists
        if column not in df.columns:

            df[column] = None

            print(
                f"WARNING: Missing column '{column}'. "
                "Filled with NULL values."
            )

            continue

        try:

            if datatype == "string":

                df[column].astype(str)

            elif datatype == "float":

                pd.to_numeric(
                    df[column],
                    errors="raise"
                )

            elif datatype == "date":

                pd.to_datetime(
                    df[column],
                    errors="raise"
                )

            elif datatype == "timestamp":

                pd.to_datetime(
                    df[column],
                    errors="raise"
                )

            else:

                raise ValueError(

                    f"Unsupported datatype "
                    f"'{datatype}' "
                    f"for column '{column}'."
                )

        except Exception as e:

            raise ValueError(

                f"Column '{column}' cannot be "
                f"validated as datatype "
                f"'{datatype}'.\n\n"

                f"Original error:\n{e}"
            )


def detect_schema_change(
    df: pd.DataFrame,
    expected_schema: dict
):
    """
    Detects missing or new columns.

    Returns:
        (
            schema_changed,
            message
        )
    """

    expected = set(expected_schema.keys())

    actual = set(df.columns)

    missing = sorted(expected - actual)

    extra = sorted(actual - expected)

    if not missing and not extra:

        return False, None

    message = []

    if missing:

        message.append(
            f"Missing columns: {missing}"
        )

    if extra:

        message.append(
            f"New columns: {extra}"
        )

    return True, "; ".join(message)