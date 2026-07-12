"""Salesforce ingestion metadata tracking utilities."""

import pyspark.sql.functions as F


def ensure_metadata_table(spark, table_name):
    """
    Creates a metadata tracking table for Salesforce data ingestion if it doesn't exist.

    Schema includes:
    - run_id: Unique identifier for each pipeline run
    - source_table: Salesforce object name
    - last_pulled_timestamp: When the pull operation started
    - record_count: Number of records ingested
    - execution_status: SUCCESS, FAILED, or NO_NEW_DATA

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    table_name : str
        Full table name (e.g., 'bronze.salesforce.ingestion_metadata')

    Example:
    --------
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> ensure_metadata_table(spark, "bronze.salesforce.ingestion_metadata")
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            run_id STRING,
            source_table STRING,
            last_pulled_timestamp TIMESTAMP,
            record_count LONG,
            execution_status STRING
        )
        USING DELTA
    """)


def update_metadata(spark, source_table, pull_start_timestamp, metadata_table, run_id=None, record_count=0, status="SUCCESS"):
    """
    Updates the metadata table with ingestion details for the given source table.

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    source_table : str
        Salesforce object name (e.g., 'Account', 'Contact')
    pull_start_timestamp : datetime
        Timestamp when the pull operation started
    metadata_table : str
        Full metadata table name (e.g., 'bronze.salesforce.ingestion_metadata')
    run_id : str, optional
        Unique identifier for this pipeline run
    record_count : int, optional
        Number of records ingested (default: 0)
    status : str, optional
        Execution status: SUCCESS, FAILED, or NO_NEW_DATA (default: SUCCESS)

    Example:
    --------
    >>> from datetime import datetime
    >>> update_metadata(spark, "Account", datetime.utcnow(),
    ...                 "bronze.salesforce.ingestion_metadata",
    ...                 run_id="20240101_1430", record_count=150, status="SUCCESS")
    """
    latest_timestamp_df = spark.createDataFrame(
        [(run_id, source_table, pull_start_timestamp, record_count, status)],
        ["run_id", "source_table", "last_pulled_timestamp", "record_count", "execution_status"]
    )

    (
        latest_timestamp_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(metadata_table)
    )


def get_latest_timestamp(spark, source_table, metadata_table):
    """
    Retrieves the latest last_pulled_timestamp for the given source_table from the metadata table,
    filtering for successful executions only (SUCCESS or NO_NEW_DATA status).

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    source_table : str
        Salesforce object name (e.g., 'Account', 'Contact')
    metadata_table : str
        Full metadata table name (e.g., 'bronze.salesforce.ingestion_metadata')

    Returns:
    --------
    datetime or None
        The latest successful pull timestamp, or None if no previous successful pull exists

    Example:
    --------
    >>> last_pull = get_latest_timestamp(spark, "Account", "bronze.salesforce.ingestion_metadata")
    >>> print(last_pull)
    """
    df = spark.table(metadata_table)

    last_pulled_timestamp = (
        df
        .filter(df.source_table == source_table)
        .filter((df.execution_status == "SUCCESS") | (df.execution_status == "NO_NEW_DATA"))
        .agg(F.max("last_pulled_timestamp").alias("last_pulled_timestamp"))
        .collect()
    )

    if last_pulled_timestamp:
        return last_pulled_timestamp[0]["last_pulled_timestamp"]
    return None
