"""Salesforce data ingestion and SOQL query utilities."""

import os
from datetime import datetime
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType

from .metadata import ensure_metadata_table, get_latest_timestamp, update_metadata


def _build_incremental_where_clause(last_modified_date, watermark_column="LastModifiedDate"):
    """
    Helper to build SOQL WHERE clause for incremental filtering.

    Parameters:
    -----------
    last_modified_date : datetime or None
        Timestamp to filter records modified after this date
    watermark_column : str
        Salesforce field to filter on, driven by incremental.source_watermark_column
        in the silver YAML config. Defaults to LastModifiedDate.

    Returns:
    --------
    str
        SOQL WHERE clause or empty string for full pull
    """
    if last_modified_date:
        return f'WHERE {watermark_column} > {last_modified_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")}'
    return ""


def get_salesforce_config_from_silver_mappings(mappings_dir, defaults_path=None, debug=True):
    """
    Automatically generates complete Salesforce ingestion configuration from silver mappings.
    Uses delta-gen's YamlConfigProvider for proper macro expansion and validation.

    Scans all YAML files in the mappings directory and extracts:
    - Salesforce object names (from extensions.salesforce_object)
    - Bronze table names (from sources.catalog, sources.schema, sources.table)
    - Source column lists (from stages.columns.inputs where source='src')

    Parameters:
    -----------
    mappings_dir : str
        Path to directory containing silver Salesforce YAML files
        (e.g., "/lakehouse/default/Files/inputs/silver/salesforce")
    defaults_path : str, optional
        Path to silver_defaults.yaml for macro expansion
        (e.g., "/lakehouse/default/Files/inputs/config/silver_defaults.yaml")
    debug : bool, optional
        If True, print detailed progress and warnings. Defaults to True.

    Returns:
    --------
    tuple(dict, dict)
        - object_to_table_map: Dictionary mapping Salesforce object names to bronze table names
        - column_config: Dictionary mapping Salesforce object names to lists of source columns

    Example:
    --------
    >>> obj_map, col_config = get_salesforce_config_from_silver_mappings(
    ...     "/lakehouse/default/Files/inputs/silver/salesforce",
    ...     "/lakehouse/default/Files/inputs/config/silver_defaults.yaml"
    ... )
    >>> print(obj_map)
    {'Account': 'bronze.salesforce.account', 'Contact': 'bronze.salesforce.contact', ...}
    >>> print(col_config['Account'])
    ['Id', 'Name', 'ParentId', ...]
    """
    from deltagen.model import TableConfig
    from deltagen.providers.yaml_provider import YamlConfigProvider

    object_to_table_map = {}
    column_config = {}
    watermark_config = {}

    # Scan all YAML files in the directory
    mappings_path = Path(mappings_dir)
    if not mappings_path.exists():
        if debug:
            print(f"Warning: Silver mappings directory not found: {mappings_dir}")
        return object_to_table_map, column_config, watermark_config

    # Create delta-gen provider for loading TableConfig with proper validation
    provider = YamlConfigProvider(TableConfig, defaults_path=defaults_path)

    for yaml_file in mappings_path.glob("*.yaml"):
        try:
            # Load using delta-gen - handles macro expansion and validation
            table_config = provider.load(yaml_file)

            # Extract Salesforce object name from extensions
            salesforce_object = table_config.extensions.get("salesforce_object")
            if not salesforce_object:
                if debug:
                    print(f"Warning: No salesforce_object in extensions for {yaml_file.name}, skipping")
                continue

            # Extract bronze table information from first source
            if not table_config.sources:
                if debug:
                    print(f"Warning: No sources found in {yaml_file.name}, skipping")
                continue

            # OVERRIDE: Always use bronze.salesforce for Salesforce bronze ingestion
            # The YAML sources section is configured for silver processing
            # but bronze ingestion always writes to bronze.salesforce regardless of YAML config
            source = table_config.sources[0]
            table = source.table

            if not table:
                if debug:
                    print(f"Warning: No table in source for {yaml_file.name}, skipping")
                continue

            bronze_table = f"bronze.salesforce.{table}"
            object_to_table_map[salesforce_object] = bronze_table


            # Extract source columns from stages using delta-gen's structured model
            source_columns = set()
            for stage in table_config.stages:
                for column in stage.columns:
                    for input_def in column.inputs:
                        # ColumnInput has source and column attributes
                        if input_def.source == "src" and input_def.column:
                            source_columns.add(input_def.column)

            column_config[salesforce_object] = sorted(source_columns)

            # Extract source watermark column for incremental SOQL filtering
            incremental = getattr(table_config, 'incremental', None)
            if incremental:
                swc = incremental.get("source_watermark_column") if isinstance(incremental, dict) else getattr(incremental, 'source_watermark_column', None)
                if swc:
                    watermark_config[salesforce_object] = swc

            if debug:
                wm = watermark_config.get(salesforce_object, "LastModifiedDate (default)")
                print(f"Loaded config for {salesforce_object}: {len(source_columns)} columns -> {bronze_table}, watermark: {wm}")

        except Exception as e:
            print(f"Error loading {yaml_file.name} with delta-gen: {e}")
            continue

    return object_to_table_map, column_config, watermark_config


def get_object_count(sf_client, object_name, last_modified_date, watermark_column="LastModifiedDate"):
    """
    Gets the row count for a Salesforce object using SOQL COUNT() query.
    Supports incremental filtering based on the configured watermark column.

    Parameters:
    -----------
    sf_client : Salesforce
        Authenticated Salesforce client
    object_name : str
        Salesforce object name (e.g., 'Account', 'Contact')
    last_modified_date : datetime or None
        Timestamp to filter records modified after this date (None for full pull)
    watermark_column : str
        Salesforce field to filter on (default: LastModifiedDate)

    Returns:
    --------
    int
        Total count of records matching the query

    Example:
    --------
    >>> from datetime import datetime
    >>> count = get_object_count(sf_client, "Account", datetime(2024, 1, 1))
    >>> print(f"Found {count} modified accounts")
    """
    where_clause = _build_incremental_where_clause(last_modified_date, watermark_column)
    soql_query = f"SELECT COUNT() FROM {object_name} {where_clause}"
    result = sf_client.query(soql_query)
    return result['totalSize']


def check_for_completeness(sf_client, records, object_name, last_modified_date, watermark_column="LastModifiedDate", debug=True):
    """
    Validates that the number of records pulled matches the expected count from Salesforce.
    Raises an exception if counts don't match.

    Parameters:
    -----------
    sf_client : Salesforce
        Authenticated Salesforce client
    records : list
        List of records returned from Salesforce query
    object_name : str
        Salesforce object name (e.g., 'Account', 'Contact')
    last_modified_date : datetime or None
        Timestamp used for incremental filtering
    watermark_column : str
        Salesforce field to filter on (default: LastModifiedDate)
    debug : bool, optional
        If True, print completeness details. Defaults to True.

    Raises:
    -------
    Exception
        If the pulled record count doesn't match the expected source count

    Example:
    --------
    >>> check_for_completeness(sf_client, records, "Account", last_pull_time)
    Pulled 150 from Account, actual count is 150
    """
    expected_count = get_object_count(sf_client, object_name, last_modified_date, watermark_column)
    actual_count = len(records)
    if debug:
        print(f"Pulled {actual_count} from {object_name}, actual count is {expected_count}")

    if expected_count != actual_count:
        raise Exception(
            f"Completeness check failed for {object_name}: "
            f"pulled {actual_count}, expected {expected_count}"
        )


def get_data_from_salesforce_object(
    spark,
    sf_client,
    object_name,
    columns,
    last_modified_date=None,
    watermark_column="LastModifiedDate",
    run_id=None,
    debug=True
):
    """
    Pulls data from a Salesforce object using SOQL query with incremental support.
    Returns a Spark DataFrame with the requested columns and run_id for tracking.

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    sf_client : Salesforce
        Authenticated Salesforce client
    object_name : str
        Salesforce object name (e.g., 'Account', 'Contact')
    columns : list
        List of column names to retrieve from the Salesforce object
    last_modified_date : datetime or None
        Timestamp to filter records modified after this date (None for full pull)
    watermark_column : str
        Salesforce field to filter on, from incremental.source_watermark_column
        in silver YAML config (default: LastModifiedDate)
    run_id : str, optional
        Run identifier to track which pipeline execution ingested this record
    debug : bool, optional
        If True, print completeness details. Defaults to True.

    Returns:
    --------
    DataFrame or None
        Spark DataFrame with retrieved records, or None if no records found

    Example:
    --------
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> columns = ['Id', 'Name', 'Email']
    >>> df = get_data_from_salesforce_object(spark, sf_client, "Contact", columns, run_id="20240101_1430")
    >>> df.show(5)
    """
    import pyspark.sql.functions as F

    where_clause = _build_incremental_where_clause(last_modified_date, watermark_column)
    soql_data_query = f"SELECT {', '.join(columns)} FROM {object_name} {where_clause}"
    results = sf_client.query_all(soql_data_query)
    records = results.get('records', [])

    check_for_completeness(sf_client, records, object_name, last_modified_date, watermark_column, debug=debug)

    if not records:
        return None

    # Create schema with sorted columns for consistency
    schema = StructType([StructField(col, StringType(), True) for col in sorted(columns)])

    # Create DataFrame and drop Salesforce metadata
    df = spark.createDataFrame(records, schema).drop("attributes")

    # Add run_id to track which pipeline execution ingested this record
    if run_id:
        df = df.withColumn("run_id", F.lit(run_id))

    return df


def run_ingestion(
    spark,
    sf_client,
    column_config,
    object_to_table_map,
    metadata_table,
    run_id,
    watermark_config=None,
    pull_start_timestamp=None,
    incremental=True,
    debug=True
):
    """
    Orchestrates the complete Salesforce-to-Lakehouse ETL pipeline with support for incremental and full loads.

    Process:
    1. Ensures schema and metadata table exist
    2. For each configured Salesforce object:
       - Incremental mode (True): Gets last successful pull timestamp and pulls only changed records
       - Full mode (False): Ignores metadata and pulls all records
       - Writes to Bronze Delta table in Lakehouse (append mode)
       - Updates metadata with current pull timestamp, record count, and status
       - Skips objects with no new data
       - Handles individual object failures without stopping the pipeline

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    sf_client : Salesforce
        Authenticated Salesforce client
    column_config : dict
        Dictionary mapping Salesforce object names to lists of column names
    object_to_table_map : dict
        Dictionary mapping Salesforce object names to target table names
    metadata_table : str
        Full metadata table name (e.g., 'bronze.salesforce.ingestion_metadata')
    run_id : str
        Unique identifier for this pipeline execution (UUID)
    pull_start_timestamp : datetime, optional
        Timestamp to use for this pull operation (defaults to current UTC time)
    incremental : bool, optional
        True (default) = incremental load using metadata for delta pulls,
        False = full load ignoring metadata and pulling all records
    debug : bool, optional
        If True, print progress and summary messages. Errors always print.
        Defaults to True for backwards compatibility.

    Returns:
    --------
    dict
        Summary of ingestion results with keys:
        - 'run_id': str
        - 'objects_processed': int
        - 'objects_skipped': int
        - 'objects_failed': int
        - 'total_records_ingested': int

    Example:
    --------
    >>> import uuid
    >>> run_id = str(uuid.uuid4())
    >>> results = run_ingestion(
    ...     spark=spark,
    ...     sf_client=sf_client,
    ...     column_config={"Account": ["Id", "Name", "Type"]},
    ...     object_to_table_map={"Account": "bronze.salesforce.account"},
    ...     metadata_table="bronze.salesforce.ingestion_metadata",
    ...     run_id=run_id,
    ...     incremental=True
    ... )
    >>> print(f"Processed {results['objects_processed']} objects")
    """
    if watermark_config is None:
        watermark_config = {}

    if pull_start_timestamp is None:
        pull_start_timestamp = datetime.utcnow()

    load_mode_str = "incremental" if incremental else "full"

    if debug:
        print(f"Starting Salesforce ingestion pipeline - RUN_ID: {run_id}, MODE: {load_mode_str}")

    # Ensure metadata table exists (schema creation handled by orchestrator)
    ensure_metadata_table(spark, metadata_table)

    results = {
        'run_id': run_id,
        'objects_processed': 0,
        'objects_skipped': 0,
        'objects_failed': 0,
        'total_records_ingested': 0
    }

    for object_name, columns in column_config.items():
        try:
            # Determine last_modified_date based on incremental flag
            if incremental:
                # Incremental load: use metadata to pull only changed records
                last_pulled_timestamp = get_latest_timestamp(spark, object_name, metadata_table)
            else:
                # Full load: ignore metadata, pull all records
                last_pulled_timestamp = None

            watermark_column = watermark_config.get(object_name, "LastModifiedDate")

            dataframe = get_data_from_salesforce_object(
                spark,
                sf_client,
                object_name,
                columns,
                last_pulled_timestamp,
                watermark_column=watermark_column,
                run_id=run_id,
                debug=debug
            )

            if dataframe:
                record_count = dataframe.count()

                # Write to Delta table (inline - no abstraction needed for 1-line operation)
                target_table = object_to_table_map[object_name]
                dataframe.write.format("delta").mode("append").saveAsTable(target_table)

                update_metadata(
                    spark, object_name, pull_start_timestamp, metadata_table,
                    run_id=run_id, record_count=record_count, status="SUCCESS"
                )
                results['objects_processed'] += 1
                results['total_records_ingested'] += record_count

                # Only print detailed progress if debug enabled
                if debug:
                    print(f"Processed {object_name}: {record_count} records -> {target_table}")
            else:
                update_metadata(
                    spark, object_name, pull_start_timestamp, metadata_table,
                    run_id=run_id, record_count=0, status="NO_NEW_DATA"
                )
                results['objects_skipped'] += 1

                if debug:
                    print(f"No new data for {object_name}")

        except Exception as e:
            error_msg = str(e)
            # Always print errors regardless of debug mode
            print(f"Error processing {object_name}: {error_msg}")
            update_metadata(
                spark, object_name, pull_start_timestamp, metadata_table,
                run_id=run_id, record_count=0, status=f"FAILED: {error_msg}"
            )
            results['objects_failed'] += 1
            # Continue processing other objects instead of failing the entire pipeline

    if debug:
        print(f"Completed Salesforce ingestion pipeline - RUN_ID: {run_id}")
    return results
