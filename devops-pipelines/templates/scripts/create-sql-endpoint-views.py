#!/usr/bin/env python3
"""
Create views in Fabric SQL Analytics Endpoint for observability lakehouse.

This script connects to the SQL Analytics Endpoint and creates T-SQL views
that are visible in the SQL endpoint (unlike Spark SQL views which are not).

Usage:
    python create-sql-endpoint-views.py
"""

import struct
import subprocess
import sys
from typing import Optional


def get_access_token() -> str:
    """Get Azure AD access token for SQL Database."""
    result = subprocess.run(
        [
            "az",
            "account",
            "get-access-token",
            "--resource",
            "https://database.windows.net/",
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def connect_to_sql_endpoint(server: str, database: str) -> "pyodbc.Connection":
    """
    Connect to Fabric SQL Analytics Endpoint using Azure AD token.

    Args:
        server: SQL endpoint server name (e.g., "xxx.datawarehouse.fabric.microsoft.com")
        database: Database/lakehouse ID

    Returns:
        pyodbc connection object
    """
    try:
        import pyodbc
    except ImportError:
        print("Error: pyodbc not installed. Install with: pip install pyodbc")
        sys.exit(1)

    token = get_access_token()

    # Encode token for ODBC
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={server};"
        f"Database={database};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )

    print(f"Connecting to SQL endpoint: {server}")
    print(f"Database: {database}")

    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    return conn


def execute_sql(conn: "pyodbc.Connection", sql: str, description: str) -> None:
    """Execute SQL statement with error handling."""
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print(f"✓ {description}")
    except Exception as e:
        print(f"✗ {description}")
        print(f"  Error: {e}")
        raise


def create_views(
    server: str,
    database: str,
    schema: str = "log",
    shortcut_schema: str = "silver_log",
) -> None:
    """
    Create observability views in SQL Analytics Endpoint.

    Args:
        server: SQL endpoint server name
        database: Database/lakehouse ID
        schema: Schema name for views (default: "log")
        shortcut_schema: Shortcut schema name for silver data (default: "silver_log")
    """
    conn = connect_to_sql_endpoint(server, database)

    try:
        # Create schema
        execute_sql(
            conn,
            f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') EXEC('CREATE SCHEMA [{schema}]')",
            f"Create schema [{schema}]",
        )

        # View 1: f_pipeline_health
        # Latest run status per table - one row per table
        f_pipeline_health = f"""
CREATE OR ALTER VIEW [{schema}].[f_pipeline_health] AS
WITH all_runs AS (
    SELECT
        table_name,
        run_id,
        load_id,
        status,
        start_time,
        end_time,
        duration_ms,
        total_rows_read,
        total_rows_written,
        total_rows_rejected,
        error_message,
        environment,
        CASE
            WHEN table_name LIKE 'bronze_%' THEN 'bronze'
            WHEN table_name LIKE 'silver_%' THEN 'silver'
            WHEN table_name LIKE 'gold_%'   THEN 'gold'
            ELSE 'unknown'
        END AS layer
    FROM [{shortcut_schema}].[deltagen_run_metrics]
),
ranked AS (
    SELECT
        *,
        CASE
            WHEN total_rows_read > 0
            THEN CAST(total_rows_rejected AS FLOAT) / total_rows_read
            ELSE 0.0
        END AS rejection_rate,
        ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY start_time DESC) AS rn
    FROM all_runs
)
SELECT
    table_name,
    layer,
    run_id,
    load_id,
    status,
    start_time,
    end_time,
    duration_ms,
    total_rows_read,
    total_rows_written,
    total_rows_rejected,
    rejection_rate,
    error_message,
    environment
FROM ranked
WHERE rn = 1
"""
        execute_sql(conn, f_pipeline_health, f"Create view [{schema}].[f_pipeline_health]")

        # View 2: f_dq_summary
        # DQ issues aggregated by table and day
        f_dq_summary = f"""
CREATE OR ALTER VIEW [{schema}].[f_dq_summary] AS
WITH all_quality AS (
    SELECT run_id, issue_type, action, column_name, rule_name, columns, row_count
    FROM [{shortcut_schema}].[deltagen_quality_metrics]
),
all_runs AS (
    SELECT
        run_id,
        table_name,
        start_time,
        total_rows_read,
        CASE
            WHEN table_name LIKE 'bronze_%' THEN 'bronze'
            WHEN table_name LIKE 'silver_%' THEN 'silver'
            WHEN table_name LIKE 'gold_%'   THEN 'gold'
            ELSE 'unknown'
        END AS layer
    FROM [{shortcut_schema}].[deltagen_run_metrics]
)
SELECT
    r.table_name,
    r.layer,
    CAST(r.start_time AS DATE) AS run_date,
    q.issue_type,
    q.action,
    q.column_name,
    SUM(q.row_count) AS total_rows_affected,
    COUNT(*) AS issue_occurrences,
    MAX(r.total_rows_read) AS rows_read
FROM all_quality q
JOIN all_runs r ON q.run_id = r.run_id
GROUP BY
    r.table_name,
    r.layer,
    CAST(r.start_time AS DATE),
    q.issue_type,
    q.action,
    q.column_name
"""
        execute_sql(conn, f_dq_summary, f"Create view [{schema}].[f_dq_summary]")

        # View 3: f_rejection_rate_trend
        # Daily rollup per table: row counts, rejection rate, failures, duration
        f_rejection_rate_trend = f"""
CREATE OR ALTER VIEW [{schema}].[f_rejection_rate_trend] AS
WITH all_runs AS (
    SELECT
        table_name,
        status,
        start_time,
        duration_ms,
        total_rows_read,
        total_rows_written,
        total_rows_rejected,
        CASE
            WHEN table_name LIKE 'bronze_%' THEN 'bronze'
            WHEN table_name LIKE 'silver_%' THEN 'silver'
            WHEN table_name LIKE 'gold_%'   THEN 'gold'
            ELSE 'unknown'
        END AS layer
    FROM [{shortcut_schema}].[deltagen_run_metrics]
)
SELECT
    table_name,
    layer,
    CAST(start_time AS DATE) AS run_date,
    COUNT(*) AS run_count,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
    SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped_runs,
    SUM(total_rows_read) AS total_rows_read,
    SUM(total_rows_written) AS total_rows_written,
    SUM(total_rows_rejected) AS total_rows_rejected,
    CASE
        WHEN SUM(total_rows_read) > 0
        THEN CAST(SUM(total_rows_rejected) AS FLOAT) / SUM(total_rows_read)
        ELSE 0.0
    END AS rejection_rate,
    AVG(duration_ms) AS avg_duration_ms,
    MAX(duration_ms) AS max_duration_ms
FROM all_runs
GROUP BY
    table_name,
    layer,
    CAST(start_time AS DATE)
"""
        execute_sql(
            conn,
            f_rejection_rate_trend,
            f"Create view [{schema}].[f_rejection_rate_trend]",
        )

        # View 4: f_dq_rejected (if tables exist)
        # Check if DQ rejected tables exist
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = 'silver_dq_rejected'"
        )
        has_silver_rejected = cursor.fetchone()[0] > 0

        cursor.execute(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = 'gold_dq_rejected'"
        )
        has_gold_rejected = cursor.fetchone()[0] > 0

        if has_silver_rejected or has_gold_rejected:
            parts = []
            if has_silver_rejected:
                parts.append(
                    f"""
    SELECT
        _dq_run_id AS run_id,
        _dq_table_name AS table_name,
        _dq_load_id AS load_id,
        _dq_column_name AS column_name,
        _dq_rejection_reason AS rejection_reason,
        _dq_rule_name AS rule_name,
        _dq_rejected_at AS rejected_at,
        _dq_natural_id AS natural_id,
        _dq_record AS record_json,
        'silver' AS layer
    FROM [{schema}].[silver_dq_rejected]"""
                )
            if has_gold_rejected:
                parts.append(
                    f"""
    SELECT
        _dq_run_id AS run_id,
        _dq_table_name AS table_name,
        _dq_load_id AS load_id,
        _dq_column_name AS column_name,
        _dq_rejection_reason AS rejection_reason,
        _dq_rule_name AS rule_name,
        _dq_rejected_at AS rejected_at,
        _dq_natural_id AS natural_id,
        _dq_record AS record_json,
        'gold' AS layer
    FROM [{schema}].[gold_dq_rejected]"""
                )

            f_dq_rejected = (
                f"CREATE OR ALTER VIEW [{schema}].[f_dq_rejected] AS"
                + "\n    UNION ALL".join(parts)
            )
            execute_sql(conn, f_dq_rejected, f"Create view [{schema}].[f_dq_rejected]")
        else:
            print(f"⊘ Skipped [{schema}].[f_dq_rejected] - no DQ rejected tables found")

        # View 5: f_dq_duplicates (if tables exist)
        cursor.execute(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = 'silver_dq_duplicates'"
        )
        has_silver_dups = cursor.fetchone()[0] > 0

        cursor.execute(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = 'gold_dq_duplicates'"
        )
        has_gold_dups = cursor.fetchone()[0] > 0

        if has_silver_dups or has_gold_dups:
            parts = []
            if has_silver_dups:
                parts.append(
                    f"""
    SELECT
        _dq_run_id AS run_id,
        _dq_table_name AS table_name,
        _dq_load_id AS load_id,
        _dq_natural_keys AS natural_keys,
        _dq_detected_at AS detected_at,
        'silver' AS layer
    FROM [{schema}].[silver_dq_duplicates]"""
                )
            if has_gold_dups:
                parts.append(
                    f"""
    SELECT
        _dq_run_id AS run_id,
        _dq_table_name AS table_name,
        _dq_load_id AS load_id,
        _dq_natural_keys AS natural_keys,
        _dq_detected_at AS detected_at,
        'gold' AS layer
    FROM [{schema}].[gold_dq_duplicates]"""
                )

            f_dq_duplicates = (
                f"CREATE OR ALTER VIEW [{schema}].[f_dq_duplicates] AS"
                + "\n    UNION ALL".join(parts)
            )
            execute_sql(
                conn, f_dq_duplicates, f"Create view [{schema}].[f_dq_duplicates]"
            )
        else:
            print(f"⊘ Skipped [{schema}].[f_dq_duplicates] - no DQ duplicate tables found")

        # Check if gold_log schema exists
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'gold_log'")
        has_gold_log = cursor.fetchone()[0] > 0

        # View: dim_load
        # Load (orchestration batch) dimension — one row per load_id with reference attributes
        dim_load = f"""
CREATE OR ALTER VIEW [{schema}].[dim_load] AS
SELECT
    load_id,
    COUNT(DISTINCT run_id) AS run_count,
    COUNT(DISTINCT table_name) AS table_count,
    CAST(MIN(start_time) AS DATE) AS load_date
FROM (
    SELECT run_id, table_name, load_id, start_time FROM [{shortcut_schema}].[deltagen_run_metrics]
"""
        if has_gold_log:
            dim_load += f"""    UNION ALL
    SELECT run_id, table_name, load_id, start_time FROM [gold_log].[deltagen_run_metrics]
"""
        dim_load += """) AS all_runs
WHERE load_id IS NOT NULL
GROUP BY load_id
"""
        execute_sql(conn, dim_load, f"Create view [{schema}].[dim_load]")

        # View: dim_run
        # Run dimension — one row per run_id with reference attributes
        dim_run = f"""
CREATE OR ALTER VIEW [{schema}].[dim_run] AS
SELECT
    run_id,
    load_id,
    table_name,
    CASE
        WHEN table_name LIKE 'bronze_%' THEN 'bronze'
        WHEN table_name LIKE 'silver_%' THEN 'silver'
        WHEN table_name LIKE 'gold_%'   THEN 'gold'
        ELSE 'unknown'
    END AS layer,
    status,
    environment,
    CAST(start_time AS DATE) AS run_date
FROM [{shortcut_schema}].[deltagen_run_metrics]
"""
        if has_gold_log:
            dim_run += f"""
UNION ALL
SELECT
    run_id,
    load_id,
    table_name,
    CASE
        WHEN table_name LIKE 'bronze_%' THEN 'bronze'
        WHEN table_name LIKE 'silver_%' THEN 'silver'
        WHEN table_name LIKE 'gold_%'   THEN 'gold'
        ELSE 'unknown'
    END AS layer,
    status,
    environment,
    CAST(start_time AS DATE) AS run_date
FROM [gold_log].[deltagen_run_metrics]
"""
        execute_sql(conn, dim_run, f"Create view [{schema}].[dim_run]")

        # View: f_dq_unresolved_fks (if table exists)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'gold_log' AND TABLE_NAME = 'gold_dq_unresolved_fks'"
        )
        has_unresolved_fks = cursor.fetchone()[0] > 0

        if has_unresolved_fks:
            f_dq_unresolved_fks = f"""
CREATE OR ALTER VIEW [{schema}].[f_dq_unresolved_fks] AS
SELECT * FROM [gold_log].[gold_dq_unresolved_fks]
"""
            execute_sql(
                conn, f_dq_unresolved_fks, f"Create view [{schema}].[f_dq_unresolved_fks]"
            )
        else:
            print(f"⊘ Skipped [{schema}].[f_dq_unresolved_fks] - table not found")

        # View: f_dq_unresolved_records (if table exists)
        cursor.execute(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'gold_log' AND TABLE_NAME = 'gold_dq_unresolved_records'"
        )
        has_unresolved_records = cursor.fetchone()[0] > 0

        if has_unresolved_records:
            f_dq_unresolved_records = f"""
CREATE OR ALTER VIEW [{schema}].[f_dq_unresolved_records] AS
SELECT * FROM [gold_log].[gold_dq_unresolved_records]
"""
            execute_sql(
                conn,
                f_dq_unresolved_records,
                f"Create view [{schema}].[f_dq_unresolved_records]",
            )
        else:
            print(
                f"⊘ Skipped [{schema}].[f_dq_unresolved_records] - table not found"
            )

        # Bronze ingestion views
        # Requires shortcut from bronze lakehouse: bronze_control
        # Note: shortcut schemas don't appear in INFORMATION_SCHEMA.SCHEMATA, check tables directly
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'bronze_control' AND TABLE_NAME = 'file_ingestion_metadata_log'")
        has_file_log = cursor.fetchone()[0] > 0

        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'bronze_control' AND TABLE_NAME = 'ingestion_metadata'")
        has_sf_log = cursor.fetchone()[0] > 0

        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'bronze_control' AND TABLE_NAME = 'sharepoint_list_delta_state'")
        has_list_log = cursor.fetchone()[0] > 0

        # f_bronze_file_ingestion — detailed file ingestion log
        if has_file_log:
            f_bronze_file_ingestion = f"""
CREATE OR ALTER VIEW [{schema}].[f_bronze_file_ingestion] AS
SELECT
    run_id,
    source,
    folderName AS folder_name,
    fileName AS file_name,
    filePath AS file_path,
    fileExtension AS file_extension,
    filePeriod AS file_period,
    targetTableName AS target_table,
    status,
    [rowCount] AS row_count,
    errorMessage AS error_message,
    modifiedOn AS source_modified_at,
    loadedOn AS loaded_at,
    CAST(loadedOn AS DATE) AS load_date
FROM [bronze_control].[file_ingestion_metadata_log]
"""
            execute_sql(conn, f_bronze_file_ingestion, f"Create view [{schema}].[f_bronze_file_ingestion]")
        else:
            print(f"⊘ Skipped [{schema}].[f_bronze_file_ingestion] - table not found")

        # f_bronze_salesforce_ingestion — Salesforce ingestion log
        if has_sf_log:
            f_bronze_sf_ingestion = f"""
CREATE OR ALTER VIEW [{schema}].[f_bronze_salesforce_ingestion] AS
SELECT
    run_id,
    source_table,
    execution_status AS status,
    record_count AS row_count,
    last_pulled_timestamp AS loaded_at,
    CAST(last_pulled_timestamp AS DATE) AS load_date
FROM [bronze_control].[ingestion_metadata]
"""
            execute_sql(conn, f_bronze_sf_ingestion, f"Create view [{schema}].[f_bronze_salesforce_ingestion]")
        else:
            print(f"⊘ Skipped [{schema}].[f_bronze_salesforce_ingestion] - table not found")

        # f_bronze_list_ingestion — SharePoint list ingestion delta state
        if has_list_log:
                f_bronze_list_ingestion = f"""
CREATE OR ALTER VIEW [{schema}].[f_bronze_list_ingestion] AS
SELECT
    list_name,
    site_url,
    delta_link,
    updated_at,
    CAST(updated_at AS DATE) AS load_date
FROM [bronze_control].[sharepoint_list_delta_state]
"""
            execute_sql(conn, f_bronze_list_ingestion, f"Create view [{schema}].[f_bronze_list_ingestion]")
        else:
            print(f"⊘ Skipped [{schema}].[f_bronze_list_ingestion] - table not found")

        # f_bronze_ingestion — consolidated view across all bronze sources
        parts = []

        if has_file_log:
            parts.append("""
    SELECT
        run_id,
        source AS source_name,
        targetTableName AS target_table,
        status,
        [rowCount] AS row_count,
        errorMessage AS error_message,
        loadedOn AS loaded_at,
        CAST(loadedOn AS DATE) AS load_date,
        'file' AS ingestion_type
    FROM [bronze_control].[file_ingestion_metadata_log]""")

        if has_list_log:
            parts.append("""
    SELECT
        NULL AS run_id,
        list_name AS source_name,
        'sharepoint.' + list_name AS target_table,
        'SUCCESS' AS status,
        NULL AS row_count,
        NULL AS error_message,
        updated_at AS loaded_at,
        CAST(updated_at AS DATE) AS load_date,
        'sharepoint_list' AS ingestion_type
    FROM [bronze_control].[sharepoint_list_delta_state]""")

        if has_sf_log:
            parts.append("""
    SELECT
        run_id,
        source_table AS source_name,
        'salesforce.' + source_table AS target_table,
        execution_status AS status,
        record_count AS row_count,
        NULL AS error_message,
        last_pulled_timestamp AS loaded_at,
        CAST(last_pulled_timestamp AS DATE) AS load_date,
        'salesforce' AS ingestion_type
    FROM [bronze_control].[ingestion_metadata]""")

        if parts:
            f_bronze_ingestion = (
                f"CREATE OR ALTER VIEW [{schema}].[f_bronze_ingestion] AS"
                + "\n    UNION ALL".join(parts)
            )
            execute_sql(conn, f_bronze_ingestion, f"Create view [{schema}].[f_bronze_ingestion]")
        else:
            print(f"⊘ Skipped [{schema}].[f_bronze_ingestion] - no bronze tables found")

        print(f"\n✓ All views created successfully in [{schema}] schema")
        print(f"  Views are now visible in the SQL Analytics Endpoint!")

    finally:
        conn.close()


def main():
    """Main entry point."""
    # Observability lakehouse SQL endpoint details
    SERVER = "bvsrb7ksrrmeljqi6ly3qjj5x4-opjgncvnr2vurgejlals35bszy.datawarehouse.fabric.microsoft.com"
    DATABASE = "5b4dfb4b-f8be-4194-b871-ddd6a8f59098"  # observability lakehouse ID

    print("=" * 80)
    print("Creating SQL Analytics Endpoint Views for Observability Lakehouse")
    print("=" * 80)
    print()

    try:
        create_views(
            server=SERVER,
            database=DATABASE,
            schema="log",
            shortcut_schema="silver_log",
        )
        print()
        print("=" * 80)
        print("SUCCESS: Views created in SQL Analytics Endpoint")
        print("=" * 80)
        print()
        print("Next steps:")
        print("1. Open the observability lakehouse SQL Analytics Endpoint in Fabric")
        print("2. Expand Schemas → log → Views")
        print("3. You should see: f_pipeline_health, f_dq_summary, f_rejection_rate_trend")
        print("4. The DirectLake semantic model can now access these views")
        print()

    except Exception as e:
        print()
        print("=" * 80)
        print("ERROR: Failed to create views")
        print("=" * 80)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
