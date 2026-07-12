"""Reconciliation and metadata inspection helpers."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException

from .io import list_files, normalize_columns, read_file
from .file_ingestion import sanitize_columns
from .matching import add_control_attributes
from .utils import extract_period_udf


def reconcile_ingestion(
    spark,
    root_path: Optional[str] = None,
    source_name: str = None,
    control_table: str = "config.file_ingestion_control_attributes",
    metadata_table: str = "control.file_ingestion_metadata_log",
    supported_extensions: Optional[List[str]] = None,
    show_details: bool = True,

) -> DataFrame:
    """Join file listing with control and metadata to show ingestion status."""
    root = root_path
    if not root:
        raise ValueError("root_path is required")
    if supported_extensions is None:
        supported_extensions = [".csv", ".txt", ".xlsx"]
    all_files = list_files(spark, root, supported_extensions).withColumn(
        "filePeriod",
        F.coalesce(
            extract_period_udf(F.col("fileName")),
            ((F.year(F.col("modifiedOn")) % 100) * 10000) + (F.month(F.col("modifiedOn")) * 100) + F.lit(1),
        ),
    )
    try:
        control_df = spark.table(control_table)
    except AnalysisException:
        return None
    matched_files = add_control_attributes(spark, all_files, control_df, source_name=source_name)
    try:
        metadata_df = spark.table(metadata_table)
        source_metadata = metadata_df.filter(F.col("source") == source_name)
        from pyspark.sql import Window
        w = Window.partitionBy("folderName", "fileName").orderBy(F.desc("loadedOn"))
        latest_metadata = source_metadata.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn").select(
            F.col("folderName").alias("meta_folder"),
            F.col("fileName").alias("meta_file"),
            F.col("status").alias("ingestion_status"),
            F.col("loadedOn"),
            F.col("rowCount"),
            F.col("errorMessage"),
        )
    except AnalysisException:
        latest_metadata = None
    recon = all_files.withColumn("in_source", F.lit(True)).select("folderName", "fileName", "filePath", "fileExtension", "modifiedOn", "filePeriod", "in_source")
    matched_keys = matched_files.select(
        F.col("folderName").alias("m_folder"),
        F.col("fileName").alias("m_file"),
        F.col("targetTableName"),
        F.lit(True).alias("matched_control"),
    ).distinct()
    recon = recon.join(matched_keys, (recon.folderName == matched_keys.m_folder) & (recon.fileName == matched_keys.m_file), "left").drop("m_folder", "m_file")
    if latest_metadata is not None:
        recon = recon.join(latest_metadata, (recon.folderName == latest_metadata.meta_folder) & (recon.fileName == latest_metadata.meta_file), "left").drop("meta_folder", "meta_file")
    else:
        recon = recon.withColumn("ingestion_status", F.lit(None).cast("string")).withColumn("loadedOn", F.lit(None).cast("timestamp")).withColumn("rowCount", F.lit(None).cast("long")).withColumn("errorMessage", F.lit(None).cast("string"))
    recon = recon.withColumn(
        "recon_status",
        F.when(F.col("ingestion_status") == "SUCCESS", "INGESTED")
         .when(F.col("ingestion_status") == "FAILED", "FAILED")
         .when(F.col("ingestion_status") == "EMPTY", "EMPTY")
         .when(F.col("ingestion_status") == "DRYRUN", "DRYRUN")
         .when(F.col("matched_control") == True, "PENDING")
         .otherwise("UNMATCHED"),
    )
    result = recon.select(
        "folderName",
        "fileName",
        "fileExtension",
        "modifiedOn",
        "filePeriod",
        F.coalesce(F.col("matched_control"), F.lit(False)).alias("matched_control"),
        "targetTableName",
        "recon_status",
        "ingestion_status",
        "loadedOn",
        "rowCount",
        "errorMessage",
    ).orderBy("folderName", "fileName")
    return result


def view_metadata_summary(spark, metadata_table: str = "control.file_ingestion_metadata_log"):
    """Show status counts and recent failures from the metadata table."""
    try:
        meta_df = spark.table(metadata_table)
        summary = meta_df.groupBy("source", "status").agg(F.count("*").alias("files"), F.sum("rowCount").alias("total_rows"), F.max("loadedOn").alias("last_load")).orderBy("source", "status")
        summary.show(truncate=False)
        failures = meta_df.filter(F.col("status") == "FAILED").orderBy(F.desc("loadedOn")).limit(10)
        if failures.count() > 0:
            failures.select("source", "folderName", "fileName", "errorMessage", "loadedOn").show(truncate=50)
    except AnalysisException:
        print(f"Metadata table {metadata_table} not found")


def test_read_file(spark, file_path: str, extension: str, sheet_name: str = None, sheet_match_type: str = "first",
                   header_start: int = None, header_marker: str = None, delimiter: str = None, multi_line: bool = None):
    """Read a single file for quick validation.

    Args:
        spark: Active SparkSession.
        file_path: Path to the file.
        extension: File extension (csv, xlsx, txt).
        sheet_name: Sheet name pattern for Excel files.
        sheet_match_type: Sheet matching strategy (exact/prefix/contains/first).
        header_start: 1-based row number where the header is located.
        header_marker: String that the header row's first column starts with.
        delimiter: Column delimiter (default depends on file type).
        multi_line: Whether to enable multiLine CSV parsing (default: True).
    """
    from types import SimpleNamespace
    test_row = SimpleNamespace(
        filePath=file_path,
        fileExtension=extension,
        folderName="TEST",
        fileName=file_path.split("/")[-1],
        modifiedOn=datetime.utcnow(),
        delimiter=delimiter,
        headerStart=header_start,
        headerMarker=header_marker,
        sheetName=sheet_name,
        sheetMatchType=sheet_match_type,
        multiLine=multi_line,
        columnNormalize=None,
    )
    df = read_file(spark, test_row)
    print(f"Read {df.count()} rows, {len(df.columns)} columns")
    print(f"   Columns: {df.columns}")
    return df


def validate_all_files(
    spark,
    root_path: str,
    source_name: str,
    control_table: str = "config.file_ingestion_control_attributes",
    supported_extensions: Optional[List[str]] = None,
    folders: Optional[List[str]] = None,
    sanitize_cols: bool = True,
    show_columns: bool = False,
) -> DataFrame:
    """Read every matched file in dry-run mode and report results as a DataFrame.

    This is the bulk validation tool: it matches all files against the control
    config, reads each one (without writing), and returns a summary with row
    counts, column counts, status, and any errors.

    Args:
        spark: Active SparkSession.
        root_path: Root folder path (e.g. 'Files/Source-Data').
        source_name: Source name for control matching (e.g. 'Source-Monthly').
        control_table: Control table name.
        supported_extensions: File extensions to include.
        folders: Optional list of folder names to restrict to.
        sanitize_cols: Whether to sanitize column names (default True).
        show_columns: Include column names in the output (default False).

    Returns:
        DataFrame with columns: folderName, fileName, fileExtension, targetTableName,
        status, row_count, col_count, columns (if show_columns), error_message.
    """
    if supported_extensions is None:
        supported_extensions = [".csv", ".txt", ".xlsx"]

    files_df = list_files(spark, root_path, supported_extensions)
    if folders:
        files_df = files_df.filter(F.col("folderName").isin(folders))

    try:
        control_df = spark.table(control_table)
    except AnalysisException:
        print(f"Control table {control_table} not found")
        return None

    matched_df = add_control_attributes(spark, files_df, control_df, source_name=source_name)
    matched_count = matched_df.count()
    if matched_count == 0:
        print(f"No files matched control rules for source={source_name}")
        return None

    print(f"Validating {matched_count} matched files...")
    rows = matched_df.collect()
    results: List[Dict[str, Any]] = []

    for i, row in enumerate(rows, 1):
        folder = str(row.folderName) if row.folderName else ""
        file_name = str(row.fileName) if row.fileName else ""
        ext = str(row.fileExtension) if row.fileExtension else ""
        target = str(row.targetTableName) if row.targetTableName else ""

        result = {
            "folderName": folder,
            "fileName": file_name,
            "fileExtension": ext,
            "targetTableName": target,
            "status": "FAILED",
            "row_count": 0,
            "col_count": 0,
            "columns": "",
            "error_message": None,
        }

        try:
            df = read_file(spark, row)
            if sanitize_cols:
                df = sanitize_columns(df)
            col_normalize = getattr(row, "columnNormalize", None)
            if col_normalize:
                df = normalize_columns(df, col_normalize)

            count = df.count()
            cols = df.columns
            result["status"] = "OK" if count > 0 else "EMPTY"
            result["row_count"] = count
            result["col_count"] = len(cols)
            result["columns"] = ", ".join(cols)
            print(f"  [{i}/{matched_count}] OK {folder}/{file_name} -> {count} rows, {len(cols)} cols")
        except Exception as e:
            result["error_message"] = str(e)[:2000]
            print(f"  [{i}/{matched_count}] FAILED {folder}/{file_name} -> {str(e)[:100]}")

        results.append(result)

    schema_fields = [
        StructField("folderName", StringType(), True),
        StructField("fileName", StringType(), True),
        StructField("fileExtension", StringType(), True),
        StructField("targetTableName", StringType(), True),
        StructField("status", StringType(), True),
        StructField("row_count", LongType(), True),
        StructField("col_count", IntegerType(), True),
        StructField("columns", StringType(), True),
        StructField("error_message", StringType(), True),
    ]
    result_df = spark.createDataFrame(results, StructType(schema_fields))

    if not show_columns:
        result_df = result_df.drop("columns")

    # Print summary
    ok = sum(1 for r in results if r["status"] == "OK")
    empty = sum(1 for r in results if r["status"] == "EMPTY")
    failed = sum(1 for r in results if r["status"] == "FAILED")
    print(f"\nSummary: {ok} OK, {empty} EMPTY, {failed} FAILED out of {len(results)} files")

    return result_df.orderBy("folderName", "fileName")


def drop_tables_by_prefix(spark, prefixes: List[str], dry_run: bool = True):
    """Drop tables by prefix in the current database."""
    current_db = spark.catalog.currentDatabase()
    all_tables = [t for t in spark.catalog.listTables(current_db) if not t.isTemporary and t.tableType != "VIEW"]
    candidates = []
    for t in all_tables:
        nm = (t.name or "").lower()
        if any(nm.startswith(p.lower()) for p in prefixes):
            candidates.append((t.database or current_db, t.name))
    print(f"Tables to drop: {len(candidates)}")
    for db, nm in candidates:
        print(f"   {db}.{nm}")
    if not dry_run and candidates:
        for db, nm in candidates:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {db}.{nm}")
                print(f"   Dropped {db}.{nm}")
            except Exception as e:
                print(f"   Failed to drop {db}.{nm}: {e}")
    elif dry_run:
        print("DRY RUN: Set dry_run=False to actually drop tables")
