"""Ingestion orchestration utilities (matching, processing, and summaries)."""

import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.utils import AnalysisException

from .io import list_files, normalize_columns, read_file
from .matching import add_control_attributes
from .tracking import get_new_files
from .utils import sanitize_name


def cast_all_to_string(df: DataFrame) -> DataFrame:
    """Cast all columns to string to avoid type conflicts during schema merge."""
    return df.select([F.col(f"`{c}`").cast("string").alias(c) for c in df.columns])


def append_to_table(spark, df: DataFrame, target_table: str) -> None:
    """Append to or create a Delta table."""
    try:
        spark.table(target_table)
        exists = True
    except AnalysisException:
        exists = False
    # Cast all columns to string to avoid type conflicts
    df_string = cast_all_to_string(df)
    writer = df_string.write.format("delta").option("mergeSchema", "true")
    if exists:
        writer.mode("append").saveAsTable(target_table)
    else:
        writer.mode("overwrite").saveAsTable(target_table)


def sanitize_columns(df: DataFrame) -> DataFrame:
    """Sanitize column names with safe fallbacks and handle duplicates."""
    seen = {}
    new_columns = []
    for i, c in enumerate(df.columns):
        sanitized = sanitize_name(c) or f"col_{i}"
        # Handle duplicates by appending a suffix
        if sanitized in seen:
            seen[sanitized] += 1
            sanitized = f"{sanitized}_{seen[sanitized]}"
        else:
            seen[sanitized] = 0
        new_columns.append(F.col(f"`{c}`").alias(sanitized))
    return df.select(new_columns)


def get_metadata_schema() -> StructType:
    """Schema for the metadata log table."""
    return StructType([
        StructField("run_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("folderName", StringType(), True),
        StructField("sanitizedFolderName", StringType(), True),
        StructField("fileName", StringType(), True),
        StructField("filePath", StringType(), True),
        StructField("fileExtension", StringType(), True),
        StructField("filePeriod", LongType(), True),
        StructField("modifiedOn", TimestampType(), True),
        StructField("loadedOn", TimestampType(), True),
        StructField("targetTableName", StringType(), True),
        StructField("status", StringType(), True),
        StructField("rowCount", LongType(), True),
        StructField("errorMessage", StringType(), True),
    ])


def process_files(spark, files_df: DataFrame, reader_fn: Callable = None, sanitize_cols: bool = True, dry_run: bool = False, run_id: str = None) -> List[Dict[str, Any]]:
    """Read, validate, and optionally write files; return metadata rows.

    Processing modes (set via processingMode column in control table):
        - 'bulk': Groups files by target table and writes once per table.
                  Prevents concurrent write conflicts when multiple folders
                  target the same table.
        - default: Writes each file individually (e.g., where each
                   folder has its own target table).
    """
    if run_id is None:
        run_id = str(uuid.uuid4())
    if reader_fn is None:
        reader_fn = read_file
    required_cols = ["folderName", "sanitizedFolderName", "fileName", "filePath", "fileExtension", "modifiedOn", "targetTableName"]
    for col in required_cols:
        if col not in files_df.columns:
            raise ValueError(f"files_df missing required column: {col}")

    rows = files_df.collect()
    print(f"Processing files count is {len(rows)}")

    metadata_rows: List[Dict[str, Any]] = []

    # Track DataFrames for bulk write mode (grouped by target table)
    bulk_table_dataframes: Dict[str, List[DataFrame]] = {}
    bulk_file_metadata: Dict[str, List[Dict[str, Any]]] = {}

    for row in rows:
        source = getattr(row, "source", None)
        file_period = getattr(row, "filePeriod", None)
        target_table = str(row.targetTableName)
        processing_mode = getattr(row, "processingMode", None) or ""
        is_bulk_mode = processing_mode.lower() == "bulk"

        print(f"  {'Reading' if is_bulk_mode else 'Processing'}: {row.folderName}/{row.fileName} -> {target_table}")

        row_count = 0
        error_message = None
        status = "DRYRUN" if dry_run else "FAILED"

        try:
            incoming_df = reader_fn(spark, row)
            if sanitize_cols:
                incoming_df = sanitize_columns(incoming_df)
            column_normalize = getattr(row, "columnNormalize", None)
            if column_normalize:
                incoming_df = normalize_columns(incoming_df, column_normalize)

            # Cast all columns to string early to avoid type conflicts during
            # unionByName in bulk mode (CANNOT_MERGE_TYPE errors)
            incoming_df = cast_all_to_string(incoming_df)

            # Add source tracking columns (all as strings to avoid type conflicts)
            incoming_df = (
                incoming_df
                .withColumn("_run_id", F.lit(run_id))
                .withColumn("_source", F.lit(source))
                .withColumn("_source_file", F.lit(row.fileName))
                .withColumn("_source_folder", F.lit(row.folderName))
                .withColumn("_source_modified", F.lit(str(row.modifiedOn) if row.modifiedOn else None))
                .withColumn("_source_period", F.lit(str(file_period) if file_period else None))
                .withColumn("_loaded_at", F.lit(str(datetime.utcnow())))
            )

            row_count = int(incoming_df.count())
            if row_count == 0:
                error_message = "File read successfully but contains no data rows"
                status = "EMPTY"
            elif dry_run:
                status = "DRYRUN"
            elif is_bulk_mode:
                # Queue for bulk write later
                status = "PENDING_WRITE"
                if target_table not in bulk_table_dataframes:
                    bulk_table_dataframes[target_table] = []
                    bulk_file_metadata[target_table] = []
                bulk_table_dataframes[target_table].append(incoming_df)
            else:
                # Immediate write (file-by-file mode)
                append_to_table(spark, incoming_df, target_table)
                status = "SUCCESS"
                print(f"   {row_count} rows written")

        except Exception as e:
            status = "FAILED"
            row_count = 0
            error_message = str(e)[:4000]
            print(f"   Error: {error_message[:100]}...")

        # Store metadata
        meta = {
            "run_id": run_id,
            "source": str(source) if source else None,
            "folderName": str(row.folderName) if row.folderName else None,
            "sanitizedFolderName": str(row.sanitizedFolderName) if row.sanitizedFolderName else None,
            "fileName": str(row.fileName) if row.fileName else None,
            "filePath": str(row.filePath) if row.filePath else None,
            "fileExtension": str(row.fileExtension) if row.fileExtension else None,
            "filePeriod": int(file_period) if file_period else None,
            "modifiedOn": row.modifiedOn,
            "loadedOn": datetime.utcnow(),
            "targetTableName": target_table,
            "status": status,
            "rowCount": row_count,
            "errorMessage": error_message,
        }

        if status == "PENDING_WRITE":
            bulk_file_metadata[target_table].append(meta)
        else:
            metadata_rows.append(meta)

    # Bulk write phase - for tables with processingMode='bulk'
    if not dry_run and bulk_table_dataframes:
        for target_table, dfs in bulk_table_dataframes.items():
            file_count = len(dfs)
            print(f"Bulk writing {file_count} files to {target_table}...")

            try:
                # Union all DataFrames for this table
                if len(dfs) == 1:
                    combined_df = dfs[0]
                else:
                    # Use unionByName to handle potential column differences
                    combined_df = dfs[0]
                    for df in dfs[1:]:
                        combined_df = combined_df.unionByName(df, allowMissingColumns=True)

                # Single write operation
                append_to_table(spark, combined_df, target_table)

                # Mark all files as SUCCESS
                for meta in bulk_file_metadata[target_table]:
                    meta["status"] = "SUCCESS"
                    metadata_rows.append(meta)

                total_rows = sum(m["rowCount"] for m in bulk_file_metadata[target_table])
                print(f"   {total_rows} total rows written from {file_count} files")

            except Exception as e:
                error_msg = str(e)[:4000]
                print(f"   Bulk write error for {target_table}: {error_msg[:100]}...")

                # Mark all files as FAILED
                for meta in bulk_file_metadata[target_table]:
                    meta["status"] = "FAILED"
                    meta["errorMessage"] = f"Bulk write failed: {error_msg}"
                    metadata_rows.append(meta)

    return metadata_rows


def run_ingestion(
    spark,
    root_path: Optional[str] = None,
    source_name: str = None,
    control_table: str = "config.file_ingestion_control_attributes",
    metadata_table: str = "control.file_ingestion_metadata_log",
    supported_extensions: Optional[List[str]] = None,
    folders_to_run: Optional[List[str]] = None,
    materialize: bool = True,
    sanitize_cols: bool = True,
    incremental: bool = True,
    *,
    sharepoint_root: Optional[str] = None,
) -> Dict[str, Any]:
    """Run ingestion for a root path.

    `root_path` is preferred; `sharepoint_root` is a legacy alias.
    """
    root = root_path or sharepoint_root
    if not root:
        raise ValueError("root_path (or legacy sharepoint_root) is required")
    if supported_extensions is None:
        supported_extensions = [".csv", ".txt", ".xlsx"]

    import threading
    thread_id = threading.current_thread().name
    folder_str = f"folders={folders_to_run}" if folders_to_run else "all folders"

    print(f"[{thread_id}] Running ingestion for source={source_name} on root={root}, {folder_str}")

    files_df = list_files(spark, root, supported_extensions, folders_to_run=folders_to_run)

    if folders_to_run:
        files_df = files_df.cache()
        _ = files_df.count()

    initial_count = files_df.count()
    print(f"   [{thread_id}] Found {initial_count} files for {folder_str}")

    files_to_match = files_df
    match_input_count = initial_count
    prefiltered_incremental = False
    found_count = initial_count

    if incremental:
        try:
            metadata_df = spark.table(metadata_table)
            files_to_match = get_new_files(files_df, metadata_df, source_name, folders_to_run)
            match_input_count = files_to_match.count()
            prefiltered_incremental = True
            found_count = match_input_count
        except AnalysisException:
            pass

    if prefiltered_incremental and match_input_count == 0:
        if folders_to_run:
            files_df.unpersist()
        return {
            "status": "ok",
            "files_found": found_count,
            "files_matched": 0,
            "files_processed": 0,
            "success": 0,
            "failed": 0,
            "empty": 0,
            "materialized": materialize,
            "message": "No new files found"
        }

    try:
        control_df = spark.table(control_table)
    except AnalysisException:
        if folders_to_run:
            files_df.unpersist()
        return {
            "status": "error",
            "files_found": found_count,
            "message": f"Control table {control_table} not found"
        }

    matched_df = add_control_attributes(spark, files_to_match, control_df, source_name=source_name)
    matched_df = matched_df.withColumn("source", F.lit(source_name))
    matched_count = matched_df.count()

    if matched_count == 0:
        print(f"   [{thread_id}] WARNING: Found {match_input_count} files but ZERO matched control table rules")
        print(f"   [{thread_id}] Check {control_table} has entries for these folders: {folders_to_run}")
        if folders_to_run:
            files_df.unpersist()
        return {
            "status": "ok",
            "files_found": found_count,
            "files_matched": 0,
            "files_processed": 0,
            "success": 0,
            "failed": 0,
            "empty": 0,
            "materialized": materialize,
            "message": "No files matched control rules"
        }

    if incremental and not prefiltered_incremental:
        try:
            metadata_df = spark.table(metadata_table)
            matched_df = get_new_files(matched_df, metadata_df, source_name, folders_to_run)
            new_count = matched_df.count()
        except AnalysisException:
            new_count = matched_count
    else:
        new_count = matched_count

    if new_count == 0:
        print(f"   [{thread_id}] All {matched_count} matched files already processed (incremental mode)")
        if folders_to_run:
            files_df.unpersist()
        return {
            "status": "ok",
            "files_found": found_count,
            "files_matched": matched_count,
            "files_processed": 0,
            "success": 0,
            "failed": 0,
            "empty": 0,
            "materialized": materialize,
            "message": "All files already processed"
        }

    print(f"   [{thread_id}] Processing {new_count} files for {folder_str}")

    # Generate a unique run ID for this ingestion run
    run_id = str(uuid.uuid4())

    metadata_rows = process_files(spark, matched_df, reader_fn=read_file, sanitize_cols=sanitize_cols, dry_run=not materialize, run_id=run_id)
    metadata_schema = get_metadata_schema()
    metadata_df = spark.createDataFrame(metadata_rows, metadata_schema)

    try:
        spark.table(metadata_table)
        metadata_df.write.format("delta").mode("append").saveAsTable(metadata_table)
    except AnalysisException:
        metadata_df.write.format("delta").mode("overwrite").saveAsTable(metadata_table)

    if folders_to_run:
        files_df.unpersist()

    success_count = sum(1 for r in metadata_rows if r["status"] == "SUCCESS")
    failed_count = sum(1 for r in metadata_rows if r["status"] == "FAILED")
    empty_count = sum(1 for r in metadata_rows if r["status"] == "EMPTY")

    print(f"   [{thread_id}] Completed {folder_str}: {success_count} success, {failed_count} failed, {empty_count} empty")

    return {
        "status": "ok",
        "source": source_name,
        "files_found": found_count,
        "files_matched": matched_count,
        "files_processed": new_count,
        "success": success_count,
        "failed": failed_count,
        "empty": empty_count,
        "materialized": materialize,
    }


def run_ingestion_parallel(
    spark,
    root_path: Optional[str] = None,
    source_name: str = None,
    control_table: str = "config.file_ingestion_control_attributes",
    metadata_table: str = "control.file_ingestion_metadata_log",
    supported_extensions: Optional[List[str]] = None,
    folders_to_run: Optional[List[str]] = None,
    materialize: bool = True,
    sanitize_cols: bool = True,
    incremental: bool = True,
    workers: int = 4,
    folder_prefix: Optional[str] = None,
    *,
    sharepoint_root: Optional[str] = None,
) -> Dict[str, Any]:
    """Run ingestion across multiple folders in parallel.

    Args:
        folder_prefix: If set, only process folders whose name starts with this
            string (case-insensitive). Useful when a root path contains many
            subfolders but only a subset should be ingested.
    """
    root = root_path or sharepoint_root
    if not root:
        raise ValueError("root_path (or legacy sharepoint_root) is required")
    if supported_extensions is None:
        supported_extensions = [".csv", ".txt", ".xlsx"]

    if not folders_to_run:
        try:
            files_df = list_files(spark, root, supported_extensions)
            if files_df is not None:
                discovered = [r[0] for r in files_df.select("folderName").distinct().collect()]
                folders_to_run = sorted([f for f in discovered if f])
                if folder_prefix:
                    prefix_lower = folder_prefix.lower()
                    folders_to_run = [f for f in folders_to_run if f.lower().startswith(prefix_lower)]
                    print(f"   folder_prefix='{folder_prefix}' -> {len(folders_to_run)} matching folders")
        except Exception:
            folders_to_run = None

    if not folders_to_run:
        return run_ingestion(
            spark,
            root_path=root,
            source_name=source_name,
            control_table=control_table,
            metadata_table=metadata_table,
            supported_extensions=supported_extensions,
            folders_to_run=folders_to_run,
            materialize=materialize,
            sanitize_cols=sanitize_cols,
            incremental=incremental,
        )

    from concurrent.futures import ThreadPoolExecutor, as_completed

    actual_workers = min(max(1, int(workers)), len(folders_to_run))
    print(f"Starting parallel ingestion with {actual_workers} workers for {len(folders_to_run)} folders: {folders_to_run}")

    results: Dict[str, Any] = {}
    summaries: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=actual_workers) as ex:
        futures = {
            ex.submit(
                run_ingestion,
                spark,
                root,
                source_name,
                control_table,
                metadata_table,
                supported_extensions,
                [folder],
                materialize,
                sanitize_cols,
                incremental,
            ): folder
            for folder in folders_to_run
        }

        for fut in as_completed(futures):
            folder = futures[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = {"status": "error", "message": str(e), "folder": folder}
            results[folder] = res
            if isinstance(res, dict):
                summaries.append(res)

    total_matched = sum(int(r.get("files_matched", 0) or 0) for r in summaries)
    total_processed = sum(int(r.get("files_processed", 0) or 0) for r in summaries)
    total_success = sum(int(r.get("success", 0) or 0) for r in summaries)
    total_failed = sum(int(r.get("failed", 0) or 0) for r in summaries)

    return {
        "status": "ok",
        "source": source_name,
        "folders": len(folders_to_run),
        "files_matched": total_matched,
        "files_processed": total_processed,
        "success": total_success,
        "failed": total_failed,
        "materialized": materialize,
        "results": results,
    }


def format_parallel_results(res: Dict[str, Any], show_details: bool = True) -> str:
    """Return a human-friendly multiline summary for parallel results."""
    if not isinstance(res, dict):
        return str(res)

    lines = []
    src = res.get("source") or "unknown"
    folders = res.get("folders")
    lines.append(f"Source: {src}")
    lines.append(f"Folders run: {folders}")
    lines.append("")

    fm = res.get("files_matched", 0) or 0
    fp = res.get("files_processed", 0) or 0
    suc = res.get("success", 0) or 0
    fail = res.get("failed", 0) or 0
    mat = bool(res.get("materialized"))
    lines.append(f"Files matched: {fm}, processed: {fp}, success: {suc}, failed: {fail}, materialized: {mat}")
    lines.append("")

    if show_details and isinstance(res.get("results"), dict):
        rows = []
        header = ("Folder", "Found", "Matched", "Processed", "Success", "Failed", "Empty", "Materialized")
        rows.append(header)
        for folder in sorted(res["results"].keys()):
            info = res["results"].get(folder) or {}
            found = info.get("files_found", 0) or 0
            matched = info.get("files_matched", 0) or 0
            processed = info.get("files_processed", 0) or 0
            success = info.get("success", 0) or 0
            failed = info.get("failed", 0) or 0
            empty = info.get("empty", 0) or 0
            materialized = bool(info.get("materialized"))
            rows.append((folder, str(found), str(matched), str(processed), str(success), str(failed), str(empty), str(materialized)))

        col_widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
        fmt = "  ".join("{:<%d}" % w for w in col_widths)
        lines.append(fmt.format(*rows[0]))
        lines.append("-" * (sum(col_widths) + 2 * (len(col_widths) - 1)))
        for r in rows[1:]:
            lines.append(fmt.format(*r))

    return "\n".join(lines)


def print_parallel_results(res: Dict[str, Any], show_details: bool = True) -> None:
    """Pretty-print parallel results to console."""
    out = format_parallel_results(res, show_details=show_details)
    print(out)
