"""Incremental ingestion helpers based on metadata logs."""

from typing import List, Optional

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


def get_new_files(
    files_df: DataFrame,
    metadata_df: DataFrame,
    source_name: Optional[str] = None,
    folders: Optional[List[str]] = None,
) -> DataFrame:
    """Return only files not yet successfully loaded, using a file-level check.

    Compares (folderName, fileName, modifiedOn) against the metadata log so that:
    - Re-runs never produce duplicate bronze rows.
    - Files older than the folder watermark that were skipped are still picked up.
    - A genuinely re-uploaded file (same name, new modifiedOn) is loaded as a new version.

    Args:
        files_df: DataFrame of files to check.
        metadata_df: Metadata log table.
        source_name: Optional source name to scope metadata.
    """
    if metadata_df is None or metadata_df.rdd.isEmpty():
        return files_df

    required_cols = {"folderName", "fileName", "modifiedOn", "status"}
    if not required_cols.issubset(set(metadata_df.columns)):
        return files_df

    already_loaded = metadata_df.filter(~F.upper(F.col("status")).isin(["FAILED", "DRYRUN"]))

    if source_name and "source" in metadata_df.columns:
        already_loaded = already_loaded.filter(F.col("source") == source_name)

    if already_loaded.rdd.isEmpty():
        return files_df

    loaded_keys = already_loaded.select("folderName", "fileName", "modifiedOn").distinct()

    new_files = files_df.join(loaded_keys, on=["folderName", "fileName", "modifiedOn"], how="left_anti")

    if new_files.rdd.isEmpty():
        scope = f"source={source_name}" if source_name else None
        folder_list = folders
        if folder_list is None:
            folder_list = [r[0] for r in files_df.select("folderName").distinct().collect() if r[0]]
        if folder_list:
            for folder in sorted(folder_list):
                if scope:
                    print(f"No new files found for {scope}, folder={folder}.")
                else:
                    print(f"No new files found for folder={folder}.")
        else:
            suffix = f" for {scope}" if scope else ""
            print(f"No new files found{suffix}.")

    return new_files
