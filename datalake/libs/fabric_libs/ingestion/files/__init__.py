"""Reusable file-based ingestion helpers for Fabric/Spark."""

from .file_ingestion import (
    append_to_table,
    format_parallel_results,
    get_metadata_schema,
    print_parallel_results,
    process_files,
    run_ingestion,
    run_ingestion_parallel,
    sanitize_columns,
)
from .io import (
    list_files,
    normalize_columns,
    read_csv_file,
    read_excel_file,
    read_file,
    read_txt_file,
)
from .matching import (
    add_control_attributes,
    get_file_match_condition_flags,
    get_file_match_join_conditions,
)
from .profiling import (
    detect_header_start,
    get_profile_schema,
    profile_files_metadata,
)
from .reconcile import (
    drop_tables_by_prefix,
    reconcile_ingestion,
    test_read_file,
    validate_all_files,
    view_metadata_summary,
)
from .tracking import get_new_files
from .utils import (
    SUPPORTED_EXTENSIONS,
    check_exclude_patterns,
    check_exclude_patterns_udf,
    extract_period,
    extract_period_udf,
    sanitize_name,
    sanitize_name_udf,
    strip_date_tokens,
)

__all__ = [
    "SUPPORTED_EXTENSIONS",
    "add_control_attributes",
    "append_to_table",
    "check_exclude_patterns",
    "check_exclude_patterns_udf",
    "detect_header_start",
    "drop_tables_by_prefix",
    "extract_period",
    "extract_period_udf",
    "format_parallel_results",
    "get_file_match_condition_flags",
    "get_file_match_join_conditions",
    "get_metadata_schema",
    "get_new_files",
    "get_profile_schema",
    "list_files",
    "normalize_columns",
    "print_parallel_results",
    "process_files",
    "profile_files_metadata",
    "read_csv_file",
    "read_excel_file",
    "read_file",
    "read_txt_file",
    "reconcile_ingestion",
    "run_ingestion",
    "run_ingestion_parallel",
    "sanitize_columns",
    "sanitize_name",
    "sanitize_name_udf",
    "strip_date_tokens",
    "test_read_file",
    "validate_all_files",
    "view_metadata_summary",
]
