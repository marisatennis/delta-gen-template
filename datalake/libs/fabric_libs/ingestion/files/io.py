"""File listing and reader utilities for ingestion."""

from typing import Any, Dict, List, Optional

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType


try:
    import notebookutils
    ms = notebookutils.mssparkutils
except Exception:
    ms = None
    print("notebookutils not available; ms.fs access will fail outside Synapse environments.")

try:
    import pandas as pd
    from io import BytesIO
    import warnings
except Exception:
    pd = None
    BytesIO = None
    warnings = None
    print("pandas/openpyxl not available; Excel readers will fail without these packages. Install with `pip install pandas openpyxl`.")


def list_files(spark, root_path: Optional[str] = None, supported_extensions: Optional[List[str]] = None, *, folders_to_run: Optional[List[str]] = None, sharepoint_root: Optional[str] = None) -> DataFrame:
    """List files under a root path using Fabric/Synapse utilities.

    Args:
        root_path: Root folder path (preferred).
        supported_extensions: File extensions to include (with or without dots).
        folders_to_run: If provided, only scan these specific subfolders instead
            of listing the entire root. Avoids slow full scans when only a
            subset of folders is needed.
        sharepoint_root: Legacy alias for root_path.
    """
    root = root_path or sharepoint_root
    if not root:
        raise ValueError("root_path (or legacy sharepoint_root) is required")
    if ms is None:
        raise RuntimeError("mssparkutils not available in this environment; list_files requires Synapse notebook utilities")
    supported = {ext.lower().lstrip(".") for ext in (supported_extensions or [])}
    rows: List[Dict[str, Any]] = []

    if folders_to_run:
        # Scan only the requested subfolders — avoids listing the entire root
        folder_items = []
        for folder_name in folders_to_run:
            folder_path = f"{root.rstrip('/')}/{folder_name}"
            try:
                folder_items.append((folder_name, folder_path))
            except Exception:
                continue
    else:
        # Full scan — list all subfolders under root
        folder_items = []
        for folder in ms.fs.ls(root):
            if folder.isDir:
                folder_path = folder.path.rstrip("/")
                folder_name = folder_path.split("/")[-1]
                folder_items.append((folder_name, folder_path))

    for folder_name, folder_path in folder_items:
        try:
            files = ms.fs.ls(folder_path)
        except Exception:
            continue
        for file in files:
            if file.isDir:
                continue
            file_name = file.name
            file_ext = file_name[file_name.rfind(".") + 1:].lower() if "." in file_name else ""
            if supported and file_ext not in supported:
                continue
            rows.append({
                "folderName": folder_name,
                "fileName": file_name,
                "filePath": file.path,
                "fileExtension": file_ext,
                "modifiedOnMillis": file.modifyTime,
            })
    if not rows:
        schema = StructType([
            StructField("folderName", StringType(), True),
            StructField("fileName", StringType(), True),
            StructField("filePath", StringType(), True),
            StructField("fileExtension", StringType(), True),
            StructField("modifiedOnMillis", LongType(), True),
        ])
        return spark.createDataFrame([], schema)
    df = spark.createDataFrame(rows)
    return df.withColumn("modifiedOn", F.from_unixtime(F.col("modifiedOnMillis") / 1000).cast("timestamp")).drop("modifiedOnMillis")


# Column-normalization used by readers when requested

def normalize_columns(df: DataFrame, normalize_pattern: Optional[str]) -> DataFrame:
    """Normalize column names based on a prefix:target rule."""
    if not normalize_pattern or ":" not in normalize_pattern:
        return df
    prefix, target = normalize_pattern.split(":", 1)
    prefix_lower = prefix.lower()
    renamed_cols = []
    found_match = False
    for col in df.columns:
        if col.lower().startswith(prefix_lower):
            if not found_match:
                renamed_cols.append(F.col(f"`{col}`").alias(target))
                found_match = True
        else:
            renamed_cols.append(F.col(f"`{col}`"))
    return df.select(*renamed_cols) if renamed_cols else df


def read_csv_file(
    spark,
    file_path: str,
    delimiter: str = ",",
    header_start: int = None,
    header_marker: Optional[str] = None,
    multi_line: bool = True,
) -> DataFrame:
    """Read a CSV with optional header start row or header marker.

    All columns are read as strings (inferSchema=false) to avoid type inference
    failures (CANNOT_DETERMINE_TYPE) and type conflicts during schema merge.

    When headerStart > 1 or headerMarker is set, the file is read with a wide
    schema (500 string columns) so that Spark does not infer column count from
    row 0 — which may be a title/metadata line with fewer fields than the real
    header.  The actual column count is derived from the header row itself.

    Args:
        spark: Active SparkSession.
        file_path: Path to the CSV file.
        delimiter: Column delimiter (default: comma).
        header_start: 1-based row number where the header is located.
                     If None or 1, uses the first row as header.
        header_marker: String that the header row's first column starts with.
                      Takes precedence over header_start when both are set.
        multi_line: Whether to enable multiLine parsing (default: True).
                   Set to False for files where multiLine causes Spark to
                   merge rows incorrectly.
    """
    delim = delimiter or ","
    multi_line_str = "true" if multi_line else "false"
    if (header_start is None or header_start <= 1) and not header_marker:
        return (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("ignoreEmptyLines", "false")
            .option("delimiter", delim)
            .option("multiLine", multi_line_str)
            .option("escape", '"')
            .option("quote", '"')
            .load(file_path)
        )

    # For headerStart > 1 or header_marker: read with a wide schema so Spark
    # does not infer column count from row 0 (which may be a title/metadata
    # line with fewer fields than the real header).  Unused trailing columns
    # are simply null and trimmed after we identify the true header row.
    _MAX_COLS = 500
    wide_schema = StructType([StructField(f"_c{i}", StringType(), True) for i in range(_MAX_COLS)])

    raw_df = (
        spark.read
        .format("csv")
        .schema(wide_schema)
        .option("header", "false")
        .option("ignoreEmptyLines", "false")
        .option("delimiter", delim)
        .option("multiLine", multi_line_str)
        .option("escape", '"')
        .option("quote", '"')
        .load(file_path)
    )

    # Index rows once — reused for both header lookup and data filtering
    indexed_rdd = raw_df.rdd.zipWithIndex()

    # Locate the header row
    if header_marker:
        marker = header_marker.strip().lower()
        matches = indexed_rdd.filter(
            lambda row_idx: str(row_idx[0][0] or "").lower().startswith(marker)
        ).take(1)
        if not matches:
            raise ValueError(f"No rows found matching headerMarker='{header_marker}'")
        header_row, header_idx = matches[0]
    else:
        header_idx = header_start - 1
        matches = indexed_rdd.filter(
            lambda row_idx: row_idx[1] == header_idx
        ).take(1)
        if not matches:
            raise ValueError(f"No rows found at headerStart={header_start}")
        header_row = matches[0][0]

    # Derive actual column names from the header row (rightmost non-null value)
    header_values = list(header_row)
    last_non_null = max(
        (i for i, v in enumerate(header_values) if v is not None), default=-1
    )
    if last_non_null < 0:
        raise ValueError(f"Header row at position {header_start or 'marker'} is empty")

    col_names = []
    seen: dict[str, int] = {}
    for i in range(last_non_null + 1):
        val = header_values[i]
        name = str(val).strip() if val is not None else ""
        name = name if name else f"_c{i}"
        # Deduplicate column names (same logic as _normalize_excel_columns)
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        col_names.append(name)
    actual_count = len(col_names)

    # Extract data rows after the header, trimmed to actual column count
    data_rdd = indexed_rdd.filter(
        lambda row_idx: row_idx[1] > header_idx
    ).map(lambda row_idx: tuple(row_idx[0][i] for i in range(actual_count)))

    if data_rdd.isEmpty():
        raise ValueError(f"No data rows found after header at row {header_start or 'marker'}")

    final_schema = StructType([StructField(name, StringType(), True) for name in col_names])
    return spark.createDataFrame(data_rdd, final_schema)


def read_txt_file(spark, file_path: str, delimiter: str = "\t", header_start: int = None, header_marker: Optional[str] = None, multi_line: bool = True) -> DataFrame:
    """Read a TXT file.  Delegates to read_csv_file with tab delimiter."""
    return read_csv_file(spark, file_path, delimiter=delimiter or "\t", header_start=header_start, header_marker=header_marker, multi_line=multi_line)


def _normalize_excel_columns(cols: List[Any]) -> List[str]:
    """Normalize Excel column names with safe fallbacks and dedupe."""
    seen = {}
    normalized: List[str] = []
    for i, c in enumerate(cols):
        name = str(c) if c is not None and c == c else f"_c{i}"
        name = name.strip() or f"_c{i}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        normalized.append(name)
    return normalized


def read_excel_file(spark, file_path: str, sheet_name: Optional[str] = None, sheet_match_type: str = "first", header_start: Optional[int] = None, header_marker: Optional[str] = None) -> DataFrame:
    """Read an Excel file into Spark via pandas/openpyxl.

    Args:
        spark: Active SparkSession.
        file_path: Path to the Excel file.
        sheet_name: Sheet name pattern to match (optional).
        sheet_match_type: How to match the sheet name:
            - "exact": case-insensitive exact match.
            - "prefix": startswith match; if the pattern contains '%',
              the '%' is stripped and a contains match is used instead.
            - "contains": case-insensitive substring match.
            - "first" (default): use the first sheet in the workbook.
        header_start: 1-based row number where the header is located.
                     If None or 1, uses the first row as header.
        header_marker: String that the header row's first column starts with.
                      Takes precedence over header_start when both are set.
    """
    if pd is None or BytesIO is None:
        raise RuntimeError("pandas/openpyxl not available; cannot read Excel without these packages")

    bin_df = spark.read.format("binaryFile").load(file_path)
    if bin_df.rdd.isEmpty():
        raise FileNotFoundError(f"File not found or unreadable: {file_path}")

    raw_bytes = bin_df.select("content").head()[0]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        xls = pd.ExcelFile(BytesIO(raw_bytes))

    sheet_names = list(xls.sheet_names or [])
    if not sheet_names:
        raise ValueError("No sheets found in Excel file")

    chosen_sheet = sheet_names[0]
    if sheet_name:
        sheet_name_lower = sheet_name.lower()
        if sheet_match_type == "exact":
            for s in sheet_names:
                if s.lower() == sheet_name_lower:
                    chosen_sheet = s
                    break
        elif sheet_match_type == "prefix":
            if "%" in sheet_name_lower:
                pattern = sheet_name_lower.replace("%", "")
                for s in sheet_names:
                    if pattern in s.lower():
                        chosen_sheet = s
                        break
            else:
                for s in sheet_names:
                    if s.lower().startswith(sheet_name_lower):
                        chosen_sheet = s
                        break
        elif sheet_match_type == "contains":
            for s in sheet_names:
                if sheet_name_lower in s.lower():
                    chosen_sheet = s
                    break
        # "first" or unrecognised: keep chosen_sheet = sheet_names[0]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        if header_marker or (header_start is not None and header_start > 1):
            pdf = xls.parse(chosen_sheet, header=None)
            if header_marker:
                marker = header_marker.strip().lower()
                first_col = pdf.iloc[:, 0].astype(str).str.lower()
                match_mask = first_col.str.startswith(marker)
                if not match_mask.any():
                    # Try other sheets before giving up
                    found_alt = False
                    for alt_sheet in sheet_names:
                        if alt_sheet == chosen_sheet:
                            continue
                        alt_pdf = xls.parse(alt_sheet, header=None)
                        alt_first = alt_pdf.iloc[:, 0].astype(str).str.lower()
                        alt_mask = alt_first.str.startswith(marker)
                        if alt_mask.any():
                            chosen_sheet = alt_sheet
                            pdf = alt_pdf
                            match_mask = alt_mask
                            found_alt = True
                            break
                    if not found_alt:
                        raise ValueError(f"No rows found matching headerMarker='{header_marker}' in any sheet")
                header_pos = match_mask.idxmax()
            else:
                header_pos = header_start - 1
                if header_pos >= len(pdf):
                    raise ValueError(f"headerStart={header_start} exceeds row count ({len(pdf)}) in sheet '{chosen_sheet}'")
            pdf.columns = pdf.iloc[header_pos]
            pdf = pdf.iloc[header_pos + 1:]
        else:
            pdf = xls.parse(chosen_sheet, header=0)

    pdf.columns = _normalize_excel_columns(list(pdf.columns))
    pdf = pdf.astype("string")
    return spark.createDataFrame(pdf)


def read_file(spark, file_row) -> DataFrame:
    """Dispatch to the correct reader based on file_row attributes.

    Supported attributes from file_row (all optional except filePath/fileExtension):
        filePath, fileExtension, delimiter, headerStart, headerMarker,
        sheetName, sheetMatchType, multiLine.
    """
    ext = (file_row.fileExtension or "").lower().lstrip(".")
    delimiter = getattr(file_row, "delimiter", None)
    header_start = getattr(file_row, "headerStart", None)
    header_marker = getattr(file_row, "headerMarker", None)
    sheet_name = getattr(file_row, "sheetName", None)
    sheet_match_type = getattr(file_row, "sheetMatchType", "first")
    multi_line_attr = getattr(file_row, "multiLine", None)
    # Default to True if not specified or empty in attributes
    multi_line = True if not multi_line_attr else str(multi_line_attr).lower() not in ("false", "0", "no")

    if ext in ("csv",):
        df = read_csv_file(
            spark,
            file_row.filePath,
            delimiter=delimiter,
            header_start=header_start,
            header_marker=header_marker,
            multi_line=multi_line,
        )
    elif ext in ("txt",):
        df = read_txt_file(spark, file_row.filePath, delimiter=delimiter or "\t", header_start=header_start, header_marker=header_marker, multi_line=multi_line)
    elif ext in ("xls", "xlsx"):
        df = read_excel_file(spark, file_row.filePath, sheet_name=sheet_name, sheet_match_type=sheet_match_type, header_start=header_start, header_marker=header_marker)
    else:
        raise ValueError(f"Unsupported extension: {ext}")

    return df
