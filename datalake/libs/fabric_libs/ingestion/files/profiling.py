"""Lightweight profiling helpers for control-table design."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType, TimestampType

try:
    import pandas as pd
except Exception:
    pd = None
    print("pandas not available; profiling and Excel introspection require pandas. Install with `pip install pandas openpyxl`.")

from .utils import extract_period


def detect_header_start(rows: list) -> Optional[int]:
    """Heuristic to detect header row index from sample rows."""
    if not rows:
        return None
    for idx, row in enumerate(rows):
        if row is None:
            continue
        cells = [c for c in row if c is not None and str(c).strip() != ""]
        if not cells:
            continue
        str_like = sum(1 for c in cells if isinstance(c, str))
        if str_like >= max(1, len(cells) // 2) or len(cells) >= max(1, len(row) // 2):
            return idx + 1
    return None


def get_profile_schema() -> StructType:
    """Schema for profile_files_metadata output."""
    return StructType([
        StructField("folderName", StringType(), True),
        StructField("fileName", StringType(), True),
        StructField("filePath", StringType(), True),
        StructField("fileExtension", StringType(), True),
        StructField("modifiedOn", TimestampType(), True),
        StructField("namePeriod", LongType(), True),
        StructField("sheetNames", ArrayType(StringType()), True),
        StructField("columnNames", ArrayType(StringType()), True),
        StructField("rowCount", LongType(), True),
        StructField("detectedHeaderStart", LongType(), True),
    ])


def profile_files_metadata(spark, files_df: DataFrame, sample_rows: int = 20) -> List[Dict[str, Any]]:
    """Collect lightweight metadata (columns/sheets/period/header) per file."""
    if pd is None:
        raise RuntimeError("pandas/openpyxl not available; profiling requires these packages")

    results: List[Dict[str, Any]] = []
    for row in files_df.select("folderName", "fileName", "filePath", "fileExtension", "modifiedOn").toLocalIterator():
        ext = (row.fileExtension or "").lower().lstrip(".")
        columns: List[str] = []
        sheet_names: List[str] = []
        sample_error = None
        header_start = None
        period = extract_period(row.fileName)
        try:
            if ext in ("csv", "txt"):
                delimiter = "," if ext == "csv" else "\t"
                sample_pdf = (
                    spark.read
                         .format("csv")
                         .option("header", "false")
                         .option("inferSchema", "false")
                         .option("delimiter", delimiter)
                         .load(row.filePath)
                         .limit(sample_rows)
                         .toPandas()
                )
                header_start = detect_header_start(sample_pdf.values.tolist())
                if header_start is not None:
                    header_row = sample_pdf.iloc[header_start].tolist()
                    columns = [str(c).strip() if c not in (None, "") else f"col_{i}" for i, c in enumerate(header_row)]
                else:
                    columns = [str(c) for c in sample_pdf.columns]
            elif ext == "xlsx":
                bin_df = spark.read.format("binaryFile").load(row.filePath)
                if bin_df.rdd.isEmpty():
                    raise FileNotFoundError(f"File not found or unreadable: {row.filePath}")
                raw_bytes = bin_df.select("content").head()[0]
                from io import BytesIO
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    xls = pd.ExcelFile(BytesIO(raw_bytes))
                sheet_names = list(xls.sheet_names or [])
                chosen_sheet = sheet_names[0] if sheet_names else None
                if chosen_sheet is not None:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        pdf = xls.parse(chosen_sheet, header=None, nrows=sample_rows)
                    header_start = detect_header_start(pdf.values.tolist())
                    if header_start is not None:
                        header_row = pdf.iloc[header_start].tolist()
                        columns = [str(c).strip() if c not in (None, "") else f"col_{i}" for i, c in enumerate(header_row)]
                    else:
                        columns = [str(c) for c in pdf.columns]
        except Exception as e:
            sample_error = str(e)[:1000]
        results.append({
            "folderName": row.folderName,
            "fileName": row.fileName,
            "filePath": row.filePath,
            "fileExtension": row.fileExtension,
            "modifiedOn": row.modifiedOn,
            "namePeriod": period,
            "headerStart": header_start,
            "columns": columns,
            "sheetNames": sheet_names,
            "sampleError": sample_error,
            "scannedOn": datetime.utcnow(),
        })
    return results
