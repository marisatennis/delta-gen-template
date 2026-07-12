"""Control-table matching and attribute enrichment for files."""

import re
import threading
import uuid

from pyspark.sql import functions as F

from .utils import extract_period_udf, sanitize_name_udf


def get_file_match_join_conditions(files_alias: str = "f", control_alias: str = "c") -> str:
    """SQL join predicate for matching files to control records."""
    f = files_alias
    c = control_alias
    return f"""(
    (
      {c}.ctrl_folderName IS NULL
      OR trim({c}.ctrl_folderName) = ''
      OR (
         (instr({c}.ctrl_folderName, '%') > 0 AND lower({f}.folderName) LIKE lower(trim({c}.ctrl_folderName)))
         OR (instr({c}.ctrl_folderName, '%') = 0 AND {f}.folderSan = {c}.ctrl_folderSan)
      )
    )
    AND ({f}._fileExtNorm = lower(regexp_replace(trim({c}.ctrl_extension), '^[.]', '')))
    AND (
      ({c}.ctrl_timeRangeMin IS NULL AND {c}.ctrl_timeRangeMax IS NULL)
      OR ({c}.ctrl_timeRangeMin IS NOT NULL AND {c}.ctrl_timeRangeMax IS NULL AND {f}._filePeriod >= {c}.ctrl_timeRangeMin)
      OR ({c}.ctrl_timeRangeMin IS NULL AND {c}.ctrl_timeRangeMax IS NOT NULL AND {f}._filePeriod <= {c}.ctrl_timeRangeMax)
      OR ({c}.ctrl_timeRangeMin IS NOT NULL AND {c}.ctrl_timeRangeMax IS NOT NULL AND {f}._filePeriod BETWEEN {c}.ctrl_timeRangeMin AND {c}.ctrl_timeRangeMax)
    )
    AND ({c}.ctrl_fileNameWildcard IS NULL OR trim({c}.ctrl_fileNameWildcard) = '' OR lower({f}.fileName) LIKE concat('%', lower(trim({c}.ctrl_fileNameWildcard)), '%'))
    AND (
      {c}.ctrl_excludeFile IS NULL
      OR trim({c}.ctrl_excludeFile) = ''
      OR NOT exists(
          transform(split({c}.ctrl_excludeFile, ','), x -> trim(lower(x))),
          pattern -> instr(lower({f}.fileName), pattern) > 0
      )
    )
  )"""


def get_file_match_condition_flags(files_alias: str = "f", control_alias: str = "c") -> str:
    """Return SQL booleans for debugging match conditions."""
    f = files_alias
    c = control_alias
    return f"""
  (
    ({c}.ctrl_folderName IS NULL OR trim({c}.ctrl_folderName) = '')
    OR (instr({c}.ctrl_folderName, '%') > 0 AND lower({f}.folderName) LIKE lower(trim({c}.ctrl_folderName)))
    OR (instr({c}.ctrl_folderName, '%') = 0 AND ({f}.folderSan = {c}.ctrl_folderSan OR instr(coalesce({f}.folderSan,''), coalesce({c}.ctrl_folderSan,'')) > 0))
  ) AS cond_folder,
  ({f}._fileExtNorm = lower(regexp_replace(trim({c}.ctrl_extension), '^[.]', ''))) AS cond_extension,
  (
    ({c}.ctrl_timeRangeMin IS NULL AND {c}.ctrl_timeRangeMax IS NULL)
    OR ({c}.ctrl_timeRangeMin IS NOT NULL AND {c}.ctrl_timeRangeMax IS NULL AND {f}._filePeriod >= {c}.ctrl_timeRangeMin)
    OR ({c}.ctrl_timeRangeMin IS NULL AND {c}.ctrl_timeRangeMax IS NOT NULL AND {f}._filePeriod <= {c}.ctrl_timeRangeMax)
    OR ({c}.ctrl_timeRangeMin IS NOT NULL AND {c}.ctrl_timeRangeMax IS NOT NULL AND {f}._filePeriod BETWEEN {c}.ctrl_timeRangeMin AND {c}.ctrl_timeRangeMax)
  ) AS cond_period,
  ({c}.ctrl_fileNameWildcard IS NULL OR trim({c}.ctrl_fileNameWildcard) = '' OR lower({f}.fileName) LIKE concat('%', lower(trim({c}.ctrl_fileNameWildcard)), '%')) AS cond_wildcard,
  ({c}.ctrl_excludeFile IS NULL OR trim({c}.ctrl_excludeFile) = '' OR NOT exists(transform(split({c}.ctrl_excludeFile, ','), x -> trim(lower(x))), pattern -> instr(lower({f}.fileName), pattern) > 0)) AS cond_exclude"""


def add_control_attributes(spark, files_df, control_df, source_name: str):
    """Join file listings to control rules and attach ingestion attributes.

    Args:
        spark: SparkSession
        files_df: DataFrame with file listings
        control_df: DataFrame with control rules
        source_name: Source name to filter control rules (e.g., 'Source-Monthly')
    """
    if control_df is None or control_df.rdd.isEmpty():
        return files_df

    files = (
        files_df
        .withColumn("folderSan", sanitize_name_udf(F.col("folderName")))
        .withColumn("_fileExtNorm", F.lower(F.col("fileExtension")))
        .withColumn("_filePeriod", F.coalesce(
            extract_period_udf(F.col("fileName")),
            ((F.year(F.col("modifiedOn")) % 100) * 10000) + (F.month(F.col("modifiedOn")) * 100) + F.lit(1),
        ))
    )

    # Filter control rules by source
    filtered_control = control_df
    if "source" in control_df.columns:
        filtered_control = control_df.filter(F.trim(F.col("source")) == source_name)

    # Build control columns list - handle optional processingMode column
    control_cols = [
        F.trim(F.col("folderName").cast("string")).alias("ctrl_folderName"),
        F.trim(F.col("extension").cast("string")).alias("ctrl_extension"),
        F.col("timeRangeMin").cast("long").alias("ctrl_timeRangeMin"),
        F.col("timeRangeMax").cast("long").alias("ctrl_timeRangeMax"),
        F.col("headerStart").cast("long").alias("ctrl_headerStart"),
        F.trim(F.col("sheetName").cast("string")).alias("ctrl_sheetName"),
        F.trim(F.col("sheetMatchType").cast("string")).alias("ctrl_sheetMatchType"),
        F.trim(F.col("headerMarker").cast("string")).alias("ctrl_headerMarker"),
        F.trim(F.col("fileNameWildcard").cast("string")).alias("ctrl_fileNameWildcard"),
        F.trim(F.col("excludeFile").cast("string")).alias("ctrl_excludeFile"),
        F.trim(F.col("delimiter").cast("string")).alias("ctrl_delimiter"),
        F.trim(F.col("targetTableName").cast("string")).alias("ctrl_targetTableName"),
        F.trim(F.col("columnNormalize").cast("string")).alias("ctrl_columnNormalize"),
    ]
    # Add multiLine if it exists in the control table
    if "multiLine" in filtered_control.columns:
        control_cols.append(F.trim(F.col("multiLine").cast("string")).alias("ctrl_multiLine"))
    # Add processingMode if it exists in the control table
    if "processingMode" in filtered_control.columns:
        control_cols.append(F.trim(F.col("processingMode").cast("string")).alias("ctrl_processingMode"))

    control = filtered_control.select(*control_cols).withColumn("ctrl_folderSan", sanitize_name_udf(F.col("ctrl_folderName")))

    view_id = f"{threading.current_thread().name}_{uuid.uuid4().hex}"
    view_id = re.sub(r"[^0-9a-zA-Z_]", "_", view_id)
    files_view = f"files_norm_{view_id}"
    control_view = f"control_norm_{view_id}"

    files.createOrReplaceTempView(files_view)
    control.createOrReplaceTempView(control_view)

    join_conditions = get_file_match_join_conditions("f", "c")

    # Build query - include optional columns if available
    has_multi_line = "multiLine" in filtered_control.columns
    has_processing_mode = "processingMode" in filtered_control.columns
    multi_line_col = ", c.ctrl_multiLine" if has_multi_line else ""
    processing_mode_col = ", c.ctrl_processingMode" if has_processing_mode else ""

    query = f"""
    SELECT /*+ BROADCAST(c) */ DISTINCT
           f.filePath, f.fileName, f.folderName, f.folderSan AS folderSan, f.folderSan AS sanitizedFolderName,
           f.fileExtension, f.modifiedOn, f._filePeriod,
           c.ctrl_delimiter,
           c.ctrl_headerStart,
           c.ctrl_sheetName,
           c.ctrl_sheetMatchType,
           c.ctrl_headerMarker,
           c.ctrl_targetTableName,
           c.ctrl_columnNormalize,
           c.ctrl_excludeFile{multi_line_col}{processing_mode_col}
    FROM `{files_view}` f
    JOIN `{control_view}` c
      ON {join_conditions}
    """

    matched = spark.sql(query)

    # Build output columns
    output_cols = [
        F.col("filePath"),
        F.col("fileName"),
        F.col("folderName"),
        F.col("sanitizedFolderName"),
        F.col("folderSan"),
        F.col("fileExtension"),
        F.col("modifiedOn"),
        F.col("_filePeriod").alias("filePeriod"),
        F.col("ctrl_delimiter").alias("delimiter"),
        F.col("ctrl_headerStart").alias("headerStart"),
        F.col("ctrl_sheetName").alias("sheetName"),
        F.col("ctrl_sheetMatchType").alias("sheetMatchType"),
        F.col("ctrl_headerMarker").alias("headerMarker"),
        F.col("ctrl_targetTableName").alias("targetTableName"),
        F.col("ctrl_columnNormalize").alias("columnNormalize"),
    ]
    if has_multi_line:
        output_cols.append(F.col("ctrl_multiLine").alias("multiLine"))
    if has_processing_mode:
        output_cols.append(F.col("ctrl_processingMode").alias("processingMode"))

    return matched.select(*output_cols).distinct()
