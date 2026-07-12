# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000000",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "00000000-0000-0000-0000-000000000000"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Manual Metadata Ingestion
#
# Ingest CSV files manually uploaded to `Files/sources/Manual-Uploads/` in the bronze
# lakehouse. Each file is loaded into a Delta table with five metadata columns:
# `_source`, `_source_file`, `_source_folder`, `_source_modified`, `_source_period`.
#
# **Usage**: add one `load_csv_with_metadata(...)` cell per file you want to ingest.

# CELL ********************

from pyspark.sql.functions import lit

SOURCE_FOLDER = "Manual-Uploads"
INGESTION_PATH = f"Files/sources/{SOURCE_FOLDER}"

def load_csv_with_metadata(file_name, table_name, source, source_modified, source_period):
    """Read CSV from Manual-Uploads, add metadata columns, write as Delta table."""
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(f"{INGESTION_PATH}/{file_name}"))

    for col_name, value in [
        ("_source",          source),
        ("_source_file",     file_name),
        ("_source_folder",   SOURCE_FOLDER),
        ("_source_modified", source_modified),
        ("_source_period",   source_period),
    ]:
        if col_name in df.columns:
            df = df.drop(col_name)
        df = df.withColumn(col_name, lit(value))

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"✓ {table_name}  ({df.count()} rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example: replace with your own file(s).
#
# load_csv_with_metadata(
#     file_name       = "customers.csv",
#     table_name      = "manual.customers",
#     source          = "customers",
#     source_modified = "2026-01-01",
#     source_period   = "260101",
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
