"""Orchestration tracking and logging utilities."""

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, IntegerType


def get_orchestration_start_time():
    return datetime.utcnow()


def log_orchestration_start(orchestration_id, orchestration_name, execution_mode, incremental=True):
    start_time = get_orchestration_start_time()
    print(f"\n{'='*100}")
    print(f"{orchestration_name.upper()}")
    print(f"{'='*100}")
    print(f"Orchestration ID: {orchestration_id}")
    print(f"Start Time: {start_time}")
    print(f"Execution Mode: {execution_mode}")
    print(f"Incremental: {incremental}")
    print(f"{'='*100}\n")
    return start_time


def log_orchestration_end(orchestration_id, start_time, results):
    end_time = datetime.utcnow()
    total_duration = (end_time - start_time).total_seconds()

    successful = [r for r in results if r['status'] == 'SUCCESS']
    failed = [r for r in results if r['status'] == 'FAILED']
    skipped = [r for r in results if r['status'] == 'SKIPPED']

    summary = {
        'orchestration_id': orchestration_id,
        'start_time': start_time,
        'end_time': end_time,
        'total_duration_seconds': total_duration,
        'total_notebooks': len(results),
        'successful': len(successful),
        'failed': len(failed),
        'skipped': len(skipped)
    }

    print(f"\n{'='*100}")
    print("ORCHESTRATION COMPLETE")
    print(f"{'='*100}")
    print(f"Orchestration ID: {orchestration_id}")
    print(f"Total Duration: {total_duration:.2f}s ({total_duration/60:.2f} minutes)")
    print(f"Total Notebooks: {len(results)}")
    print(f"  - Successful: {len(successful)}")
    print(f"  - Failed: {len(failed)}")
    print(f"  - Skipped: {len(skipped)}")
    print(f"{'='*100}\n")

    return summary


def persist_run_results(spark, results, orchestration_id, table_name="control.notebook_run_metrics"):
    if not results:
        return

    schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("table_name", StringType(), True),
        StructField("load_id", StringType(), True),
        StructField("environment", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("total_rows_read", IntegerType(), True),
        StructField("total_rows_written", IntegerType(), True),
        StructField("total_rows_rejected", IntegerType(), True),
        StructField("source_count", IntegerType(), True),
        StructField("stage_count", IntegerType(), True),
        StructField("quality_issue_count", IntegerType(), True),
        StructField("schema_change_count", IntegerType(), True),
        StructField("partition_year", IntegerType(), True),
        StructField("partition_month", IntegerType(), True),
        StructField("partition_day", IntegerType(), True),
    ])

    rows = []
    for r in results:
        start = r.get("start_time")
        end = r.get("end_time")
        duration_s = r.get("duration_seconds", 0) or 0
        status_raw = r.get("status", "UNKNOWN")
        status_map = {"SUCCESS": "completed", "FAILED": "failed", "SKIPPED": "skipped"}
        status = status_map.get(status_raw, status_raw.lower())

        rows.append({
            "run_id": orchestration_id,
            "table_name": r.get("notebook_name", "unknown"),
            "load_id": orchestration_id,
            "environment": None,
            "start_time": start,
            "end_time": end,
            "duration_ms": int(duration_s * 1000),
            "status": status,
            "error_message": r.get("error"),
            "total_rows_read": None,
            "total_rows_written": None,
            "total_rows_rejected": None,
            "source_count": None,
            "stage_count": None,
            "quality_issue_count": None,
            "schema_change_count": None,
            "partition_year": start.year if start else None,
            "partition_month": start.month if start else None,
            "partition_day": start.day if start else None,
        })

    df = spark.createDataFrame(rows, schema)
    df.write.format("delta").mode("append").saveAsTable(table_name)
    print(f"[persist_run_results] Written {len(rows)} rows to {table_name}")
