# Fabric ingestion utilities

Generic, reusable ingestion helpers designed for Fabric/Spark notebooks.
These helpers are storage-agnostic and can be used for SharePoint, ADLS, or
other sources as long as `list_files` can enumerate them.

## Quick usage

All public APIs expect `spark` as the first argument.

```python
from fabric_libs.ingestion import run_ingestion_parallel

result = run_ingestion_parallel(
    spark,
    root_path="Files/Your-Source-Data",
    source_name="Your-Source",
    control_table="config.file_ingestion_control_attributes",
    metadata_table="control.file_ingestion_metadata_log",
    supported_extensions=[".csv", ".txt", ".xlsx"],
    folders_to_run=["FolderA", "FolderB"],
    materialize=False,
    incremental=False,
    workers=2,
)
```

## Module map

- `utils.py`: sanitization, period parsing, UDFs, root path resolution.
- `io.py`: file listing + CSV/TXT/Excel readers and a dispatcher.
- `matching.py`: control-table join logic.
- `tracking.py`: incremental filtering based on metadata.
- `file_ingestion.py`: orchestration and parallel runner.
- `profiling.py`: lightweight profiling helpers.
- `reconcile.py`: reconciliation and metadata utilities.

## Backward compatibility

Functions that previously accepted `sharepoint_root` now accept `root_path`.
The legacy parameter still works, but `root_path` is preferred.
