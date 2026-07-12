"""SharePoint list storage helpers."""


def write_dataframe_to_lakehouse(dataframe, target_table, mode="append", merge_schema=True):
    """
    Writes a Spark DataFrame to a Lakehouse Delta table.

    Parameters:
    -----------
    dataframe : DataFrame
        Spark DataFrame to write
    target_table : str
        Full target table name (e.g., 'bronze.sharepoint.my_list')
    mode : str
        Write mode (append, overwrite)
    merge_schema : bool
        Whether to merge schema on write
    """
    writer = dataframe.write.format("delta").mode(mode)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    writer.saveAsTable(target_table)
    print(f"Wrote {target_table} with mode={mode}, mergeSchema={merge_schema}")
