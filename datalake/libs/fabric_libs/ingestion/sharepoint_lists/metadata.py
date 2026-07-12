"""SharePoint list delta metadata tracking utilities."""

import pyspark.sql.functions as F


def ensure_delta_table(spark, table_name):
    """
    Creates a delta metadata table for SharePoint list ingestion if it doesn't exist.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            site_url STRING,
            list_name STRING,
            delta_link STRING,
            updated_at TIMESTAMP
        )
        USING DELTA
    """)


def update_delta_link(spark, site_url, list_name, delta_link, table_name, updated_at):
    """
    Appends a delta link checkpoint entry.
    """
    df = spark.createDataFrame(
        [(site_url, list_name, delta_link, updated_at)],
        ["site_url", "list_name", "delta_link", "updated_at"],
    )
    df.write.format("delta").mode("append").saveAsTable(table_name)


def get_latest_delta_link(spark, site_url, list_name, table_name):
    """
    Retrieves the latest delta link for a given list.
    """
    df = spark.table(table_name)
    latest = (
        df.filter((df.site_url == site_url) & (df.list_name == list_name))
        .agg(F.max("updated_at").alias("updated_at"))
        .collect()
    )
    if not latest or latest[0]["updated_at"] is None:
        return None

    updated_at = latest[0]["updated_at"]
    link_df = (
        df.filter((df.site_url == site_url) & (df.list_name == list_name))
        .filter(df.updated_at == updated_at)
        .select("delta_link")
        .limit(1)
        .collect()
    )
    if link_df:
        return link_df[0]["delta_link"]
    return None
