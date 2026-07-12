"""Schema management utilities for Fabric Lakehouses.

This module provides utilities to list, create, and manage schemas in Fabric Lakehouses.
"""

from pyspark.sql import SparkSession


def list_schemas(spark: SparkSession, exclude_system=True):
    """
    List all schemas/databases in the current lakehouse.

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    exclude_system : bool
        If True, excludes system schemas (default, information_schema)

    Returns:
    --------
    list
        List of schema names

    Example:
    --------
    >>> schemas = list_schemas(spark)
    >>> print(f"Found {len(schemas)} schemas: {', '.join(schemas)}")
    """
    schemas_df = spark.sql("SHOW SCHEMAS")
    schemas = [row.namespace for row in schemas_df.collect()]

    if exclude_system:
        system_schemas = ['default', 'information_schema']
        schemas = [s for s in schemas if s not in system_schemas]

    return schemas


def get_schema_details(spark: SparkSession, schema_name=None):
    """
    Get detailed information about schemas including table counts.

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    schema_name : str, optional
        Specific schema to get details for. If None, gets all schemas.

    Returns:
    --------
    list of dict
        List of dictionaries with schema information

    Example:
    --------
    >>> details = get_schema_details(spark)
    >>> for schema in details:
    ...     print(f"{schema['name']}: {schema['table_count']} tables")
    """
    if schema_name:
        schemas = [schema_name]
    else:
        schemas = list_schemas(spark, exclude_system=True)

    schema_info = []

    for schema in schemas:
        try:
            tables_df = spark.sql(f"SHOW TABLES IN {schema}")
            table_count = tables_df.count()
            tables = [row.tableName for row in tables_df.collect()]

            schema_info.append({
                'name': schema,
                'table_count': table_count,
                'tables': tables
            })
        except Exception as e:
            schema_info.append({
                'name': schema,
                'table_count': 0,
                'tables': [],
                'error': str(e)
            })

    return schema_info


def create_schema(spark: SparkSession, schema_name, comment=None, if_not_exists=True):
    """
    Create a new schema in the lakehouse.

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    schema_name : str
        Name of the schema to create
    comment : str, optional
        Description/comment for the schema
    if_not_exists : bool
        If True, only creates if schema doesn't exist (default: True)

    Returns:
    --------
    bool
        True if schema was created, False if it already existed

    Example:
    --------
    >>> create_schema(spark, 'bronze_salesforce', 'Salesforce raw data')
    >>> create_schema(spark, 'silver_customers', 'Cleaned customer data')
    """
    exists_clause = "IF NOT EXISTS" if if_not_exists else ""
    comment_clause = f"COMMENT '{comment}'" if comment else ""

    sql = f"CREATE SCHEMA {exists_clause} {schema_name} {comment_clause}"

    try:
        spark.sql(sql)
        print(f"Schema '{schema_name}' created successfully")
        return True
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"Schema '{schema_name}' already exists")
            return False
        else:
            print(f"Error creating schema '{schema_name}': {e}")
            raise


def drop_schema(spark: SparkSession, schema_name, cascade=False, if_exists=True):
    """
    Drop a schema from the lakehouse.

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    schema_name : str
        Name of the schema to drop
    cascade : bool
        If True, drops all tables in the schema (default: False)
    if_exists : bool
        If True, doesn't error if schema doesn't exist (default: True)

    Example:
    --------
    >>> drop_schema(spark, 'test_schema', cascade=True)
    """
    exists_clause = "IF EXISTS" if if_exists else ""
    cascade_clause = "CASCADE" if cascade else ""

    sql = f"DROP SCHEMA {exists_clause} {schema_name} {cascade_clause}"

    try:
        spark.sql(sql)
        print(f"Schema '{schema_name}' dropped successfully")
        return True
    except Exception as e:
        print(f"Error dropping schema '{schema_name}': {e}")
        raise


def print_schema_summary(spark: SparkSession):
    """
    Print a formatted summary of all schemas and their tables.

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session

    Example:
    --------
    >>> print_schema_summary(spark)
    """
    details = get_schema_details(spark)

    print("\n" + "="*80)
    print("LAKEHOUSE SCHEMA SUMMARY")
    print("="*80)
    print(f"\nTotal Schemas: {len(details)}\n")

    for schema in details:
        print(f"  {schema['name']}")
        print(f"   Tables: {schema['table_count']}")

        if 'error' in schema:
            print(f"   Error: {schema['error']}")
        elif schema['tables']:
            print(f"   > {', '.join(schema['tables'][:5])}")
            if len(schema['tables']) > 5:
                print(f"      ... and {len(schema['tables']) - 5} more")
        else:
            print(f"   > (empty)")
        print()

    print("="*80 + "\n")


def get_table_schema_info(spark: SparkSession, table_name):
    """
    Get detailed schema information for a specific table.

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    table_name : str
        Fully qualified table name (schema.table) or just table name

    Returns:
    --------
    list of dict
        List of column information dictionaries

    Example:
    --------
    >>> columns = get_table_schema_info(spark, 'bronze_salesforce.accounts')
    >>> for col in columns:
    ...     print(f"{col['name']} ({col['type']})")
    """
    try:
        describe_df = spark.sql(f"DESCRIBE TABLE {table_name}")

        columns = []
        for row in describe_df.collect():
            # Skip partition information and other metadata
            if row.col_name.startswith('#') or row.col_name == '':
                break

            columns.append({
                'name': row.col_name,
                'type': row.data_type,
                'comment': row.comment if hasattr(row, 'comment') else None
            })

        return columns
    except Exception as e:
        print(f"Error getting table schema for '{table_name}': {e}")
        raise
