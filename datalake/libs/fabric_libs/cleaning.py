"""Generic data cleaning plugins for Delta-Gen.

Reusable transformations for common data quality issues like decimal parsing,
mixed date formats, and UK postcode standardization. These plugins extend
Delta-Gen's core functionality with commonly-needed cleaning rules.

Usage:
    Import this module in notebooks or __init__.py to register all plugins.
    Reference plugins in YAML configs via extensions.transform property.
"""
from deltagen.plugins.registry import register_column
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType

try:
    from deltagen.plugins.context import PluginContext
    from deltagen.core.config import ColumnConfig
except ImportError:
    PluginContext = None
    ColumnConfig = None


def _get_input_column_name(column: "ColumnConfig") -> str:
    """Extract the input column name from column config."""
    if column.inputs and len(column.inputs) > 0:
        first_input = column.inputs[0]
        if hasattr(first_input, 'column') and first_input.column:
            return first_input.column
    return column.name


@register_column("clean_decimal")
def clean_decimal(df: "DataFrame", column: "ColumnConfig", ctx: "PluginContext") -> "DataFrame":
    """Strip non-numeric formatting from string columns and cast to decimal.

    Removes commas, currency symbols, spaces, and any other characters that
    are not digits, decimal points, or minus signs, then casts to DecimalType.

    Optional YAML extensions:
        precision: Total number of digits (default 38)
        scale:     Digits after the decimal point (default 2)

    Examples:
        "1,234,567.89"  -> 1234567.89
        "£1,234.56"     -> 1234.56
        "-3,000.00"     -> -3000.00

    Usage in YAML:
        - name: amount
          data_type: decimal
          nullable: false
          inputs:
            - source: src
              column: value
          extensions:
            transform: clean_decimal
            precision: 18
            scale: 2
    """
    input_col_name = _get_input_column_name(column)
    output_col_name = column.name
    extensions = column.extensions or {}
    precision = int(extensions.get("precision", 38))
    scale = int(extensions.get("scale", 2))

    col = F.col(output_col_name)

    cleaned = F.regexp_replace(col, r'[^0-9.\-]', '')
    cleaned = F.when(cleaned == '', None).otherwise(cleaned)
    cleaned = cleaned.cast(DecimalType(precision, scale))

    ctx.log_info(f"Cleaning decimal: {input_col_name} -> {output_col_name} (precision={precision}, scale={scale})")

    return df.withColumn(output_col_name, cleaned)


@register_column("parse_mixed_date")
def parse_mixed_date(df: "DataFrame", column: "ColumnConfig", ctx: "PluginContext") -> "DataFrame":
    """Parse date columns that contain a mix of formats into a consistent date.

    Detects and handles common formats:
    1. Excel serial numbers ("46024" or "45903.4723")
    2. ISO date only ("yyyy-MM-dd")
    3. ISO timestamps with optional fractional seconds
    4. UK date only ("dd/MM/yyyy")
    5. UK datetime ("dd/MM/yyyy HH:mm:ss")
    6. UK short date with 2-digit year ("dd/MM/yy")
    7. Slash ISO date ("yyyy/MM/dd")

    Optional YAML extensions:
        excel_epoch: Base date for Excel serial conversion (default '1899-12-30')

    Usage in YAML:
        - name: trans_date
          data_type: date
          nullable: true
          inputs:
            - source: src
              column: trans_date
          extensions:
            transform: parse_mixed_date
    """
    input_col_name = _get_input_column_name(column)
    output_col_name = column.name
    extensions = column.extensions or {}
    excel_epoch = extensions.get("excel_epoch", "1899-12-30")

    col = F.col(output_col_name)

    parsed = (
        F.when(
            col.rlike(r'^[0-9]+(\.[0-9]+)?$'),
            F.expr(f"DATE_ADD('{excel_epoch}', CAST(CAST({output_col_name} AS DOUBLE) AS INT))")
        )
        .when(
            col.rlike(r'^\d{4}-\d{2}-\d{2}$'),
            F.to_date(col, 'yyyy-MM-dd')
        )
        .when(
            col.like('%-%-%'),
            F.to_date(F.regexp_replace(col, r'\.\d+$', ''), 'yyyy-MM-dd HH:mm:ss')
        )
        .when(
            col.like('%/%/% %:%:%'),
            F.to_date(col, 'dd/MM/yyyy HH:mm:ss')
        )
        .when(
            col.rlike(r'^\d{2}/\d{2}/\d{4}$'),
            F.to_date(col, 'dd/MM/yyyy')
        )
        .when(
            col.rlike(r'^\d{2}/\d{2}/\d{2}$'),
            F.to_date(
                F.concat(F.substring(col, 1, 6), F.lit('20'), F.substring(col, 7, 2)),
                'dd/MM/yyyy'
            )
        )
        .when(
            col.rlike(r'^\d{4}/\d{2}/\d{2}$'),
            F.to_date(col, 'yyyy/MM/dd')
        )
    )

    ctx.log_info(f"Parsing mixed date: {input_col_name} -> {output_col_name}")

    return df.withColumn(output_col_name, parsed)


@register_column("clean_uk_postcode")
def clean_uk_postcode(df: "DataFrame", column: "ColumnConfig", ctx: "PluginContext") -> "DataFrame":
    """UK postcode standardization.

    Cleaning steps:
    1. Uppercase
    2. Ensure space before last 3 characters (UK standard format)
    3. Trim whitespace

    Examples:
        "ab12cd"      -> "AB1 2CD"
        "SW1A1AA"     -> "SW1A 1AA"
        "  m1  1ae  " -> "M1 1AE"

    Usage in YAML:
        - name: postcode_clean
          data_type: string
          nullable: true
          inputs:
            - column: postcode
          extensions:
            transform: clean_uk_postcode
    """
    input_col_name = _get_input_column_name(column)
    output_col_name = column.name
    col = F.col(input_col_name)

    cleaned = F.trim(F.upper(col))
    cleaned = F.regexp_replace(
        cleaned,
        r'^([A-Z]{1,2}\d{1,2}[A-Z]?)\s?(\d[A-Z]{2})$',
        r'$1 $2'
    )

    ctx.log_info(f"Cleaning postcode: {input_col_name} -> {output_col_name}")

    return df.withColumn(output_col_name, cleaned)


@register_column("clean_contact_name")
def clean_contact_name(df: "DataFrame", column: "ColumnConfig", ctx: "PluginContext") -> "DataFrame":
    """Contact name standardization.

    Cleaning steps:
    1. Handle "Last, First" format -> "First Last"
    2. Handle "Firm - Person" format -> "Person"
    3. Remove title prefixes (Mr, Mrs, Dr, Prof, etc.)
    4. Trim and collapse whitespace

    Usage in YAML:
        - name: contact_clean
          data_type: string
          nullable: true
          inputs:
            - column: contact
          extensions:
            transform: clean_contact_name
    """
    input_col_name = _get_input_column_name(column)
    output_col_name = column.name
    col = F.col(input_col_name)

    has_comma = col.contains(',')
    parts = F.split(col, ',')
    reversed_name = F.concat(
        F.trim(F.element_at(parts, 2)),
        F.lit(' '),
        F.trim(F.element_at(parts, 1))
    )

    has_dash = col.contains(' - ')
    dash_parts = F.split(col, ' - ')
    person_only = F.trim(F.element_at(dash_parts, -1))

    cleaned = F.when(has_comma, reversed_name) \
               .when(has_dash, person_only) \
               .otherwise(F.trim(col))

    title_pattern = r'^(Mr|Mrs|Miss|Ms|Dr|Prof|Sir|Lady|Lord)\.?\s+'
    cleaned = F.regexp_replace(cleaned, title_pattern, '')
    cleaned = F.regexp_replace(cleaned, r'\s+', ' ')

    ctx.log_info(f"Cleaning contact name: {input_col_name} -> {output_col_name}")

    return df.withColumn(output_col_name, cleaned)


@register_column("clean_company_name")
def clean_company_name(df: "DataFrame", column: "ColumnConfig", ctx: "PluginContext") -> "DataFrame":
    """Company/firm name standardization.

    Cleaning steps:
    1. Uppercase for consistency
    2. Trim and collapse multiple spaces
    3. Remove legal entity suffixes (LTD, LIMITED, LLP, PLC, INC)
    4. Standardize "&" vs "AND"

    Usage in YAML:
        - name: company_clean
          data_type: string
          nullable: true
          inputs:
            - column: company_name
          extensions:
            transform: clean_company_name
    """
    input_col_name = _get_input_column_name(column)
    output_col_name = column.name
    col = F.col(input_col_name)

    cleaned = F.trim(F.upper(col))
    cleaned = F.regexp_replace(cleaned, r'\s+', ' ')

    remove_legal_suffix = column.extensions.get('remove_legal_suffix', True) if column.extensions else True
    if remove_legal_suffix:
        suffixes = ['LIMITED', 'LTD', 'LLP', 'PLC', 'INC']
        for suffix in suffixes:
            cleaned = F.regexp_replace(cleaned, rf'\s+{suffix}\.?$', '')

    cleaned = F.regexp_replace(cleaned, r'\s+&\s+', ' AND ')

    ctx.log_info(f"Cleaning company name: {input_col_name} -> {output_col_name}")

    return df.withColumn(output_col_name, cleaned)
