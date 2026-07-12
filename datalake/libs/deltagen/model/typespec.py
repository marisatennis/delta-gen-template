"""Type specification and normalization for Delta-Gen v2.

This module provides a TypeSpec class for normalizing data type definitions
from various shorthand notations (varchar(255), money, int, etc.) into
consistent representations suitable for Spark SQL and standard SQL.
"""
from __future__ import annotations

import re
from typing import NamedTuple


class TypeSpec(NamedTuple):
    """Normalized data type specification.

    Attributes:
        base_type: The normalized base type name (e.g., "STRING", "DECIMAL", "INTEGER")
        precision: Precision for DECIMAL types or length for VARCHAR/CHAR types
        scale: Scale for DECIMAL types (number of digits after decimal point)
    """

    base_type: str
    precision: int | None = None
    scale: int | None = None

    def to_spark_sql(self) -> str:
        """Generate Spark SQL type representation.

        Spark SQL uses different type names than traditional SQL:
        - VARCHAR/CHAR → STRING
        - MONEY → DECIMAL(19,4)
        - INT → INTEGER (though INT is also accepted)

        Returns:
            Spark SQL type string (e.g., "STRING", "DECIMAL(18,2)", "INTEGER")
        """
        if self.base_type in ("STRING", "BINARY"):
            # Spark STRING has no length constraint
            return self.base_type
        elif self.base_type == "DECIMAL":
            if self.precision is not None and self.scale is not None:
                return f"DECIMAL({self.precision},{self.scale})"
            elif self.precision is not None:
                return f"DECIMAL({self.precision},0)"
            else:
                # Default DECIMAL(10,0) in Spark
                return "DECIMAL(10,0)"
        elif self.base_type in ("INTEGER", "BIGINT", "SMALLINT", "TINYINT"):
            return self.base_type
        elif self.base_type in ("DOUBLE", "FLOAT"):
            return self.base_type
        elif self.base_type in ("DATE", "TIMESTAMP"):
            return self.base_type
        elif self.base_type == "BOOLEAN":
            return "BOOLEAN"
        else:
            return self.base_type

    def to_standard_sql(self) -> str:
        """Generate standard SQL type representation.

        Standard SQL uses traditional type names:
        - STRING → VARCHAR (with optional length)
        - Preserves DECIMAL, INT, etc.

        Returns:
            Standard SQL type string (e.g., "VARCHAR(255)", "DECIMAL(18,2)", "INT")
        """
        if self.base_type == "STRING":
            if self.precision is not None:
                return f"VARCHAR({self.precision})"
            else:
                # VARCHAR without length (some DBs support this as VARCHAR(MAX))
                return "VARCHAR"
        elif self.base_type == "DECIMAL":
            if self.precision is not None and self.scale is not None:
                return f"DECIMAL({self.precision},{self.scale})"
            elif self.precision is not None:
                return f"DECIMAL({self.precision},0)"
            else:
                return "DECIMAL"
        elif self.base_type == "INTEGER":
            return "INT"
        elif self.base_type in ("BIGINT", "SMALLINT", "TINYINT"):
            return self.base_type
        elif self.base_type in ("DOUBLE", "FLOAT"):
            return self.base_type
        elif self.base_type in ("DATE", "TIMESTAMP", "DATETIME"):
            return self.base_type
        elif self.base_type == "BOOLEAN":
            return "BOOLEAN"
        elif self.base_type == "BINARY":
            if self.precision is not None:
                return f"VARBINARY({self.precision})"
            else:
                return "VARBINARY"
        else:
            return self.base_type


# Type alias mappings from common shorthand to normalized forms
TYPE_ALIASES = {
    # String types
    "string": ("STRING", None, None),
    "str": ("STRING", None, None),
    "text": ("STRING", None, None),
    # Integer types
    "int": ("INTEGER", None, None),
    "integer": ("INTEGER", None, None),
    "bigint": ("BIGINT", None, None),
    "long": ("BIGINT", None, None),
    "smallint": ("SMALLINT", None, None),
    "tinyint": ("TINYINT", None, None),
    "byte": ("TINYINT", None, None),
    # Floating point
    "double": ("DOUBLE", None, None),
    "float": ("FLOAT", None, None),
    "real": ("FLOAT", None, None),
    # Fixed point
    "decimal": ("DECIMAL", None, None),
    "numeric": ("DECIMAL", None, None),
    "number": ("DECIMAL", None, None),
    "money": ("DECIMAL", 19, 4),
    "smallmoney": ("DECIMAL", 10, 4),
    # Date/time types
    "date": ("DATE", None, None),
    "timestamp": ("TIMESTAMP", None, None),
    "datetime": ("TIMESTAMP", None, None),
    "datetime2": ("TIMESTAMP", None, None),
    # Boolean
    "boolean": ("BOOLEAN", None, None),
    "bool": ("BOOLEAN", None, None),
    "bit": ("BOOLEAN", None, None),
    # Binary
    "binary": ("BINARY", None, None),
    "varbinary": ("BINARY", None, None),
}


def parse_type(type_string: str) -> TypeSpec:
    """Parse a type string into a normalized TypeSpec.

    Supports various input formats:
    - Simple types: "int", "string", "date", "money"
    - Parameterized types: "varchar(255)", "decimal(18,2)", "char(50)"
    - Case-insensitive matching

    Args:
        type_string: Type definition string (e.g., "VARCHAR(255)", "money", "decimal(18,2)")

    Returns:
        TypeSpec instance with normalized type information

    Raises:
        ValueError: If the type string has invalid syntax or is unrecognized

    Examples:
        >>> parse_type("varchar(255)")
        TypeSpec(base_type='STRING', precision=255, scale=None)

        >>> parse_type("money")
        TypeSpec(base_type='DECIMAL', precision=19, scale=4)

        >>> parse_type("decimal(18,2)")
        TypeSpec(base_type='DECIMAL', precision=18, scale=2)

        >>> parse_type("int")
        TypeSpec(base_type='INTEGER', precision=None, scale=None)
    """
    if not type_string or not isinstance(type_string, str):
        raise ValueError(f"Type string must be a non-empty string, got: {type_string!r}")

    # Normalize whitespace and case
    type_string = type_string.strip().lower()

    # Pattern for parameterized types: typename(param1[,param2])
    # Note: ([^)]*) captures empty params too, we'll validate after
    param_pattern = r"^([a-z_][a-z0-9_]*)\s*\(([^)]*)\)$"
    match = re.match(param_pattern, type_string)

    if match:
        # Parameterized type like varchar(255) or decimal(18,2)
        base_name = match.group(1)
        params_str = match.group(2).strip()

        # Check for empty parameters
        if not params_str:
            if base_name in ("varchar", "char", "character"):
                raise ValueError(
                    f"VARCHAR/CHAR expects exactly 1 parameter (length), got: {base_name}()"
                )
            elif base_name in ("decimal", "numeric", "number"):
                raise ValueError(
                    f"DECIMAL expects 1 or 2 parameters (precision[,scale]), got: {base_name}()"
                )
            else:
                raise ValueError(f"Type {base_name} requires parameters but none provided")

        # Parse parameters
        params = [p.strip() for p in params_str.split(",")]

        if base_name in ("varchar", "char", "character"):
            # VARCHAR(n) or CHAR(n) → STRING with precision
            if len(params) != 1:
                raise ValueError(
                    f"VARCHAR/CHAR expects exactly 1 parameter (length), got: {params_str}"
                )
            try:
                length = int(params[0])
                if length <= 0:
                    raise ValueError(f"VARCHAR/CHAR length must be positive, got: {length}")
                return TypeSpec(base_type="STRING", precision=length, scale=None)
            except ValueError as e:
                if "invalid literal" in str(e):
                    raise ValueError(f"Invalid length parameter for VARCHAR/CHAR: {params[0]}")
                raise

        elif base_name in ("decimal", "numeric", "number"):
            # DECIMAL(p,s) or DECIMAL(p)
            if len(params) == 1:
                try:
                    precision = int(params[0])
                    if precision <= 0:
                        raise ValueError(f"DECIMAL precision must be positive, got: {precision}")
                    return TypeSpec(base_type="DECIMAL", precision=precision, scale=0)
                except ValueError as e:
                    if "invalid literal" in str(e):
                        raise ValueError(f"Invalid precision parameter for DECIMAL: {params[0]}")
                    raise
            elif len(params) == 2:
                try:
                    precision = int(params[0])
                    scale = int(params[1])
                    if precision <= 0:
                        raise ValueError(f"DECIMAL precision must be positive, got: {precision}")
                    if scale < 0:
                        raise ValueError(f"DECIMAL scale must be non-negative, got: {scale}")
                    if scale > precision:
                        raise ValueError(
                            f"DECIMAL scale ({scale}) cannot exceed precision ({precision})"
                        )
                    return TypeSpec(base_type="DECIMAL", precision=precision, scale=scale)
                except ValueError as e:
                    if "invalid literal" in str(e):
                        raise ValueError(f"Invalid DECIMAL parameters: {params_str}")
                    raise
            else:
                raise ValueError(
                    f"DECIMAL expects 1 or 2 parameters (precision[,scale]), got: {params_str}"
                )

        elif base_name in ("varbinary", "binary"):
            # VARBINARY(n) → BINARY with precision
            if len(params) != 1:
                raise ValueError(
                    f"VARBINARY/BINARY expects exactly 1 parameter (length), got: {params_str}"
                )
            try:
                length = int(params[0])
                if length <= 0:
                    raise ValueError(f"VARBINARY/BINARY length must be positive, got: {length}")
                return TypeSpec(base_type="BINARY", precision=length, scale=None)
            except ValueError as e:
                if "invalid literal" in str(e):
                    raise ValueError(f"Invalid length parameter for VARBINARY/BINARY: {params[0]}")
                raise

        else:
            # Unknown parameterized type
            raise ValueError(f"Unrecognized parameterized type: {base_name}({params_str})")

    else:
        # Simple type without parameters
        if type_string in TYPE_ALIASES:
            base_type, precision, scale = TYPE_ALIASES[type_string]
            return TypeSpec(base_type=base_type, precision=precision, scale=scale)
        else:
            # Try uppercase version as-is (could be a custom type)
            raise ValueError(
                f"Unrecognized type: {type_string!r}. "
                f"Known types: {', '.join(sorted(set(TYPE_ALIASES.keys())))}"
            )
