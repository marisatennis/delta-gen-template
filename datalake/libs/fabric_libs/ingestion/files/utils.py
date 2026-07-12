"""Utility helpers for ingestion (sanitization, period parsing, UDFs)."""

from datetime import datetime
import re
from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType

SUPPORTED_EXTENSIONS = [".csv", ".txt", ".xlsx", ".xls"]



def sanitize_name(name: Optional[str]) -> Optional[str]:
    """Normalize a name into a safe, lowercased snake-like token."""
    if name is None:
        return None
    s = name.strip().lower()
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or None


def extract_period(file_name: Optional[str]) -> Optional[int]:
    """Extract a YYMMDD-style period token from a filename."""
    if not file_name:
        return None

    month_map = {
        "january": 1, "jan": 1,
        "february": 2, "feb": 2,
        "march": 3, "mar": 3,
        "april": 4, "apr": 4,
        "may": 5,
        "june": 6, "jun": 6,
        "july": 7, "jul": 7,
        "august": 8, "aug": 8,
        "september": 9, "sep": 9, "sept": 9,
        "october": 10, "oct": 10,
        "november": 11, "nov": 11,
        "december": 12, "dec": 12,
    }

    m8 = re.search(r"(?<!\d)(\d{8})(?!\d)", file_name)
    if m8:
        try:
            return int(datetime.strptime(m8.group(1), "%Y%m%d").strftime("%y%m%d"))
        except Exception:
            pass

    m6 = re.search(r"\b(\d{6})\b", file_name)
    if m6:
        try:
            return int(datetime.strptime(m6.group(1), "%y%m%d").strftime("%y%m%d"))
        except Exception:
            pass

    month_year_pattern = r"\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)[\s\-_]*(\d{4})\b"
    m_month_year = re.search(month_year_pattern, file_name, re.IGNORECASE)
    if m_month_year:
        month_str = m_month_year.group(1).lower()
        year = int(m_month_year.group(2))
        month = month_map.get(month_str)
        if month and 2000 <= year <= 2099:
            yy = year % 100
            return yy * 10000 + month * 100 + 1

    year_month_pattern = r"\b(\d{4})[\s\-_]*(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\b"
    m_year_month = re.search(year_month_pattern, file_name, re.IGNORECASE)
    if m_year_month:
        year = int(m_year_month.group(1))
        month_str = m_year_month.group(2).lower()
        month = month_map.get(month_str)
        if month and 2000 <= year <= 2099:
            yy = year % 100
            return yy * 10000 + month * 100 + 1

    m4 = re.search(r"\b(\d{4})\b", file_name)
    if m4:
        token = m4.group(1)
        try:
            mm = int(token[-2:])
            if 1 <= mm <= 12:
                return int(token) * 100 + 1
        except Exception:
            pass

    return None


def check_exclude_patterns(file_name: Optional[str], exclude_patterns: Optional[str]) -> bool:
    """Return True if file_name contains any comma-separated exclude pattern."""
    if not file_name or not exclude_patterns:
        return False
    file_lower = file_name.lower()
    patterns = [p.strip().lower() for p in exclude_patterns.split(",") if p.strip()]
    for pattern in patterns:
        if pattern in file_lower:
            return True
    return False


# Spark UDF wrappers
sanitize_name_udf = F.udf(sanitize_name, StringType())
extract_period_udf = F.udf(extract_period, LongType())
check_exclude_patterns_udf = F.udf(check_exclude_patterns, StringType())


def strip_date_tokens(name: str) -> str:
    """Normalize filenames for mapping by stripping date-like tokens."""
    if not name:
        return name
    s = name
    s = re.sub(r"(^|_)\d{8}(?=_|$)", "_", s)
    s = re.sub(r"(^|_)\d{6}(?=_|$)", "_", s)
    s = re.sub(r"(^|_)\d{4}(?:_\d{2}){1,2}(?=_|$)", "_", s)
    s = re.sub(r"(^|_)\d{2}(?:_\d{2}){1,2}(?=_|$)", "_", s)
    s = re.sub(r"(^|_)\d{4}(?=_|$)", "_", s)
    s = re.sub(r"_(csv|xlsx|txt)(?=_|$)", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    s = re.sub(r"^sp_", "", s)
    return s
