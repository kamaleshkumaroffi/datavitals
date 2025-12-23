"""
datavitals.sql_builder

Provides a safe, reusable, and dynamic SQL query builder
to avoid manual query writing errors.

Author: Kamaleshkumar.K
"""

from typing import List, Dict, Any, Optional


class SQLBuilderError(Exception):
    """Custom exception for SQL builder related errors."""
    pass


def _format_value(value: Any) -> str:
    """
    Safely format a value for SQL usage.
    (Basic protection against common mistakes)
    """
    if isinstance(value, str):
        return f"'{value}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if value is None:
        return "NULL"
    return str(value)


def build_select_query(
    *,
    table: str,
    columns: Optional[List[str]] = None,
    where: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None
) -> str:
    """
    Build a dynamic SELECT SQL query.
    """

    if not table or not isinstance(table, str):
        raise SQLBuilderError("Table name must be a non-empty string")

    if columns is not None and not isinstance(columns, list):
        raise SQLBuilderError("Columns must be a list of strings")

    if where is not None and not isinstance(where, dict):
        raise SQLBuilderError("WHERE clause must be a dictionary")

    if limit is not None and (not isinstance(limit, int) or limit <= 0):
        raise SQLBuilderError("LIMIT must be a positive integer")

    select_clause = "SELECT *" if not columns else "SELECT " + ", ".join(columns)
    from_clause = f"FROM {table}"

    where_clause = ""
    if where:
        conditions = [
            f"{col} = {_format_value(val)}"
            for col, val in where.items()
        ]
        where_clause = "WHERE " + " AND ".join(conditions)

    limit_clause = f"LIMIT {limit}" if limit else ""

    query = " ".join(
        part for part in [select_clause, from_clause, where_clause, limit_clause]
        if part
    ).strip()

    return query
