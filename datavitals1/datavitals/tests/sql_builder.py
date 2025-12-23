"""
Tests for datavitals.sql_builder module

Author       : Kamaleshkumar.K
Project Name : datavitals
Purpose      : Validate SQL Builder including:
              1. Basic SELECT query generation
              2. WHERE clause handling
              3. Limit clause
              4. Edge cases like empty columns
              5. Error handling
"""

import pytest
from datavitals.sql_builder import build_select_query, SQLBuilderError


def test_basic_select_query():
    """
    Test basic SELECT query generation with columns and WHERE clause.
    """
    query = build_select_query(
        table="users",
        columns=["id", "name"],
        where={"active": True},
        limit=10
    )

    assert isinstance(query, str)
    assert "SELECT" in query.upper()
    assert "FROM users" in query
    assert "WHERE" in query.upper()
    assert "active" in query
    assert "LIMIT 10" in query


def test_select_without_where():
    """
    Test SELECT query generation without WHERE clause.
    """
    query = build_select_query(
        table="products",
        columns=["id", "price"]
    )

    assert "SELECT" in query.upper()
    assert "FROM products" in query
    assert "WHERE" not in query.upper()


def test_select_without_columns():
    """
    Test SELECT query with empty columns should default to '*'.
    """
    query = build_select_query(
        table="logs",
        columns=[]
    )

    assert "SELECT *" in query.upper()


def test_select_with_various_value_types():
    """
    Test WHERE clause with string, boolean, and None values.
    """
    query = build_select_query(
        table="employees",
        columns=["id", "name"],
        where={"name": "Alice", "active": False, "department": None}
    )

    assert "'Alice'" in query
    assert "FALSE" in query
    assert "NULL" in query


def test_invalid_table_name():
    """
    Invalid table name should raise SQLBuilderError.
    """
    with pytest.raises(SQLBuilderError):
        build_select_query(table="", columns=["id"])


def test_invalid_columns_type():
    """
    Columns must be a list of strings, otherwise raise error.
    """
    with pytest.raises(SQLBuilderError):
        build_select_query(table="users", columns="id,name")


def test_invalid_where_type():
    """
    WHERE must be a dictionary, otherwise raise error.
    """
    with pytest.raises(SQLBuilderError):
        build_select_query(table="users", where="active=True")


def test_invalid_limit_type():
    """
    LIMIT must be positive integer, otherwise raise error.
    """
    with pytest.raises(SQLBuilderError):
        build_select_query(table="users", limit=-5)
