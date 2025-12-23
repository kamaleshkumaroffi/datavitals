"""
Tests for datavitals.cleaning module

Author       : Kamaleshkumar.K
Project Name : datavitals
Purpose      : Validate real-world cleaning behaviour including:
              1. Null removal
              2. Duplicate removal
              3. String trimming
              4. Numeric conversion
"""

import pandas as pd
import pytest

from datavitals.cleaning import clean_dataframe, DataCleaningError


def test_cleaning_removes_nulls_and_duplicates(sample_raw_data):
    """
    Test that clean_dataframe removes nulls and duplicates correctly.
    """
    df = sample_raw_data.copy()
    cleaned_df = clean_dataframe(df)

    # Nulls removed
    assert cleaned_df.isnull().sum().sum() == 0

    # Duplicates removed
    assert cleaned_df.duplicated().sum() == 0

    # Dataframe is not empty
    assert len(cleaned_df) > 0

    # Columns remain unchanged
    assert list(cleaned_df.columns) == list(df.columns)


def test_cleaning_with_fillna(sample_raw_data):
    """
    Test fillna_map parameter for selective NA filling.
    """
    df = sample_raw_data.copy()
    fill_map = {"name": "Unknown", "salary": "0"}
    cleaned_df = clean_dataframe(df, fillna_map=fill_map, drop_nulls=False)

    # No nulls for columns in fillna_map
    for col in fill_map.keys():
        assert cleaned_df[col].isnull().sum() == 0


def test_cleaning_preserves_clean_data():
    """
    Ensure clean data remains unchanged.
    """
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "value": [10, 20, 30]
    })

    cleaned_df = clean_dataframe(df)

    # Original data preserved
    assert cleaned_df.equals(df)


def test_cleaning_raises_error_on_invalid_input():
    """
    Test that invalid input raises DataCleaningError.
    """
    with pytest.raises(DataCleaningError):
        clean_dataframe("not_a_dataframe")


def test_cleaning_raises_error_on_empty_result():
    """
    Test that cleaning resulting in empty dataframe raises DataCleaningError.
    """
    df = pd.DataFrame({"col1": [None, None], "col2": [None, None]})
    with pytest.raises(DataCleaningError):
        clean_dataframe(df)
