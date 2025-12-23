"""
Tests for datavitals.etl module

Author       : Kamaleshkumar.K
Project Name : datavitals
Purpose      : Validate ETL pipeline including:
              1. Full flow transformation
              2. Empty input handling
              3. Custom transformation functions
              4. Error handling
"""

import pytest
from datavitals.etl import run_etl_pipeline, ETLError


@pytest.fixture
def sample_source_data():
    return [
        {"id": 1, "amount": 100},
        {"id": 2, "amount": 200},
        {"id": 3, "amount": 300}
    ]


def test_etl_pipeline_doubles_values(sample_source_data):
    """
    Test standard 'double' transformation type.
    """
    result = run_etl_pipeline(
        source=sample_source_data,
        transform_type="double",
        destination="memory"
    )

    assert isinstance(result, list)
    assert len(result) == len(sample_source_data)
    assert result[0]["amount"] == 200
    assert result[1]["amount"] == 400
    assert result[2]["amount"] == 600


def test_etl_pipeline_identity_transform(sample_source_data):
    """
    Test 'none' transformation type preserves original data.
    """
    result = run_etl_pipeline(
        source=sample_source_data,
        transform_type="none",
        destination="memory"
    )

    assert result == sample_source_data


def test_etl_pipeline_custom_transform(sample_source_data):
    """
    Test user-defined custom transformation function.
    """
    def triple_amount(record):
        record["amount"] = record["amount"] * 3
        return record

    result = run_etl_pipeline(
        source=sample_source_data,
        custom_transform=triple_amount,
        destination="memory"
    )

    assert result[0]["amount"] == 300
    assert result[1]["amount"] == 600
    assert result[2]["amount"] == 900


def test_etl_pipeline_empty_source():
    """
    Ensure empty source returns empty list gracefully.
    """
    result = run_etl_pipeline(
        source=[],
        transform_type="double",
        destination="memory"
    )

    assert result == []


def test_etl_pipeline_invalid_source_type():
    """
    Invalid source type should raise ETLError.
    """
    with pytest.raises(ETLError):
        run_etl_pipeline(
            source="not_a_list",
            transform_type="double",
            destination="memory"
        )


def test_etl_pipeline_unsupported_transform(sample_source_data):
    """
    Unsupported transform type should raise ETLError.
    """
    with pytest.raises(ETLError):
        run_etl_pipeline(
            source=sample_source_data,
            transform_type="unknown",
            destination="memory"
        )


def test_etl_pipeline_unsupported_destination(sample_source_data):
    """
    Unsupported destination type should raise ETLError.
    """
    with pytest.raises(ETLError):
        run_etl_pipeline(
            source=sample_source_data,
            transform_type="double",
            destination="unknown_destination"
        )
