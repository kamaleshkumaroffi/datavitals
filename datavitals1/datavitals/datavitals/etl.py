"""
datavitals.etl

Provides a standardized ETL (Extract, Transform, Load) pipeline
that can be reused across multiple data engineering projects.

Author: Kamaleshkumar.K
"""

from typing import List, Dict, Any, Callable, Optional


class ETLError(Exception):
    """Custom exception for ETL-related failures."""
    pass


# -------------------------
# Transform helpers
# -------------------------
def _double_numeric_values(record: Dict[str, Any]) -> Dict[str, Any]:
    """Double all numeric values in a record."""
    transformed = {}
    for key, value in record.items():
        if isinstance(value, (int, float)):
            transformed[key] = value * 2
        else:
            transformed[key] = value
    return transformed


def _identity_transform(record: Dict[str, Any]) -> Dict[str, Any]:
    """Return record as-is (no transformation)."""
    return record


# -------------------------
# Main ETL function
# -------------------------
def run_etl_pipeline(
    *,
    source: List[Dict[str, Any]],
    transform_type: str = "none",
    destination: str = "memory",
    custom_transform: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    Run a standardized ETL pipeline.
    """

    if source is None:
        raise ETLError("Source data cannot be None")

    if not isinstance(source, list):
        raise ETLError("Source data must be a list of dictionaries")

    if len(source) == 0:
        return []

    for record in source:
        if not isinstance(record, dict):
            raise ETLError("Each source record must be a dictionary")

    if custom_transform:
        transform_fn = custom_transform
    elif transform_type == "double":
        transform_fn = _double_numeric_values
    elif transform_type == "none":
        transform_fn = _identity_transform
    else:
        raise ETLError(f"Unsupported transform type: {transform_type}")

    transformed_data: List[Dict[str, Any]] = []

    for record in source:
        try:
            transformed_record = transform_fn(record)
            transformed_data.append(transformed_record)
        except Exception as exc:
            raise ETLError(f"Transformation failed for record {record}") from exc

    if destination == "memory":
        return transformed_data
    else:
        raise ETLError(f"Unsupported destination type: {destination}")
