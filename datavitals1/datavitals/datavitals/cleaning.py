"""
datavitals.cleaning

Provides standardized data cleaning utilities
for reusable data engineering pipelines.

Author: Kamaleshkumar.K
"""

from typing import Optional, Dict, Any
import pandas as pd


class DataCleaningError(Exception):
    """Custom exception for data cleaning errors."""
    pass


def clean_dataframe(
        df: pd.DataFrame,
        *,
        drop_nulls: bool = True,
        drop_duplicates: bool = True,
        trim_strings: bool = True,
        convert_numeric: bool = True,
        fillna_map: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Clean a pandas DataFrame using a standard, reusable strategy.

    Steps:
    1. Trim string columns
    2. Fill missing values using fillna_map
    3. Convert columns to numeric where possible
    4. Drop rows with nulls if drop_nulls=True
    5. Drop duplicates if drop_duplicates=True
    """

    if not isinstance(df, pd.DataFrame):
        raise DataCleaningError("Input must be a pandas DataFrame")

    if df.empty:
        return df.copy()

    cleaned_df = df.copy()

    # 1️⃣ Trim strings
    if trim_strings:
        for col in cleaned_df.select_dtypes(include=["object"]).columns:
            cleaned_df[col] = cleaned_df[col].astype(str).str.strip()

    # 2️⃣ Fill missing values
    if fillna_map:
        for col, value in fillna_map.items():
            if col in cleaned_df.columns:
                cleaned_df[col] = cleaned_df[col].fillna(value)

    # 3️⃣ Convert numeric columns safely
    if convert_numeric:
        for col in cleaned_df.columns:
            try:
                cleaned_df[col] = pd.to_numeric(cleaned_df[col])
            except Exception:
                # Skip columns that cannot be converted to numeric
                continue

    # 4️⃣ Drop nulls
    if drop_nulls:
        cleaned_df = cleaned_df.dropna(how="any")

    # 5️⃣ Drop duplicates
    if drop_duplicates:
        cleaned_df = cleaned_df.drop_duplicates()

    if cleaned_df.empty:
        raise DataCleaningError(
            "Data cleaning resulted in an empty DataFrame. "
            "Check input data or cleaning rules."
        )

    cleaned_df.reset_index(drop=True, inplace=True)
    return cleaned_df
