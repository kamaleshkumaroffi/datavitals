"""
datavitals – Test Package Initialization

Author       : Kamaleshkumar.K
Project Name : datavitals
Purpose      : Initialize the test package for pytest and other test runners.
              Validates:
               1. Data Cleaning module
               2. ETL module
               3. SQL Builder module
"""

# -------------------------
# Optional package-level setup for tests
# -------------------------
import pytest


@pytest.fixture(scope="session")
def sample_raw_data():
    """
    Provides a sample raw DataFrame for multiple test modules.
    """
    import pandas as pd

    data = {
        "id": [1, 2, 2, 3, None],
        "name": ["Alice", "Bob", "Bob", None, "Eve"],
        "salary": ["1000", "2000", "2000", "3000", "4000"]
    }

    return pd.DataFrame(data)
