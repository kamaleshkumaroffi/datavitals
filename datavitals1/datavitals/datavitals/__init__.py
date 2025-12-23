"""
datavitals

A reusable data engineering helper library that standardizes
data cleaning, ETL pipelines, and SQL query building.

Project Name : datavitals
Author       : Kamaleshkumar.K
Version      : 0.1.0
"""

# -------------------------
# Library Metadata
# -------------------------
__project_name__ = "datavitals"
__author__ = "Kamaleshkumar.K"
__version__ = "0.1.0"
__license__ = "MIT"

# -------------------------
# Public API Imports
# -------------------------
from .cleaning import clean_dataframe
from .etl import run_etl_pipeline
from .sql_builder import build_select_query

# -------------------------
# What this package exposes
# -------------------------
__all__ = [
    "clean_dataframe",
    "run_etl_pipeline",
    "build_select_query",
    "__project_name__",
    "__author__",
    "__version__",
]
