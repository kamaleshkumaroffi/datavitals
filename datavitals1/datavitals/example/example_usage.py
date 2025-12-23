"""
datavitals - Full Example Script

This example demonstrates a complete data engineering workflow
using the datavitals library:
1. Data Cleaning
2. ETL Pipeline Execution
3. SQL Query Generation

Author: Kamaleshkumar.K
"""

import pandas as pd

from datavitals.cleaning import clean_dataframe
from datavitals.etl import run_etl_pipeline
from datavitals.sql_builder import build_select_query


def main():
    print("\n==============================")
    print("🚀 DATAVITALS - FULL EXAMPLE")
    print("==============================\n")

    # --------------------------------------------------
    # 1️⃣ CREATE RAW (DIRTY) DATA
    # --------------------------------------------------
    raw_data = {
        "id": [1, 2, 2, 3, None],
        "name": [" Alice ", "Bob", "Bob", None, "Eve"],
        "salary": ["1000", "2000", "2000", "3000", "4000"]
    }

    raw_df = pd.DataFrame(raw_data)
    print("🔹 Raw Data:")
    print(raw_df)

    # --------------------------------------------------
    # 2️⃣ CLEAN THE DATA
    # --------------------------------------------------
    cleaned_df = clean_dataframe(raw_df)

    print("\n✅ Cleaned Data:")
    print(cleaned_df)

    # --------------------------------------------------
    # 3️⃣ PREPARE DATA FOR ETL
    # --------------------------------------------------
    source_data = cleaned_df.to_dict(orient="records")

    # --------------------------------------------------
    # 4️⃣ RUN ETL PIPELINE
    # --------------------------------------------------
    etl_output = run_etl_pipeline(
        source=source_data,
        transform_type="double",
        destination="memory"
    )

    print("\n🔄 ETL Output:")
    for record in etl_output:
        print(record)

    # --------------------------------------------------
    # 5️⃣ BUILD SQL QUERY
    # --------------------------------------------------
    sql_query = build_select_query(
        table="employees",
        columns=["id", "name", "salary"],
        where={"active": True},
        limit=5
    )

    print("\n🧠 Generated SQL Query:")
    print(sql_query)

    print("\n🎉 DATAVITALS WORKFLOW COMPLETED SUCCESSFULLY!\n")


if __name__ == "__main__":
    main()

