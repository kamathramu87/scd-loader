from __future__ import annotations

from datetime import datetime

import pytest
from chispa.dataframe_comparer import assert_df_equality

from loadx import SourceType
from loadx.scd2.loader import SCD2Loader


class TestDeltaTargetTable:
    """Tests for target_table parameter using Delta Lake format."""

    def test_target_table_delta_incremental(self, spark_session, tmp_path):
        """Write SCD2 output to a Delta table, then use target_table for incremental load."""
        delta_path = str(tmp_path / "employees_delta")
        table_name = "test_delta_employees"

        day1 = spark_session.createDataFrame(
            [
                {
                    "employee_id": 100,
                    "name": "Alice",
                    "city": "Amsterdam",
                    "snapshot_date": datetime(2022, 1, 1),
                },
                {
                    "employee_id": 200,
                    "name": "Bob",
                    "city": "London",
                    "snapshot_date": datetime(2022, 1, 1),
                },
            ]
        )
        day2 = spark_session.createDataFrame(
            [
                {
                    "employee_id": 200,
                    "name": "Bob",
                    "city": "Paris",
                    "snapshot_date": datetime(2022, 1, 2),
                },
            ]
        )

        scd = SCD2Loader(spark_session)

        # Initial load — write result to a Delta table
        initial = scd.slowly_changing_dimension(
            df_src=day1,
            business_keys="employee_id",
            source_type=SourceType.INCREMENTAL,
        )
        initial.write.format("delta").save(delta_path)
        spark_session.sql(
            f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_path}'"
        )

        try:
            # Incremental load using target_table — should filter active records
            output = scd.slowly_changing_dimension(
                df_src=day1.union(day2),
                target_table=table_name,
                business_keys="employee_id",
                source_type=SourceType.INCREMENTAL,
            )

            # Same result expected when df_tgt is active records from the Delta table
            expected = scd.slowly_changing_dimension(
                df_src=day1.union(day2),
                df_tgt=spark_session.read.format("delta").load(delta_path).filter(
                    "active_flag = true"
                ),
                business_keys="employee_id",
                source_type=SourceType.INCREMENTAL,
            )

            assert_df_equality(
                expected,
                output,
                ignore_row_order=True,
                ignore_nullable=True,
            )
        finally:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_target_table_delta_only_active_records_used(self, spark_session, tmp_path):
        """Verify inactive records in the Delta table are excluded from the merge."""
        delta_path = str(tmp_path / "employees_active_check")
        table_name = "test_delta_active_check"

        day1 = spark_session.createDataFrame(
            [
                {
                    "employee_id": 100,
                    "name": "Alice",
                    "city": "Amsterdam",
                    "snapshot_date": datetime(2022, 1, 1),
                },
            ]
        )
        day2 = spark_session.createDataFrame(
            [
                {
                    "employee_id": 100,
                    "name": "Alice",
                    "city": "Rotterdam",
                    "snapshot_date": datetime(2022, 1, 2),
                },
            ]
        )
        day3 = spark_session.createDataFrame(
            [
                {
                    "employee_id": 100,
                    "name": "Alice",
                    "city": "Utrecht",
                    "snapshot_date": datetime(2022, 1, 3),
                },
            ]
        )

        scd = SCD2Loader(spark_session)

        # Build a target that has both active and inactive rows
        target_with_history = scd.slowly_changing_dimension(
            df_src=day1.union(day2),
            business_keys="employee_id",
            source_type=SourceType.INCREMENTAL,
        )
        target_with_history.write.format("delta").save(delta_path)
        spark_session.sql(
            f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_path}'"
        )

        try:
            # The table has an inactive row (Amsterdam→Rotterdam transition).
            # target_table should only feed the active row (Rotterdam) into the merge.
            output = scd.slowly_changing_dimension(
                df_src=day1.union(day2).union(day3),
                target_table=table_name,
                business_keys="employee_id",
                source_type=SourceType.INCREMENTAL,
            )

            # Utrecht row must be present and active
            active_rows = output.filter("active_flag = true").collect()
            assert len(active_rows) == 1
            assert active_rows[0]["city"] == "Utrecht"
        finally:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
