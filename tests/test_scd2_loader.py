from __future__ import annotations

from datetime import datetime

from scd_loader.exceptions import EmptyDataExceptionError, OldDataExceptionError
import pytest
from chispa.dataframe_comparer import assert_df_equality

from scd_loader.scd2_loader import SCD2Loader


@pytest.fixture(scope="session")
def data_day1(spark_session):
    data = [
        {
            "employee_id": 100,
            "first_name": "ramu",
            "last_name": "kamath",
            "place": "voorburg",
            "country": "netherlands",
            "snapshot_date": datetime.strptime("2022-01-01", "%Y-%m-%d"),
        }
    ]
    df = spark_session.createDataFrame(data)
    return df


@pytest.fixture(scope="session")
def data_day2(spark_session):
    data = [
        {
            "employee_id": 100,
            "first_name": "ramu",
            "last_name": "kamath",
            "place": "voorburg",
            "country": "netherlands",
            "snapshot_date": datetime.strptime("2022-01-02", "%Y-%m-%d"),
        },
        {
            "employee_id": 200,
            "first_name": "deena",
            "last_name": "mehroof",
            "place": "amsterdam",
            "country": "netherlands",
            "snapshot_date": datetime.strptime("2022-01-02", "%Y-%m-%d"),
        },
    ]
    df = spark_session.createDataFrame(data)
    return df


@pytest.fixture(scope="session")
def data_day3(spark_session):
    data = [
        {
            "employee_id": 200,
            "first_name": "deena",
            "last_name": "mehroof",
            "place": "mangalore",
            "country": "india",
            "snapshot_date": datetime.strptime("2022-01-03", "%Y-%m-%d"),
        },
        {
            "employee_id": 300,
            "first_name": "akansha",
            "last_name": "sahoo",
            "place": "bangalore",
            "country": "india",
            "snapshot_date": datetime.strptime("2022-01-03", "%Y-%m-%d"),
        },
    ]
    df = spark_session.createDataFrame(data)
    return df


class TestSCD2Load:
    def test_scd2_initial_load_null_end_date(
        self,
        data_day1,
        data_day2,
        primary_key,
        date_attribute,
        expected_data_null_end_date,
    ):
        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=data_day1.union(data_day2),
            date_column=date_attribute,
            business_keys=primary_key,
            open_end_date=None,
        )
        assert_df_equality(
            expected_data_null_end_date,
            output_df.select(expected_data_null_end_date.columns),
            ignore_nullable=True,
        )

    def test_scd2_initial_load_open_end_date(
        self,
        data_day1,
        data_day2,
        primary_key,
        date_attribute,
        expected_data_open_end_date,
    ):
        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=data_day1.union(data_day2),
            date_column=date_attribute,
            business_keys=primary_key,
        )
        assert_df_equality(
            expected_data_open_end_date,
            output_df.select(expected_data_open_end_date.columns),
            ignore_nullable=True,
        )

    def test_scd2_target_exist_cacthup_one_day(
        self,
        data_day1,
        data_day2,
        data_day3,
        primary_key,
        date_attribute,
        expected_data_open_end_date,
        expected_data_catchup_one_day,
    ):
        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=data_day1.union(data_day2).union(data_day3),
            df_tgt=expected_data_open_end_date,
            date_column=date_attribute,
            business_keys=primary_key,
        )
        assert_df_equality(
            expected_data_catchup_one_day,
            output_df.select(expected_data_catchup_one_day.columns),
            ignore_nullable=True,
        )

    def test_no_data_exception(self, data_day1, primary_key, date_attribute):
        scd = SCD2Loader()
        with pytest.raises(EmptyDataExceptionError) as msg:
            scd.slowly_changing_dimension(
                df_src=data_day1.filter("1=0"),
                date_column=date_attribute,
                business_keys=primary_key,
            )
        assert str(msg.value) == "Empty dataframe, exiting scd2 load"

    def test_old_data_exception(
        self,
        data_day1,
        primary_key,
        date_attribute,
        expected_data_open_end_date,
    ):
        scd = SCD2Loader()
        with pytest.raises(OldDataExceptionError) as msg:
            scd.slowly_changing_dimension(
                df_src=data_day1,
                df_tgt=expected_data_open_end_date,
                date_column=date_attribute,
                business_keys=primary_key,
            )
        assert str(msg.value) == "Data is older than last target load date"

    def test_latest_record_flag(self, spark_session):
        data = [
            {
                "region": "US",
                "product": "A",
                "version": 1,
                "snapshot_date": datetime(2022, 1, 1),
            },
            {
                "region": "US",
                "product": "A",
                "version": 2,
                "snapshot_date": datetime(2022, 2, 1),
            },
            {
                "region": "US",
                "product": "B",
                "version": 1,
                "snapshot_date": datetime(2022, 1, 1),
            },
            {
                "region": "EU",
                "product": "A",
                "version": 1,
                "snapshot_date": datetime(2022, 1, 1),
            },
            {
                "region": "EU",
                "product": "A",
                "version": 2,
                "snapshot_date": datetime(2022, 3, 1),
            },
        ]
        df_src = spark_session.createDataFrame(data)

        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=df_src,
            business_keys=["region", "product"],
            date_column="snapshot_date",
            enable_latest_record_flag=True,
        )

        # Each business key combination should have exactly one latest_record_flag=True
        latest_counts = (
            output_df.filter("latest_record_flag = true")
            .groupBy("region", "product")
            .count()
        )
        assert latest_counts.count() == 3
        for row in latest_counts.collect():
            assert row["count"] == 1

        # Verify correct records are flagged as latest
        latest_records = output_df.filter("latest_record_flag = true").collect()
        latest_versions = {
            (r["region"], r["product"]): r["version"] for r in latest_records
        }
        assert latest_versions[("US", "A")] == 2
        assert latest_versions[("US", "B")] == 1
        assert latest_versions[("EU", "A")] == 2
