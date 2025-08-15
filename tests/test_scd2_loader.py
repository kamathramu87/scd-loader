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


class TestActiveIndFeature:
    """Test cases for the latest_record_flag feature."""

    def test_latest_record_flag_disabled_by_default(
        self, data_day1, data_day2, primary_key, date_attribute
    ):
        """Test that latest_record_flag is disabled by default."""
        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=data_day1.union(data_day2),
            date_column=date_attribute,
            business_keys=primary_key,
        )

        # latest_record_flag column should not be present
        assert "latest_record_flag" not in output_df.columns

    def test_latest_record_flag_enabled_column_present(
        self, data_day1, data_day2, primary_key, date_attribute
    ):
        """Test that latest_record_flag column is present when enabled."""
        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=data_day1.union(data_day2),
            date_column=date_attribute,
            business_keys=primary_key,
            enable_latest_record_flag=True,
        )

        # latest_record_flag column should be present
        assert "latest_record_flag" in output_df.columns

    def test_latest_record_flag_single_business_key(self, spark_session):
        """Test latest_record_flag with single business key scenario."""
        # Create test data with multiple records for same business key
        data = [
            {
                "customer_id": 1,
                "status": "Active",
                "snapshot_date": datetime(2022, 1, 1),
            },
            {
                "customer_id": 1,
                "status": "Inactive",
                "snapshot_date": datetime(2022, 2, 1),
            },
            {
                "customer_id": 1,
                "status": "Active",
                "snapshot_date": datetime(2022, 3, 1),
            },
            {
                "customer_id": 1,
                "status": "Suspended",
                "snapshot_date": datetime(2022, 4, 1),
            },
        ]
        df_src = spark_session.createDataFrame(data)

        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=df_src,
            business_keys=["customer_id"],
            date_column="snapshot_date",
            enable_latest_record_flag=True,
        )

        # Should have exactly one record with latest_record_flag=True (the latest one)
        latest_record_flag_true_count = output_df.filter(
            "latest_record_flag = true"
        ).count()
        assert latest_record_flag_true_count == 1

        # The latest record should have latest_record_flag=True
        latest_record = output_df.filter("latest_record_flag = true").collect()[0]
        assert latest_record["status"] == "Suspended"
        assert latest_record["snapshot_date"] == datetime(2022, 4, 1)

    def test_latest_record_flag_multiple_business_keys(self, spark_session):
        """Test latest_record_flag with multiple business keys."""
        data = [
            {
                "customer_id": 1,
                "status": "Active",
                "snapshot_date": datetime(2022, 1, 1),
            },
            {
                "customer_id": 1,
                "status": "Inactive",
                "snapshot_date": datetime(2022, 2, 1),
            },
            {
                "customer_id": 2,
                "status": "Active",
                "snapshot_date": datetime(2022, 1, 1),
            },
            {
                "customer_id": 2,
                "status": "Premium",
                "snapshot_date": datetime(2022, 3, 1),
            },
            {"customer_id": 3, "status": "New", "snapshot_date": datetime(2022, 2, 1)},
        ]
        df_src = spark_session.createDataFrame(data)

        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=df_src,
            business_keys=["customer_id"],
            date_column="snapshot_date",
            enable_latest_record_flag=True,
        )

        # Should have exactly one record with latest_record_flag=True per customer
        latest_record_flag_counts = (
            output_df.filter("latest_record_flag = true").groupBy("customer_id").count()
        )
        collected_counts = latest_record_flag_counts.collect()

        # Each customer should have exactly 1 latest_record_flag=True record
        assert len(collected_counts) == 3
        for row in collected_counts:
            assert row["count"] == 1

    def test_latest_record_flag_vs_active_flag_difference(self, spark_session):
        """Test that latest_record_flag and active_flag work independently."""
        # Create data where latest record has ended (not active)
        data = [
            {
                "customer_id": 1,
                "status": "Active",
                "snapshot_date": datetime(2022, 1, 1),
            },
            {
                "customer_id": 1,
                "status": "Inactive",
                "snapshot_date": datetime(2022, 2, 1),
            },
            {
                "customer_id": 1,
                "status": "Closed",
                "snapshot_date": datetime(2022, 3, 1),
            },  # Latest but ended
            {
                "customer_id": 1,
                "status": "Deleted",
                "snapshot_date": datetime(2022, 4, 1),
            },  # Latest
        ]
        df_src = spark_session.createDataFrame(data)

        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=df_src,
            business_keys=["customer_id"],
            date_column="snapshot_date",
            enable_latest_record_flag=True,
        )

        # Get the record with latest_record_flag=True
        latest_record_flag_record = output_df.filter(
            "latest_record_flag = true"
        ).collect()[0]

        # The latest record should have latest_record_flag=True but active_flag=True (it's the open record)
        assert latest_record_flag_record["latest_record_flag"] == True
        assert (
            latest_record_flag_record["active_flag"] == True
        )  # Should be True since it's the open record
        assert latest_record_flag_record["status"] == "Deleted"

    def test_latest_record_flag_with_custom_column_name(
        self, data_day1, data_day2, primary_key, date_attribute
    ):
        """Test latest_record_flag with custom column name."""
        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=data_day1.union(data_day2),
            date_column=date_attribute,
            business_keys=primary_key,
            enable_latest_record_flag=True,
            scd_columns={"latest_record_flag": "latest_record_flag"},
        )

        # Custom column name should be present
        assert "latest_record_flag" in output_df.columns
        assert "latest_record_flag" not in output_df.columns

    def test_latest_record_flag_with_composite_business_key(self, spark_session):
        """Test latest_record_flag with composite business keys."""
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

        # Should have one latest_record_flag=True per region-product combination
        latest_record_flag_counts = (
            output_df.filter("latest_record_flag = true")
            .groupBy("region", "product")
            .count()
        )
        collected_counts = latest_record_flag_counts.collect()

        # Should have 3 unique region-product combinations
        assert len(collected_counts) == 3
        for row in collected_counts:
            assert row["count"] == 1

        # Verify the latest records are correctly identified
        latest_records = output_df.filter("latest_record_flag = true").collect()
        latest_versions = {
            (row["region"], row["product"]): row["version"] for row in latest_records
        }

        assert latest_versions[("US", "A")] == 2  # Latest version for US-A
        assert latest_versions[("US", "B")] == 1  # Only version for US-B
        assert latest_versions[("EU", "A")] == 2  # Latest version for EU-A
