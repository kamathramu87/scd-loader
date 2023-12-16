from __future__ import annotations

from datetime import datetime

import pytest
from chispa.dataframe_comparer import assert_df_equality

from helpers.exceptions import EmptyDataExceptionError, OldDataExceptionError
from scd_loader.scd2_loader import slowly_changing_dimension


@pytest.fixture(scope="session")
def data_day1(spark):
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
    df = spark.createDataFrame(data)
    return df


@pytest.fixture(scope="session")
def data_day2(spark):
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
    df = spark.createDataFrame(data)
    return df


@pytest.fixture(scope="session")
def data_day3(spark):
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
    df = spark.createDataFrame(data)
    return df


def test_scd2_initial_load_null_end_date(
    spark,
    data_day1,
    data_day2,
    primary_key,
    date_attribute,
    scd2_schema,
    expected_data_null_end_date,
):
    output_df = slowly_changing_dimension(
        df_src=data_day1.union(data_day2),
        df_tgt=data_day1,
        date_column=date_attribute,
        business_keys=primary_key,
        initial_load_flag=True,
        open_end_date=None,
    )
    assert_df_equality(
        expected_data_null_end_date.select(output_df.columns),
        output_df,
        ignore_nullable=True,
    )


def test_scd2_initial_load_open_end_date(
    spark,
    data_day1,
    data_day2,
    primary_key,
    date_attribute,
    scd2_schema,
    expected_data_open_end_date,
):
    output_df = slowly_changing_dimension(
        df_src=data_day1.union(data_day2),
        df_tgt=data_day1,
        date_column=date_attribute,
        business_keys=primary_key,
        initial_load_flag=True,
    )
    assert_df_equality(
        expected_data_open_end_date.select(output_df.columns),
        output_df,
        ignore_nullable=True,
    )


def test_scd2_target_exist_cacthup_one_day(
    spark,
    data_day1,
    data_day2,
    data_day3,
    primary_key,
    date_attribute,
    scd2_schema,
    expected_data_open_end_date,
    expected_data_catchup_one_day,
):
    output_df = slowly_changing_dimension(
        df_src=data_day1.union(data_day2).union(data_day3),
        df_tgt=expected_data_open_end_date,
        date_column=date_attribute,
        business_keys=primary_key,
        initial_load_flag=False,
    )
    assert_df_equality(
        expected_data_catchup_one_day.select(output_df.columns),
        output_df,
        ignore_nullable=True,
    )


def test_no_data_exception(
    spark,
    data_day1,
    data_day2,
    data_day3,
    primary_key,
    date_attribute,
    scd2_schema,
    expected_data_open_end_date,
    expected_data_catchup_one_day,
):
    with pytest.raises(EmptyDataExceptionError) as msg:
        slowly_changing_dimension(
            df_src=data_day1.filter("1=0"),
            df_tgt=None,
            date_column=date_attribute,
            business_keys=primary_key,
            initial_load_flag=True,
        )
    assert str(msg.value) == "Empty dataframe, exiting scd2 load"


def test_old_data_exception(
    spark,
    data_day1,
    data_day2,
    data_day3,
    primary_key,
    date_attribute,
    scd2_schema,
    expected_data_open_end_date,
    expected_data_catchup_one_day,
):
    with pytest.raises(OldDataExceptionError) as msg:
        slowly_changing_dimension(
            df_src=data_day1,
            df_tgt=expected_data_open_end_date,
            date_column=date_attribute,
            business_keys=primary_key,
            initial_load_flag=False,
        )
    assert str(msg.value) == "Data is older than last target load date"
