from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from helpers.exceptions import EmptyDataExceptionError, OldDataExceptionError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

OPEN_END_DATE = datetime(9999, 12, 31)


def slowly_changing_dimension(
    df_src: DataFrame,
    df_tgt: DataFrame,
    initial_load_flag: bool,
    business_keys: list[str],
    date_column: str,
    ignore_columns: list[str] | None = None,
    non_copy_fields: list[str] | None = None,
    open_end_date: datetime = OPEN_END_DATE,
) -> DataFrame:
    """

    ==============================================

    Slowly changing dimension type2 implementation

    ==============================================



    SCD2 is a storage technique used to represent the historical changes in the data. source data needs to be organized
    with date column

    which represnets data in snapshots/increments

    Method is designed to load target in following mode



    FULL INITIAL LOAD

    -----------------

    Loader switches to full initial load mode when taget dataframe is empty. Data mutation is calculated based on the
    entertity of input dataframe



    INCREMENTAL/CACTH LOAD

    ----------------------

    Loader switches to incremental load mode when taget dataframe exist and data older than max date availbale in input
    dataframe.

    Data mutation is calculated based on the input dataframe filtered for snapshot date greater than max target load
    date




    #TO DO

    - Define the load type (Full/Incremntal)

    - Params to define custom start and end date column names



    :param df_src: Input dataframe object

    :type df_src: DataFrame

    :param df_tgt: Target dataframe object

    :type df_tgt: DataFrame

    :param initial_load_flag: Indicate if target exist or not

    :type initial_load_flag: boolean

    :param business_keys: One of more attributes which constitue uniqueness of the data

    :type business_keys: list

    :param ignore_columns: One of more attributes which can be ignored for historical changes

    :type ignore_columns: list

    :param non_copy_fields: One of more attributes which can be ignored from source to be copied to target

    :type non_copy_fields: list

    :param date_column: snapshot date column

    :type date_column: list

    :param open_end_date: specific datetime to be used for open end date for active records.Default is 9999-12-31

    :type open_end_date: datetime

    """

    if non_copy_fields is None:
        non_copy_fields = []
    if ignore_columns is None:
        ignore_columns = []
    scd_columns = [
        "valid_from",
        "valid_until",
        # "insert_dts",
        # "update_dts",
        "active_flag",
        "delete_flag",
        "row_hash",
    ]

    v_start_date = df_src.agg(f.max(date_column)).first()
    if v_start_date is None or v_start_date[0] is None:
        raise EmptyDataExceptionError

    v_start_date = v_start_date[0]

    df = df_src.drop(*non_copy_fields)

    source_columns = df.columns

    source_columns.remove("snapshot_date")

    target_columns = df.columns + scd_columns

    target_columns.remove("snapshot_date")

    target_columns.append("upsert_flag")

    df_processing = df.withColumn(
        "orig_valid_from", f.lit(None).cast("timestamp")
    ).withColumn("orig_valid_until", f.lit(None).cast("timestamp"))

    if not initial_load_flag:
        spark = SparkSession.builder.getOrCreate()

        df_dates = df_tgt.withColumnRenamed(
            "valid_from", "orig_valid_from"
        ).withColumnRenamed("valid_until", "orig_valid_until")

        df_tgt_select = df_dates.drop(*scd_columns)

        tgt_max_load_date_row = df_tgt.agg(
            f.max("valid_from").alias("max_date")
        ).first()
        tgt_max_load_date = None
        df_tgt_curr = spark.createDataFrame(
            [], df_tgt_select.schema
        )  # Initialize with an empty DataFrame
        if tgt_max_load_date_row is not None:
            tgt_max_load_date = tgt_max_load_date_row["max_date"]

        if v_start_date < tgt_max_load_date:
            raise OldDataExceptionError

        if tgt_max_load_date:
            df_tgt_curr = df_tgt_select.withColumn(
                date_column, f.lit(tgt_max_load_date)
            )

        df_processing = df_processing.filter(
            df_processing.snapshot_date > f.lit(tgt_max_load_date)
        ).union(df_tgt_curr.select(df_processing.columns))

    # remove the duplicate records

    df_dist = df_processing.distinct()

    window_func = Window.partitionBy(business_keys).orderBy(date_column)

    df_date_window = (
        df_dist.select(date_column)
        .distinct()
        .withColumn(
            "next_date_available",
            f.lead(date_column).over(Window.partitionBy().orderBy(date_column)),
        )
    )

    # last delivered date

    max_delivered_date_row = df_date_window.agg(
        f.max(date_column).alias("max_date")
    ).first()
    max_delivered_date = (
        max_delivered_date_row.max_date if max_delivered_date_row else None
    )

    df_with_dates = (
        df_dist.join(df_date_window, date_column, "left")
        .withColumn(
            "date_lead",
            f.lead(date_column).over(
                Window.partitionBy(business_keys).orderBy(date_column)
            ),
        )
        .withColumn(
            "deleted",
            f.when(
                (f.col("next_date_available") != f.col("date_lead"))
                | (
                    (f.col("date_lead").isNull())
                    & (df_dist[date_column] != max_delivered_date)
                ),
                True,
            ).otherwise(False),
        )
    )

    df_with_delete_periods = (
        df_with_dates.drop("next_date_available", "date_lead")
        .withColumn("deleted", f.lit(False))
        .union(
            df_with_dates.where(f.col("deleted"))
            .drop(date_column, "date_lead")
            .withColumnRenamed("next_date_available", date_column)
            .select(df_with_dates.drop("next_date_available", "date_lead").columns)
        )
    )

    df_hashed = df_with_delete_periods.withColumn(
        "row_hash_changed",
        f.sha2(
            f.concat_ws(
                "|",
                *[x for x in source_columns if x not in ignore_columns] + ["deleted"],
            ),
            256,
        ),
    ).withColumn(
        "row_hash",
        f.sha2(
            f.concat_ws("|", *[x for x in source_columns if x not in ignore_columns]),
            256,
        ),
    )

    df_hash_window = df_hashed.withColumn(
        "row_hash_changed_lag", f.lag("row_hash_changed").over(window_func)
    )

    # filter to keep only changed hash key values

    df_hash_filter: DataFrame = (
        df_hash_window.filter(
            (df_hash_window.row_hash_changed_lag.isNull())
            | (df_hash_window.row_hash_changed_lag != df_hash_window.row_hash_changed)
        )
        .drop("date_lead", "row_hash_changed_lag", "row_hash_changed")
        .withColumn("next_change", f.lead(date_column).over(window_func))
        .withColumn("delete_flag", f.lead("deleted").over(window_func))
    )

    df_support_columns = (
        df_hash_filter.filter(~f.col("deleted"))
        .withColumn(
            "valid_from",
            f.coalesce(
                df_hash_filter.orig_valid_from,
                f.col(date_column).cast("timestamp"),
            ),
        )
        .withColumn(
            date_column,
            f.coalesce(df_hash_filter.orig_valid_from, f.col(date_column)),
        )
        .withColumn(
            "valid_until",
            f.coalesce(df_hash_filter.next_change, f.lit(open_end_date)),
        )
        .withColumn(
            "delete_flag",
            f.coalesce(df_hash_filter.delete_flag, f.lit(False)),
        )
        .withColumn(
            "active_flag",
            f.when(
                (f.col("valid_until").isNull())
                | (f.col("valid_until") == datetime(9999, 12, 31)),
                True,
            ).otherwise(False),
        )
        .withColumn(
            "upsert_flag",
            f.when(df_hash_filter.orig_valid_from.isNull(), "I").otherwise("U"),
        )
    )

    df_model = df_support_columns.filter(
        (
            f.coalesce(df_support_columns.orig_valid_until, f.lit("9999-12-31"))
            != df_support_columns.valid_until
        )
        | (df_support_columns.orig_valid_from.isNull())
    )

    df_output = df_model.select(target_columns)

    return df_output
