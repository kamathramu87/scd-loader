from __future__ import annotations


class OldDataExceptionError(Exception):
    def __init__(self):
        super().__init__("Data is older than last target load date")


class EmptyDataExceptionError(Exception):
    def __init__(self):
        super().__init__("Empty dataframe, exiting scd2 load")
