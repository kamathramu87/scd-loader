class OldDataException(Exception):
    "Data is older than last target load date"
    pass


class EmptyDataException(Exception):
    "Empty dataframe, exiting scd2 load"
    pass
