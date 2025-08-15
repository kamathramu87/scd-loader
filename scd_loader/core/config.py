from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any

# Constants
OPEN_END_DATE = datetime(9999, 12, 31)
DEFAULT_DATE_COLUMN = "snapshot_date"
UPSERT_FLAG_COLUMN = "upsert_flag"
HASH_ALGORITHM = "SHA-256"
HASH_SEPARATOR = "|"


@dataclass
class SCD2Columns:
    """Configuration for SCD2 column names."""

    valid_from: str = "valid_from"
    valid_until: str = "valid_until"
    active_flag: str = "active_flag"
    delete_flag: str = "delete_flag"
    row_hash: str = "row_hash"
    latest_record_flag: str = "latest_record_flag"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SCD2Columns:
        """Create SCD2Columns instance from dictionary, filtering valid fields."""
        field_names = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in field_names}
        return cls(**filtered_data)

    def field_list(self) -> list[str]:
        """Get list of all field names."""
        return [f.name for f in fields(self)]

    def column_list(self) -> list[str]:
        """Get list of all column values."""
        return [getattr(self, f.name) for f in fields(self)]


@dataclass
class SCD2Config:
    """Configuration for SCD2 processing."""

    business_keys: list[str]
    date_column: str = DEFAULT_DATE_COLUMN
    ignore_columns: list[str] | None = None
    non_copy_fields: list[str] | None = None
    open_end_date: datetime | None = OPEN_END_DATE
    scd_columns: SCD2Columns | None = None
    enable_latest_record_flag: bool = False

    def __post_init__(self) -> None:
        """Initialize default values for optional fields."""
        if self.ignore_columns is None:
            self.ignore_columns = []
        if self.non_copy_fields is None:
            self.non_copy_fields = []
        if self.scd_columns is None:
            self.scd_columns = SCD2Columns()

    @classmethod
    def create(
        cls,
        business_keys: list[str] | str,
        date_column: str = DEFAULT_DATE_COLUMN,
        ignore_columns: list[str] | None = None,
        non_copy_fields: list[str] | None = None,
        open_end_date: datetime | None = OPEN_END_DATE,
        scd_columns: dict[str, str] | None = None,
        enable_latest_record_flag: bool = False,
    ) -> SCD2Config:
        """Factory method to create SCD2Config with proper validation."""
        if isinstance(business_keys, str):
            business_keys = [business_keys]

        return SCD2Config(
            business_keys=business_keys,
            date_column=date_column,
            ignore_columns=ignore_columns or [],
            non_copy_fields=non_copy_fields or [],
            open_end_date=open_end_date,
            scd_columns=SCD2Columns.from_dict(scd_columns) if scd_columns else None,
            enable_latest_record_flag=enable_latest_record_flag,
        )
