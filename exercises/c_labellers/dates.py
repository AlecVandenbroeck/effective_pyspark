from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import col, udf

import holidays

def is_belgian_holiday(date: datetime.date) -> bool:
    if date is None:
        return None
    be_holidays = holidays.BE()
    return date in be_holidays


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    """Adds a column indicating whether or not the attribute `colname`
    in the corresponding row is a weekend day."""
    is_weekend = udf(lambda z: z.weekday() in [5,6], BooleanType())
    frame = frame.withColumn(new_colname, is_weekend(col(colname)))
    return frame


def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday."""
    is_holiday = udf(is_belgian_holiday, BooleanType())
    frame = frame.withColumn(new_colname, is_holiday(col(colname)))
    return frame


def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    pass


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    pass
