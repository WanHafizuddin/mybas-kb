"""Time-based train/test split: oldest ~80% of rows train, newest ~20% test.

Whole service dates go to one side only, so no day is split between train
and test (same-day trips share traffic and weather, which would leak)."""

from dataclasses import dataclass
from datetime import date

import pandas as pd


@dataclass(frozen=True)
class SplitInfo:
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    train_rows: int
    test_rows: int


def time_split(df: pd.DataFrame, train_frac: float = 0.8, date_col: str = "service_date"):
    dates = sorted(df[date_col].unique())
    if len(dates) < 2:
        raise ValueError("need at least 2 service dates for a time-based split")
    counts = df.groupby(date_col).size().reindex(dates)
    cum = counts.cumsum() / counts.sum()
    # Last training date: the latest date at which we are still <= train_frac,
    # but always leave at least one date for test and one for train.
    n_train_dates = max(1, min(len(dates) - 1, int((cum <= train_frac).sum())))
    cutoff = dates[n_train_dates - 1]
    train = df[df[date_col] <= cutoff]
    test = df[df[date_col] > cutoff]
    info = SplitInfo(
        train_start=dates[0], train_end=cutoff,
        test_start=dates[n_train_dates], test_end=dates[-1],
        train_rows=len(train), test_rows=len(test),
    )
    return train, test, info
