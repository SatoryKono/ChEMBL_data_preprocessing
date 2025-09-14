"""I/O helpers for the classification pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import pandera as pa

from pipeline import STATUS_FLAGS, COUNT_COLUMNS
from constants import Cols

# ---------------------------------------------------------------------------
# Schema definitions --------------------------------------------------------
STATUS_SCHEMA = pa.DataFrameSchema(
    {
        "status": pa.Column(str),
        "condition_field": pa.Column(str),
        "condition_value": pa.Column(str),
        "order": pa.Column(int),
        "score": pa.Column(int),
    }
)

ACTIVITY_SCHEMA = pa.DataFrameSchema(
    {
        Cols.ACTIVITY_ID: pa.Column(str),
        Cols.ASSAY_ID: pa.Column(str),
        Cols.DOCUMENT_ID: pa.Column(str),
        Cols.TESTITEM_ID: pa.Column(str),
        Cols.TARGET_ID: pa.Column(str),
        Cols.MEASUREMENT_TYPE: pa.Column(str),
        **{flag: pa.Column(bool, required=False) for flag in STATUS_FLAGS},
        **{col: pa.Column(float, required=False) for col in COUNT_COLUMNS},
    },
    strict=False,
)

PAIRS_SCHEMA = pa.DataFrameSchema(
    {
        Cols.ACTIVITY_ID1: pa.Column(str),
        Cols.ACTIVITY_ID2: pa.Column(str),
    },
    strict=False,
)

# ---------------------------------------------------------------------------


def read_csv(path: Path, schema: Optional[pa.DataFrameSchema] = None) -> pd.DataFrame:
    """Read a CSV file using UTF-8 encoding and optional validation."""

    df = pd.read_csv(path)
    if schema is not None:
        df = schema.validate(df, lazy=False)
    return df


def read_activities(path: Path, strict: bool = True) -> pd.DataFrame:
    """Read ``activities.csv`` applying default values and validation."""
    df = read_csv(path, None)
    for flag in STATUS_FLAGS:
        if flag not in df.columns:
            df[flag] = False
        df[flag] = df[flag].fillna(False).astype(bool)
    for col in COUNT_COLUMNS:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].fillna(0.0).astype(float)
    if strict:
        df = ACTIVITY_SCHEMA.validate(df, lazy=False)
    return df


def read_status(path: Path, strict: bool = True) -> pd.DataFrame:
    """Read ``status.csv`` with optional schema validation."""

    return read_csv(path, STATUS_SCHEMA if strict else None)


def read_pairs(path: Path, strict: bool = True) -> pd.DataFrame:
    """Read ``pairs.csv`` with optional validation."""

    return read_csv(path, PAIRS_SCHEMA if strict else None)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write dataframe to ``path`` in a deterministic way."""

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8", lineterminator="\n")
