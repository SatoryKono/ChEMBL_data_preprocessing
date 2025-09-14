"""Data processing pipeline for activity classifications."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yaml

from constants import Cols
from status_utils import StatusUtils

STATUS_FLAGS: List[str] = [
    "high_citation_rate",
    "unicellular_organism",
    "review",
    "rounded_data_citation",
    "shuffled_assay",
    "higly_correlated_assay",
    "exact_data_citation",
    "multmol_assay",
    "multifunctional_enzyme",
    "unknown_chirality",
]


@dataclass
class Config:
    """Configuration options for the pipeline."""

    io: Dict[str, str]
    status: Dict[str, str]
    runtime: Dict[str, object]
    log: Dict[str, str]


# ---------------------------------------------------------------------------
def load_csv(path: Path, dtype: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """Load a CSV file using UTF-8 encoding."""

    return pd.read_csv(path, dtype=dtype)


def initialize_status(
    activities: pd.DataFrame, status: StatusUtils, empty_fallback: str
) -> pd.DataFrame:
    """Add ``no_issue`` and ``Filtered.init`` columns to *activities*.

    Parameters
    ----------
    activities:
        Raw activities dataframe.
    status:
        :class:`StatusUtils` instance.
    empty_fallback:
        Behaviour when no flags are active. ``GLOBAL_MIN`` returns the
        minimal status from the global order, ``ERROR`` raises ``ValueError``.
    """

    df = activities.copy()
    df[Cols.NO_ISSUE] = ~df[STATUS_FLAGS].any(axis=1)

    def _compute(row: pd.Series) -> str:
        active_fields = [f for f in STATUS_FLAGS if row.get(f, False)]
        valid = [f for f in active_fields if f in status.condition_fields]
        if valid:
            return status.get_min(valid)
        if empty_fallback.upper() == "GLOBAL_MIN":
            return status.status_list[0]
        raise ValueError("no active status flags")

    df[Cols.FILTERED_INIT] = df.apply(_compute, axis=1)
    return df


def initialize_pairs(
    pairs: pd.DataFrame, activities: pd.DataFrame, status: StatusUtils
) -> pd.DataFrame:
    """Attach initial statuses from *activities* to *pairs* and compute ``Filtered``."""

    left = pairs.merge(
        activities[[Cols.ACTIVITY_ID, Cols.FILTERED_INIT]],
        left_on=Cols.ACTIVITY_ID1,
        right_on=Cols.ACTIVITY_ID,
        how="left",
    ).rename(columns={Cols.FILTERED_INIT: "Filtered1"})
    left = left.drop(columns=[Cols.ACTIVITY_ID])
    merged = left.merge(
        activities[[Cols.ACTIVITY_ID, Cols.FILTERED_INIT]],
        left_on=Cols.ACTIVITY_ID2,
        right_on=Cols.ACTIVITY_ID,
        how="left",
    ).rename(columns={Cols.FILTERED_INIT: "Filtered2"})
    merged = merged.drop(columns=[Cols.ACTIVITY_ID])
    merged[Cols.FILTERED] = merged.apply(
        lambda r: status.pair(r["Filtered1"], r["Filtered2"]), axis=1
    )
    return merged


# ---------------------------------------------------------------------------
def _agg_filtered(status: StatusUtils, series: pd.Series) -> str:
    statuses = [s for s in series if isinstance(s, str)]
    return status.get_max(statuses)


def _aggregate(df: pd.DataFrame, group_col: str, status: StatusUtils) -> pd.DataFrame:
    return (
        df.groupby(group_col)
        .agg(
            {
                Cols.FILTERED: lambda s: _agg_filtered(status, s),
                Cols.INDEPENDENT_IC50: "sum",
                Cols.NON_INDEPENDENT_IC50: "sum",
                Cols.INDEPENDENT_KI: "sum",
                Cols.NON_INDEPENDENT_KI: "sum",
            }
        )
        .rename(columns={Cols.FILTERED: Cols.FILTERED_NEW})
        .reset_index()
    )


def activity_from_pairs(pairs: pd.DataFrame) -> pd.DataFrame:
    cols = [
        Cols.ACTIVITY_ID1,
        Cols.TESTITEM_ID,
        Cols.TARGET_ID,
        Cols.MEASUREMENT_TYPE,
        Cols.FILTERED,
        Cols.INDEPENDENT_IC50,
        Cols.NON_INDEPENDENT_IC50,
        Cols.INDEPENDENT_KI,
        Cols.NON_INDEPENDENT_KI,
    ]
    left = pairs[cols].rename(columns={Cols.ACTIVITY_ID1: Cols.ACTIVITY_ID})
    right = pairs[
        [
            Cols.ACTIVITY_ID2,
            Cols.TESTITEM_ID,
            Cols.TARGET_ID,
            Cols.MEASUREMENT_TYPE,
            Cols.FILTERED,
            Cols.INDEPENDENT_IC50,
            Cols.NON_INDEPENDENT_IC50,
            Cols.INDEPENDENT_KI,
            Cols.NON_INDEPENDENT_KI,
        ]
    ].rename(columns={Cols.ACTIVITY_ID2: Cols.ACTIVITY_ID})
    unified = pd.concat([left, right], ignore_index=True).drop_duplicates()
    unified = unified[
        unified[Cols.ACTIVITY_ID].notna() & (unified[Cols.ACTIVITY_ID] != "")
    ]
    return unified


def aggregate_entities(
    pair_table: pd.DataFrame, activity_table: pd.DataFrame, status: StatusUtils
) -> Dict[str, pd.DataFrame]:
    """Return aggregated tables for all required entities."""

    act_pairs = activity_from_pairs(pair_table)
    activity = _aggregate(act_pairs, Cols.ACTIVITY_ID, status)

    act_df = activity_table.rename(columns={Cols.FILTERED_INIT: Cols.FILTERED})
    assay = _aggregate(act_df, Cols.ASSAY_ID, status)
    document = _aggregate(act_df, Cols.DOCUMENT_ID, status)

    sys_df = act_df.copy()
    sys_df[Cols.SYSTEM_ID] = (
        sys_df[Cols.TESTITEM_ID].astype(str)
        + "_"
        + sys_df[Cols.TARGET_ID].astype(str)
        + "_"
        + sys_df[Cols.MEASUREMENT_TYPE].astype(str)
    )
    system = _aggregate(sys_df, Cols.SYSTEM_ID, status)

    ti_df = system.rename(columns={Cols.FILTERED_NEW: Cols.FILTERED}).copy()
    ti_df[[Cols.TESTITEM_ID, Cols.TARGET_ID, Cols.TYPE]] = ti_df[
        Cols.SYSTEM_ID
    ].str.split("_", expand=True)
    testitem = _aggregate(ti_df, Cols.TESTITEM_ID, status)

    tar_df = system.rename(columns={Cols.FILTERED_NEW: Cols.FILTERED}).copy()
    tar_df[[Cols.TESTITEM_ID, Cols.TARGET_ID, Cols.TYPE]] = tar_df[
        Cols.SYSTEM_ID
    ].str.split("_", expand=True)
    target = _aggregate(tar_df, Cols.TARGET_ID, status)

    return {
        "activity": activity,
        "assay": assay,
        "document": document,
        "system": system,
        "testitem": testitem,
        "target": target,
    }


# ---------------------------------------------------------------------------
def write_csv_with_meta(
    df: pd.DataFrame,
    path: Path,
    inputs: List[Path],
    version: str,
) -> None:
    """Write ``df`` to ``path`` and create accompanying ``.meta.yaml``."""

    path.parent.mkdir(parents=True, exist_ok=True)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    path.write_bytes(csv_bytes)

    meta = {
        "generated": datetime.utcnow().isoformat(),
        "version": version,
        "inputs": [str(p) for p in inputs],
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "sha256": hashlib.sha256(csv_bytes).hexdigest(),
    }
    meta_path = path.with_suffix(".meta.yaml")
    meta_path.write_text(yaml.safe_dump(meta))
