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
from status_api import StatusAPI

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

# Columns containing activity counts that may be absent in input data.
COUNT_COLUMNS: List[str] = [
    Cols.INDEPENDENT_IC50,
    Cols.NON_INDEPENDENT_IC50,
    Cols.INDEPENDENT_KI,
    Cols.NON_INDEPENDENT_KI,
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

    return pd.read_csv(path, dtype=dtype)  # type: ignore[arg-type]


def _normalise_activity_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return *df* with legacy column names mapped to the canonical ones.

    Older datasets may label columns differently, e.g. ``test_item.id`` or
    ``standard_type``.  This helper accepts such variations and renames them to
    the identifiers defined in :class:`constants.Cols`.

    Parameters
    ----------
    df:
        Raw activities dataframe.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns renamed in-place when legacy names are
        encountered.
    """

    rename_map: Dict[str, str] = {}
    # Map canonical column names to commonly seen alternatives.
    legacy_names = {
        Cols.TESTITEM_ID: [
            "test_item.id",
            "testitem_id",
            "molecule_chembl_id",
            "molecule_id",
        ],
        Cols.MEASUREMENT_TYPE: ["measurement_type", "standard_type"],
        Cols.ASSAY_ID: ["assay_id"],
        Cols.DOCUMENT_ID: ["document_id"],
        Cols.TARGET_ID: ["target_id"],
    }
    for canonical, alts in legacy_names.items():
        if canonical not in df.columns:
            for alt in alts:
                if alt in df.columns:
                    rename_map[alt] = canonical
                    break
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def initialize_status(
    activities: pd.DataFrame, status: StatusAPI, empty_fallback: str
) -> pd.DataFrame:
    """Add ``no_issue`` and ``Filtered.init`` columns to *activities*.

    Parameters
    ----------
    activities:
        Raw activities dataframe.
    status:
        :class:`StatusAPI` instance.
    empty_fallback:
        Behaviour when no flags are active. ``GLOBAL_MIN`` returns the
        minimal status from the global order, ``ERROR`` raises ``ValueError``.
    """

    df = _normalise_activity_columns(activities.copy())
    df[Cols.NO_ISSUE] = ~df[STATUS_FLAGS].any(axis=1)

    def _compute(row: pd.Series) -> str:
        active_fields = [f for f in STATUS_FLAGS if row.get(f, False)]
        valid = [f for f in active_fields if f in status.condition_fields]
        if valid:
            return status.get_min(valid)
        if empty_fallback.upper() == "GLOBAL_MIN":
            return status.status_list[21]
        raise ValueError("no active status flags")

    df[Cols.FILTERED_INIT] = df.apply(_compute, axis=1)
    return df


def initialize_pairs(
    pairs: pd.DataFrame, activities: pd.DataFrame, status: StatusAPI
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
def _agg_filtered(status: StatusAPI, series: pd.Series) -> str:
    statuses = [s for s in series if isinstance(s, str)]
    return status.get_max(statuses)


def ensure_count_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure count columns exist in ``df``.

    Parameters
    ----------
    df:
        DataFrame that should contain columns defined in :data:`COUNT_COLUMNS`.

    Returns
    -------
    pandas.DataFrame
        Original dataframe with any missing count columns added and filled with
        zeros.  A copy is returned only if new columns are created.
    """

    missing = [c for c in COUNT_COLUMNS if c not in df.columns]
    if not missing:
        return df
    result = df.copy()
    for col in missing:
        result[col] = 0
    return result


def _aggregate(df: pd.DataFrame, group_col: str, status: StatusAPI) -> pd.DataFrame:
    df = ensure_count_columns(df)
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
    """Return a unified activity table built from *pairs*.

    The input ``pairs`` table may originate from different preprocessing
    pipelines.  Some datasets use legacy column names such as
    ``molecule_chembl_id`` or ``standard_type`` instead of the canonical
    :data:`Cols.TESTITEM_ID` and :data:`Cols.MEASUREMENT_TYPE`.  This helper
    normalises such variations before aggregating the activity information.

    Parameters
    ----------
    pairs:
        Dataframe with pairwise activity information.

    Returns
    -------
    pandas.DataFrame
        Deduplicated list of activities with the minimal set of columns
        required for later aggregation steps.
    """

    # ``pairs`` may lack canonical column names when sourced from older
    # pipelines.  Accept common fallbacks and normalise them to the expected
    # names.  This avoids ``KeyError`` when selecting ``cols`` below.
    rename_map: Dict[str, str] = {}
    if Cols.TESTITEM_ID not in pairs.columns:
        for alt in (
            "test_item.id",
            "testitem_id",
            "molecule_chembl_id",
            "molecule_id",
        ):
            if alt in pairs.columns:
                rename_map[alt] = Cols.TESTITEM_ID
                break
    if Cols.MEASUREMENT_TYPE not in pairs.columns:
        # the column is historically misspelled; also accept ``standard_type``
        for alt in ("measurement_type", "standard_type"):
            if alt in pairs.columns:
                rename_map[alt] = Cols.MEASUREMENT_TYPE
                break
    if rename_map:
        pairs = pairs.rename(columns=rename_map)

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
    missing = [c for c in cols if c not in pairs.columns]
    if missing:
        raise KeyError(f"required columns {missing} not found in pairs table")

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
    pair_table: pd.DataFrame, activity_table: pd.DataFrame, status: StatusAPI
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
