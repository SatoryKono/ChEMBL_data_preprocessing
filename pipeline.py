"""Data processing pipeline for activity classification.

This module contains functions that replicate the behaviour of the original
M-code workflow using pandas. The pipeline performs the following steps:

1. Load input CSV files.
2. Initialise statuses for activities.
3. Process activity pairs.
4. Aggregate data at various entity levels.
5. Write deterministic CSV exports accompanied by meta information.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from collections.abc import Sequence
from dataclasses import dataclass, field, fields
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from status_utils import StatusUtils

__all__ = [
    "Config",
    "IOConfig",
    "StatusConfig",
    "RuntimeConfig",
    "LogConfig",
    "load_config",
    "run_pipeline",
    "load_status",
    "load_activities",
    "load_pairs",
    "initialize_status",
    "initialize_pairs",
    "aggregate_activity",
    "aggregate_assay",
    "aggregate_document",
    "aggregate_system",
    "aggregate_testitem",
    "aggregate_target",
]

VERSION = "0.1.0"

# ---------------------------------------------------------------------------
# configuration models


@dataclass
class IOConfig:
    input_dir: str = "input/same_document"
    output_dir: str = "output"


@dataclass
class StatusConfig:
    empty_min_fallback: str = "GLOBAL_MIN"  # or "ERROR"


@dataclass
class RuntimeConfig:
    fail_on_missing_columns: bool = True
    float_na_fill: float | None = None


@dataclass
class LogConfig:
    level: str = "INFO"


@dataclass
class Config:
    io: IOConfig = field(default_factory=IOConfig)
    status: StatusConfig = field(default_factory=StatusConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    log: LogConfig = field(default_factory=LogConfig)


# ---------------------------------------------------------------------------
# constants

STATUS_FLAGS: list[str] = [
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

ID_COLS = [
    "activity_id",
    "assay_id",
    "document_id",
    "testitem_id",
    "target_id",
    "mesurement_type",
]

METRIC_COLS = [
    "independent_IC50",
    "non_independent_IC50",
    "independent_Ki",
    "non_independent_Ki",
]

# ---------------------------------------------------------------------------
# configuration helpers


def _deep_update(obj: Any, updates: dict[str, Any]) -> None:
    for f in fields(obj):
        key = f.name
        if key in updates:
            value = getattr(obj, key)
            new_val = updates[key]
            if hasattr(value, "__dict__"):
                _deep_update(value, new_val)
            else:
                setattr(obj, key, new_val)


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data


def _env_overrides() -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in os.environ.items():
        if key.startswith("CLASSIFY__"):
            parts = key[len("CLASSIFY__") :].lower().split("__")
        elif key.startswith("CLASSIFY_"):
            parts = key[len("CLASSIFY_") :].lower().split("__")
            if len(parts) == 1 and parts[0] in {"input_dir", "output_dir"}:
                parts = ["io", parts[0]]
        else:
            continue
        target = result
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = yaml.safe_load(value)
    return result


def load_config(
    path: Path, *, input_dir: str | None = None, output_dir: str | None = None
) -> Config:
    """Load configuration from YAML file and environment variables."""

    cfg = Config()
    _deep_update(cfg, _load_yaml(path))
    _deep_update(cfg, _env_overrides())
    if input_dir is not None:
        cfg.io.input_dir = input_dir
    if output_dir is not None:
        cfg.io.output_dir = output_dir
    return cfg


# ---------------------------------------------------------------------------
# CSV loading helpers


def _validate_columns(
    df: pd.DataFrame, required: Sequence[str], name: str, fail: bool
) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        msg = f"{name}: missing columns {missing}"
        if fail:
            raise ValueError(msg)
        logging.warning(msg)


def load_status(path: Path) -> StatusUtils:
    dtype = {
        "status": "string",
        "condition_field": "string",
        "condition_value": "string",
        "order": "int64",
        "score": "int64",
    }
    df = pd.read_csv(path, dtype=dtype, na_filter=False)  # type: ignore[arg-type]
    _validate_columns(df, list(dtype.keys()), path.name, True)
    return StatusUtils(df)


def load_activities(path: Path, runtime: RuntimeConfig) -> pd.DataFrame:
    dtype = {col: "string" for col in ID_COLS}
    bool_dtype = {flag: "boolean" for flag in STATUS_FLAGS}
    dtype.update(bool_dtype)
    df = pd.read_csv(path, dtype=dtype, na_values=[""], keep_default_na=True)  # type: ignore[arg-type]
    _validate_columns(
        df, list(dtype.keys()) + METRIC_COLS, path.name, runtime.fail_on_missing_columns
    )
    for col in METRIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if runtime.float_na_fill is not None:
        df[METRIC_COLS] = df[METRIC_COLS].fillna(runtime.float_na_fill)
    return df


def load_pairs(path: Path, runtime: RuntimeConfig) -> pd.DataFrame:
    dtype = {
        "activity_id1": "string",
        "activity_id2": "string",
        "testitem_id": "string",
        "target_id": "string",
        "mesurement_type": "string",
    }
    df = pd.read_csv(path, dtype=dtype, na_values=[""], keep_default_na=True)  # type: ignore[arg-type]
    _validate_columns(
        df, list(dtype.keys()) + METRIC_COLS, path.name, runtime.fail_on_missing_columns
    )
    for col in METRIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if runtime.float_na_fill is not None:
        df[METRIC_COLS] = df[METRIC_COLS].fillna(runtime.float_na_fill)
    return df


# ---------------------------------------------------------------------------
# status initialisation and pair processing


def initialize_status(
    df: pd.DataFrame, statuses: StatusUtils, empty_fallback: str
) -> pd.DataFrame:
    """Initialise status flags for activities."""

    result = df.copy()
    result["no_issue"] = ~result[STATUS_FLAGS].any(axis=1)

    valid = statuses.status_df[statuses.status_df["condition_value"] != "null"]
    mapping = dict(zip(valid["condition_field"], valid["status"], strict=False))
    global_min = statuses.status_order[0]

    def compute(row: pd.Series) -> str:
        active_flags = [flag for flag in STATUS_FLAGS if bool(row.get(flag))]
        active_statuses = [mapping[f] for f in active_flags if f in mapping]
        if active_statuses:
            return statuses.get_min(active_statuses)
        if empty_fallback == "ERROR":
            raise ValueError("No status for activity")
        return global_min

    result["Filtered.init"] = result.apply(compute, axis=1)
    result["Filtered.init"] = result["Filtered.init"].astype("string")
    return result


def initialize_pairs(
    pairs: pd.DataFrame, activities: pd.DataFrame, statuses: StatusUtils
) -> pd.DataFrame:
    """Attach initial statuses to activity pairs."""

    act = activities[["activity_id", "Filtered.init"]]
    merged = pairs.merge(
        act, left_on="activity_id1", right_on="activity_id", how="left"
    ).rename(columns={"Filtered.init": "Filtered1"})
    merged = merged.drop(columns=["activity_id"])
    merged = merged.merge(
        act, left_on="activity_id2", right_on="activity_id", how="left"
    ).rename(columns={"Filtered.init": "Filtered2"})
    merged = merged.drop(columns=["activity_id"])
    merged["Filtered"] = merged.apply(
        lambda r: statuses.pair(r["Filtered1"], r["Filtered2"]), axis=1
    )
    return merged


# ---------------------------------------------------------------------------
# aggregation helpers


def _aggregate_entity(
    df: pd.DataFrame, group_col: str, statuses: StatusUtils
) -> pd.DataFrame:
    agg_metrics = {col: "sum" for col in METRIC_COLS if col in df.columns}
    grouped = df.groupby(group_col, as_index=False).agg(agg_metrics)
    filtered = (
        df.groupby(group_col)["Filtered"]
        .apply(lambda s: statuses.get_max([v for v in s if isinstance(v, str)]))
        .rename("Filtered.new")
    )
    result = grouped.merge(filtered, left_on=group_col, right_index=True)
    cols = [group_col, "Filtered.new", *METRIC_COLS]
    return result[cols]


def aggregate_activity(pairs: pd.DataFrame, statuses: StatusUtils) -> pd.DataFrame:
    part1 = pairs[
        [
            "activity_id1",
            "testitem_id",
            "target_id",
            "mesurement_type",
            "Filtered",
            *METRIC_COLS,
        ]
    ]
    part2 = pairs[
        [
            "activity_id2",
            "testitem_id",
            "target_id",
            "mesurement_type",
            "Filtered",
            *METRIC_COLS,
        ]
    ].rename(columns={"activity_id2": "activity_id1"})
    unified = pd.concat([part1, part2], ignore_index=True).drop_duplicates()
    unified = unified[unified["activity_id1"].notna() & (unified["activity_id1"] != "")]
    unified = unified.rename(columns={"Filtered": "Filtered"})
    return _aggregate_entity(unified, "activity_id1", statuses)


def aggregate_assay(activities: pd.DataFrame, statuses: StatusUtils) -> pd.DataFrame:
    df = activities.rename(columns={"Filtered.init": "Filtered"})
    return _aggregate_entity(df, "assay_id", statuses)


def aggregate_document(activities: pd.DataFrame, statuses: StatusUtils) -> pd.DataFrame:
    df = activities.rename(columns={"Filtered.init": "Filtered"})
    return _aggregate_entity(df, "document_id", statuses)


def aggregate_system(activities: pd.DataFrame, statuses: StatusUtils) -> pd.DataFrame:
    df = activities.copy()
    df = df.rename(columns={"Filtered.init": "Filtered"})
    df["system_id"] = (
        df["testitem_id"].astype(str)
        + "_"
        + df["target_id"].astype(str)
        + "_"
        + df["mesurement_type"].astype(str)
    )
    return _aggregate_entity(df, "system_id", statuses)


def aggregate_testitem(system_df: pd.DataFrame, statuses: StatusUtils) -> pd.DataFrame:
    df = system_df.copy().rename(columns={"Filtered.new": "Filtered"})
    split = df["system_id"].str.split("_", expand=True)
    df = df.assign(testitem_id=split[0], target_id=split[1], type=split[2])
    return _aggregate_entity(df, "testitem_id", statuses)


def aggregate_target(system_df: pd.DataFrame, statuses: StatusUtils) -> pd.DataFrame:
    df = system_df.copy().rename(columns={"Filtered.new": "Filtered"})
    split = df["system_id"].str.split("_", expand=True)
    df = df.assign(testitem_id=split[0], target_id=split[1], type=split[2])
    return _aggregate_entity(df, "target_id", statuses)


# ---------------------------------------------------------------------------
# output


def _save_with_meta(
    df: pd.DataFrame, path: Path, inputs: Sequence[Path], version: str
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8", lineterminator="\n")
    checksum = sha256(path.read_bytes()).hexdigest()
    meta = {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "version": version,
        "inputs": [str(p) for p in inputs],
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "sha256": checksum,
    }
    with path.with_suffix(".meta.yaml").open("w", encoding="utf-8") as fh:
        yaml.safe_dump(meta, fh, sort_keys=False)


# ---------------------------------------------------------------------------
# orchestration


def run_pipeline(cfg: Config) -> None:
    input_dir = Path(cfg.io.input_dir)
    output_dir = Path(cfg.io.output_dir)

    status_path = input_dir / "status.csv"
    activities_path = input_dir / "activities.csv"
    pairs_path = input_dir / "pairs.csv"

    logging.info("Loading status table from %s", status_path)
    statuses = load_status(status_path)

    logging.info("Loading activities from %s", activities_path)
    activities = load_activities(activities_path, cfg.runtime)
    activities = initialize_status(activities, statuses, cfg.status.empty_min_fallback)

    logging.info("Loading pairs from %s", pairs_path)
    pairs = load_pairs(pairs_path, cfg.runtime)
    pairs = initialize_pairs(pairs, activities, statuses)

    logging.info("Aggregating activity level")
    activity_tbl = aggregate_activity(pairs, statuses).sort_values("activity_id1")
    logging.info("Aggregating assay level")
    assay_tbl = aggregate_assay(activities, statuses).sort_values("assay_id")
    logging.info("Aggregating document level")
    document_tbl = aggregate_document(activities, statuses).sort_values("document_id")
    logging.info("Aggregating system level")
    system_tbl = aggregate_system(activities, statuses).sort_values("system_id")
    logging.info("Aggregating test item level")
    testitem_tbl = aggregate_testitem(system_tbl, statuses).sort_values("testitem_id")
    logging.info("Aggregating target level")
    target_tbl = aggregate_target(system_tbl, statuses).sort_values("target_id")

    logging.info("Writing outputs to %s", output_dir)
    _save_with_meta(
        activity_tbl,
        output_dir / "activity.csv",
        [activities_path, pairs_path],
        VERSION,
    )
    _save_with_meta(assay_tbl, output_dir / "assay.csv", [activities_path], VERSION)
    _save_with_meta(
        document_tbl, output_dir / "document.csv", [activities_path], VERSION
    )
    _save_with_meta(system_tbl, output_dir / "system.csv", [activities_path], VERSION)
    _save_with_meta(
        testitem_tbl, output_dir / "testitem.csv", [activities_path], VERSION
    )
    _save_with_meta(target_tbl, output_dir / "target.csv", [activities_path], VERSION)
