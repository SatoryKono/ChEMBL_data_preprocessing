"""Command line interface to classify activity data.

This script mirrors the Power Query logic provided in the M script and
exposes a small CLI for classifying activities, assays, documents and other
entities.  The implementation relies on the functions defined in
:mod:`pipeline` and :mod:`status_utils`.

Example
-------
Run the classifier on the bundled example dataset::

    python main.py --input input/same_document --output output
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List, Sequence

import pandas as pd

from constants import Cols
from pipeline import (
    activity_from_pairs,
    aggregate_entities,
    initialize_pairs,
    initialize_status,
    write_csv_with_meta,
)
from status_utils import StatusUtils


def classify_directory(
    input_dir: Path,
    output_dir: Path,
    *,
    sep: str = ",",
    encoding: str = "utf-8",
    log_level: str = "INFO",
) -> None:
    """Classify activity data located in ``input_dir``.

    The classification pipeline writes intermediate tables
    ``InitializeStatus.csv``, ``InitializePairs.csv`` and the new
    ``ActivityInitializeStatus.csv`` before aggregating entities such as
    activities, assays or documents.

    Parameters
    ----------
    input_dir:
        Directory containing ``status.csv``, ``activities.csv`` and
        ``pairs.csv`` files.
    output_dir:
        Destination directory for the generated CSV files.
    sep:
        Field separator used by the input CSV files.  Defaults to ",".
    encoding:
        Encoding of the CSV files.  Defaults to ``"utf-8"``.
    log_level:
        Logging level passed to :func:`logging.basicConfig`.
    """

    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    # Load the raw input tables
    status_df = pd.read_csv(input_dir / "status.csv", sep=sep, encoding=encoding)
    activities_df = pd.read_csv(
        input_dir / "activities.csv", sep=sep, encoding=encoding
    )
    pairs_df = pd.read_csv(input_dir / "pairs.csv", sep=sep, encoding=encoding)

    inputs: List[Path] = [
        input_dir / "status.csv",
        input_dir / "activities.csv",
        input_dir / "pairs.csv",
    ]

    output_dir.mkdir(parents=True, exist_ok=True)

    utils = StatusUtils(status_df)

    # Apply the status initialisation and pair logic
    activities_init = initialize_status(activities_df, utils, "GLOBAL_MIN")
    activities_sorted = activities_init.sort_values(Cols.ACTIVITY_ID).reset_index(
        drop=True
    )
    write_csv_with_meta(
        activities_sorted, output_dir / "InitializeStatus.csv", inputs, "1.0"
    )

    pairs_init = initialize_pairs(pairs_df, activities_init, utils)
    pairs_sorted = pairs_init.sort_values(
        [Cols.ACTIVITY_ID1, Cols.ACTIVITY_ID2]
    ).reset_index(drop=True)
    write_csv_with_meta(pairs_sorted, output_dir / "InitializePairs.csv", inputs, "1.0")

    # Derive activity-level table from pairs to avoid recomputation downstream
    act_pairs = activity_from_pairs(pairs_init, activities_init, utils)
    act_pairs_sorted = act_pairs.sort_values(Cols.ACTIVITY_ID).reset_index(drop=True)
    write_csv_with_meta(
        act_pairs_sorted,
        output_dir / "ActivityInitializeStatus.csv",
        inputs,
        "1.0",
    )

    # Aggregate to all required entity levels
    entities = aggregate_entities(pairs_init, activities_init, utils, act_pairs)

    # Map from entity name to primary sort key column
    sort_keys = {
        "activity": Cols.ACTIVITY_ID,
        "assay": Cols.ASSAY_ID,
        "document": Cols.DOCUMENT_ID,
        "system": Cols.SYSTEM_ID,
        "testitem": Cols.TESTITEM_ID,
        "target": Cols.TARGET_ID,
    }

    for name, df in entities.items():
        key = sort_keys[name]
        df_sorted = df.sort_values(key).reset_index(drop=True)
        cols = [
            key,
            Cols.FILTERED_NEW,
            Cols.INDEPENDENT_IC50,
            Cols.NON_INDEPENDENT_IC50,
            Cols.INDEPENDENT_KI,
            Cols.NON_INDEPENDENT_KI,
        ]
        df_sorted = df_sorted[cols]
        write_csv_with_meta(df_sorted, output_dir / f"{name}.csv", inputs, "1.0")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Return command line arguments."""

    parser = argparse.ArgumentParser(description="Activity classification")
    parser.add_argument("--input", type=Path, default=Path("input/same_document"))
    parser.add_argument("--output", type=Path, default=Path("output"))
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--sep", default=",")
    parser.add_argument("--encoding", default="utf-8")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Script entry point."""

    args = parse_args(argv)
    classify_directory(
        args.input,
        args.output,
        sep=args.sep,
        encoding=args.encoding,
        log_level=args.log_level,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
