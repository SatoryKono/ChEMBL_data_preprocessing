"""CLI wrapper for the activity classification pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from constants import Cols
from io_utils import read_activities, read_pairs, read_status, write_csv
from pipeline import aggregate_entities, initialize_pairs, initialize_status
from status_api import StatusAPI

PLAN = [
    "InitializeStatus",
    "InitializePairs",
    "Activity",
    "Assay",
    "Document",
    "System",
    "TestItem",
    "Target",
]


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Activity classification")
    parser.add_argument("--input", default="input/same_document")
    parser.add_argument("--output", default="output/same_document")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--print-plan", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    if args.print_plan:
        print(" -> ".join(PLAN))
        return 0

    input_dir = Path(args.input)
    output_dir = Path(args.output)

    logging.info("loading inputs from %s", input_dir)
    status_df = read_status(input_dir / "status.csv", strict=args.strict)
    activities_df = read_activities(input_dir / "activities.csv", strict=args.strict)
    pairs_df = read_pairs(input_dir / "pairs.csv", strict=args.strict)

    output_dir.mkdir(parents=True, exist_ok=True)

    status = StatusAPI(status_df)
    logging.info("initialising statuses")
    init_act = initialize_status(activities_df, status, empty_fallback="GLOBAL_MIN")
    write_csv(
        init_act.sort_values(Cols.ACTIVITY_ID).reset_index(drop=True),
        output_dir / "InitializeStatus.csv",
    )

    logging.info("processing pairs")
    init_pairs = initialize_pairs(pairs_df, init_act, status)
    write_csv(
        init_pairs.sort_values([Cols.ACTIVITY_ID1, Cols.ACTIVITY_ID2]).reset_index(
            drop=True
        ),
        output_dir / "InitializePairs.csv",
    )

    logging.info("aggregating entities")
    entities = aggregate_entities(init_pairs, init_act, status)

    for name, df in entities.items():
        logging.debug("writing %s with %d rows", name, df.shape[0])
        write_csv(df, output_dir / f"{name}.csv")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
