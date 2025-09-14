"""Command line interface for the classification pipeline."""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any, Dict

import yaml

from constants import Cols
from pipeline import (
    Config,
    aggregate_entities,
    initialize_pairs,
    initialize_status,
    load_csv,
    write_csv_with_meta,
)
from status_utils import StatusUtils

DEFAULT_CONFIG = {
    "io": {"input_dir": "input/same_document", "output_dir": "output"},
    "status": {"empty_min_fallback": "GLOBAL_MIN"},
    "runtime": {"fail_on_missing_columns": True, "float_na_fill": None},
    "log": {"level": "INFO"},
}


def read_config(path: Path) -> Dict[str, Any]:
    """Load configuration from *path* and apply environment overrides."""

    cfg: Dict[str, Any] = DEFAULT_CONFIG.copy()
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            cfg.update(yaml.safe_load(fh) or {})
    for env, val in os.environ.items():
        if env.startswith("CLASSIFY__"):
            keys = env[len("CLASSIFY__") :].lower().split("__")
            ref = cfg
            for k in keys[:-1]:
                ref = ref.setdefault(k, {})
            ref[keys[-1]] = val
        elif env.startswith("CLASSIFY_"):
            key = env[len("CLASSIFY_") :].lower()
            if key in cfg.get("io", {}):
                cfg["io"][key] = val
    return cfg


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Activity classification")
    parser.add_argument("--config", default="config.yaml", type=Path)
    parser.add_argument("--print-config", action="store_true")
    parser.add_argument("--input-dir")
    parser.add_argument("--output-dir")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg_dict = read_config(args.config)
    if args.input_dir:
        cfg_dict.setdefault("io", {})["input_dir"] = args.input_dir
    if args.output_dir:
        cfg_dict.setdefault("io", {})["output_dir"] = args.output_dir
    if args.print_config:
        print(yaml.safe_dump(cfg_dict))
        return 0

    config = Config(**cfg_dict)  # type: ignore[arg-type]
    logging.basicConfig(level=getattr(logging, config.log.get("level", "INFO")))

    input_dir = Path(config.io["input_dir"])
    output_dir = Path(config.io["output_dir"])

    status_df = load_csv(input_dir / "status.csv")
    activities_df = load_csv(input_dir / "activities.csv")
    pairs_df = load_csv(input_dir / "pairs.csv")

    utils = StatusUtils(status_df)
    activities_init = initialize_status(
        activities_df, utils, config.status.get("empty_min_fallback", "GLOBAL_MIN")
    )
    pairs_init = initialize_pairs(pairs_df, activities_init, utils)

    entities = aggregate_entities(pairs_init, activities_init, utils)

    sort_keys = {
        "activity": Cols.ACTIVITY_ID,
        "assay": Cols.ASSAY_ID,
        "document": Cols.DOCUMENT_ID,
        "system": Cols.SYSTEM_ID,
        "testitem": Cols.TESTITEM_ID,
        "target": Cols.TARGET_ID,
    }

    inputs = [
        input_dir / "status.csv",
        input_dir / "activities.csv",
        input_dir / "pairs.csv",
    ]

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

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
