"""Command line interface for the classification pipeline."""

from __future__ import annotations

import argparse
import logging
from dataclasses import asdict
from pathlib import Path

import yaml

from pipeline import load_config, run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Classify activities")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument(
        "--print-config", action="store_true", help="Print final config and exit"
    )
    parser.add_argument("--input-dir", help="Override input directory")
    parser.add_argument("--output-dir", help="Override output directory")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    cfg = load_config(
        Path(args.config), input_dir=args.input_dir, output_dir=args.output_dir
    )
    logging.basicConfig(level=getattr(logging, cfg.log.level.upper(), logging.INFO))

    if args.print_config:
        print(yaml.safe_dump(asdict(cfg), sort_keys=False))
        return 0

    try:
        run_pipeline(cfg)
    except Exception:  # pragma: no cover - top-level exception handler
        logging.exception("Pipeline failed")
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
