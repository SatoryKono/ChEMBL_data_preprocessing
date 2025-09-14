# ChEMBL data preprocessing

This project provides a small command line tool and library for
reproducing parts of the ChEMBL activity classification pipeline using
`pandas`.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python main.py --input input/same_document --output output
```

The command generates intermediate tables `InitializeStatus.csv`,
`InitializePairs.csv` and `ActivityInitializeStatus.csv` alongside the
aggregated entity tables (e.g. `activity.csv`, `assay.csv`). Outputs are
written into the directory given via `--output`.
