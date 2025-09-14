# ChEMBL Data Classification

This repository provides a Python implementation of the classification logic
initially expressed in M-code. The pipeline loads activity data, applies status
rules, processes pairs and produces deterministic exports for multiple entity
levels.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
python classify.py --config config.yaml
```

Use `--print-config` to display the final configuration after applying
environment overrides.

## Development

Formatting and linting:

```bash
black .
ruff .
mypy .
pytest
```
