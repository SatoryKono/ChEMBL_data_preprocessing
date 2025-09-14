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
python classify.py --config config.yaml
```

Outputs are written into the directory configured under
`io.output_dir`.
