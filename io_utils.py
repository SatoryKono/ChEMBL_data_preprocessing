"""I/O helper utilities for the classification pipeline."""

from __future__ import annotations

from datetime import datetime
import hashlib
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import yaml


def load_csv(path: Path, dtype: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """Load a CSV file using UTF-8 encoding."""

    return pd.read_csv(path, dtype=dtype)  # type: ignore[arg-type]


def write_csv_with_meta(
    df: pd.DataFrame,
    path: Path,
    inputs: list[Path],
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
    path.with_suffix(".meta.yaml").write_text(yaml.safe_dump(meta))
