import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from status_utils import StatusUtils


def make_utils() -> StatusUtils:
    df = pd.read_csv("tests/data/status.csv")
    return StatusUtils(df)


def test_basic_helpers():
    utils = make_utils()
    assert utils.get_min(["high_citation_rate"]) == "S1"
    assert utils.get_max(["S1", "S3"]) == "S3"
    assert utils.pair("S1", "S2") == "S1"
    assert utils.next("S1") == "S2"
    assert utils.get_order("S2") == 2
    assert utils.get_score("S3") == 0


def test_pair_unknown_status_handling():
    utils = make_utils()
    # Unknown status falls back to known value
    assert utils.pair("S1", "bad") == "S1"
    assert utils.pair("bad", "S2") == "S2"

    # Both unknown but identical -> preserved
    assert utils.pair("mystery", "mystery") == "mystery"

    # Distinct unknown statuses trigger error
    with pytest.raises(ValueError):
        utils.pair("x", "y")

    # ``None`` is treated as unknown
    assert utils.pair(None, "S3") == "S3"
    assert utils.pair("S2", None) == "S2"
    assert utils.pair(None, None) is None
