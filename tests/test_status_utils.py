import sys
from pathlib import Path

import pandas as pd

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
