import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pandas as pd

from status_utils import StatusUtils


def _utils() -> StatusUtils:
    df = pd.DataFrame(
        {
            "status": ["A", "B", "C"],
            "condition_field": ["fa", "fb", "fc"],
            "condition_value": ["x", "x", "x"],
            "order": [1, 2, 3],
            "score": [10, 20, 30],
        }
    )
    return StatusUtils(df)


def test_helpers() -> None:
    utils = _utils()
    assert utils.get_min(["B", "C"]) == "B"
    assert utils.get_max(["A", "C"]) == "C"
    assert utils.pair("B", "C") == "B"
    assert utils.ascending("C", "B") == 1
    assert utils.descending("C", "B") == -1
    assert utils.next("B") == "C"
    assert utils.next("C") == "C"
    assert utils.get_order("B") == 2
    assert utils.get_score("B") == 20
