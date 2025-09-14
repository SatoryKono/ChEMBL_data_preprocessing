import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from main import classify_directory


def test_classify_directory(tmp_path: Path) -> None:
    """Running ``classify_directory`` creates expected output files."""
    input_dir = Path("tests/data")
    classify_directory(input_dir, tmp_path)
    activity = pd.read_csv(tmp_path / "activity.csv")
    assert "Filtered.new" in activity.columns
    assert (
        activity.loc[activity["activity_chembl_id"] == "a1", "independent_IC50"].iat[0]
        == 1
    )
