import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from io_utils import read_activities
from pipeline import STATUS_FLAGS
from constants import Cols


def test_read_activities_defaults() -> None:
    df = read_activities(Path("tests/data/activities.csv"))
    # numeric NaNs become 0.0
    assert df.loc[df[Cols.ACTIVITY_ID] == "a1", Cols.NON_INDEPENDENT_IC50].iat[0] == 0.0
    # all status flags are boolean columns
    assert df[STATUS_FLAGS].dtypes.eq("bool").all()
