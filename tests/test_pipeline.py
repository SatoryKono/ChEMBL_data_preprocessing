import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pipeline import (
    activity_from_pairs,
    aggregate_entities,
    initialize_pairs,
    initialize_status,
)
from status_utils import StatusUtils
from constants import Cols


def load_data():
    status = StatusUtils(pd.read_csv("tests/data/status.csv"))
    activities = pd.read_csv(
        "tests/data/activities.csv",
        true_values=["True"],
        false_values=["False"],
    )
    pairs = pd.read_csv("tests/data/pairs.csv")
    return status, activities, pairs


def test_initialize_status():
    status, activities, _ = load_data()
    init = initialize_status(activities, status, "GLOBAL_MIN")
    assert init.loc[init["activity_chembl_id"] == "a1", "Filtered.init"].iat[0] == "S1"
    assert (
        init.loc[init["activity_chembl_id"] == "a2", "Filtered.init"].iat[0]
        == "no_issue"
    )
    assert init["no_issue"].tolist() == [False, True]


def test_pairs_and_aggregates():
    status, activities, pairs = load_data()
    init_act = initialize_status(activities, status, "GLOBAL_MIN")
    init_pairs = initialize_pairs(pairs, init_act, status)
    assert init_pairs["Filtered"].iat[0] == "S1"

    entities = aggregate_entities(init_pairs, init_act, status)
    activity = entities["activity"]
    assert (
        activity.loc[activity["activity_chembl_id"] == "a1", "independent_IC50"].iat[0]
        == 1
    )
    assay = entities["assay"]
    assert (
        assay.loc[assay["assay_chembl_id"] == "ass1", "Filtered.new"].iat[0]
        == "no_issue"
    )
    document = entities["document"]
    assert (
        document.loc[
            document["document_chembl_id"] == "doc1", "non_independent_Ki"
        ].iat[0]
        == 4
    )
    system = entities["system"]
    assert (
        system.loc[system["system_id"] == "t1_tar1_type1", "independent_Ki"].iat[0] == 2
    )
    testitem = entities["testitem"]
    assert (
        testitem.loc[
            testitem["testitem_chembl_id"] == "t1", "non_independent_IC50"
        ].iat[0]
        == 3
    )
    target = entities["target"]
    assert (
        target.loc[target["target_chembl_id"] == "tar1", "independent_IC50"].iat[0] == 1
    )


def test_activity_from_pairs_merges_status() -> None:
    """``activity_from_pairs`` merges status and resolves final flags."""
    status, activities, pairs = load_data()
    init_act = initialize_status(activities, status, "GLOBAL_MIN")
    init_pairs = initialize_pairs(pairs, init_act, status)
    merged = activity_from_pairs(init_pairs, init_act, status)

    # Columns from ``InitializeStatus`` are present
    assert Cols.FILTERED_INIT in merged.columns
    assert Cols.NO_ISSUE in merged.columns
    # Pair-derived column is retained for comparison
    assert Cols.FILTERED_NEW in merged.columns

    # Count columns are not duplicated after the merge
    assert "independent_IC50_x" not in merged.columns
    assert "independent_IC50_y" not in merged.columns

    # ``Filtered`` is updated according to the status ordering rules
    row_a2 = merged.loc[merged[Cols.ACTIVITY_ID] == "a2"].iloc[0]
    assert row_a2[Cols.FILTERED] == "S2"


def test_pairs_with_legacy_columns() -> None:
    """``aggregate_entities`` handles pairs with legacy column names."""
    status, activities, pairs = load_data()
    legacy_pairs = pairs.rename(
        columns={
            "testitem_chembl_id": "molecule_chembl_id",
            "mesurement_type": "standard_type",
        }
    )
    init_act = initialize_status(activities, status, "GLOBAL_MIN")
    init_pairs = initialize_pairs(legacy_pairs, init_act, status)
    entities = aggregate_entities(init_pairs, init_act, status)
    activity = entities["activity"]
    assert (
        activity.loc[activity["activity_chembl_id"] == "a1", "independent_IC50"].iat[0]
        == 1
    )


def test_activities_with_legacy_columns() -> None:
    """``initialize_status`` accepts activities with legacy column names."""
    status, activities, pairs = load_data()
    legacy_act = activities.rename(
        columns={
            "testitem_chembl_id": "test_item.id",
            "mesurement_type": "measurement_type",
        }
    )
    init_act = initialize_status(legacy_act, status, "GLOBAL_MIN")
    init_pairs = initialize_pairs(pairs, init_act, status)
    # ``aggregate_entities`` should succeed using the normalised columns
    entities = aggregate_entities(init_pairs, init_act, status)
    system = entities["system"]
    assert (
        system.loc[system["system_id"] == "t1_tar1_type1", "independent_Ki"].iat[0] == 2
    )
