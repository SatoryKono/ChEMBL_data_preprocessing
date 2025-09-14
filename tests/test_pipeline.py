import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pipeline import (
    RuntimeConfig,
    aggregate_activity,
    aggregate_assay,
    aggregate_document,
    aggregate_system,
    aggregate_target,
    aggregate_testitem,
    initialize_pairs,
    initialize_status,
    load_activities,
    load_pairs,
    load_status,
)

DATA_DIR = Path(__file__).parent / "data"


def test_initialize_status() -> None:
    statuses = load_status(DATA_DIR / "status.csv")
    activities = load_activities(DATA_DIR / "activities.csv", RuntimeConfig())
    init = initialize_status(activities, statuses, "GLOBAL_MIN")
    assert list(init["Filtered.init"]) == ["WARNING", "ERROR"]


def test_initialize_pairs_and_aggregations() -> None:
    statuses = load_status(DATA_DIR / "status.csv")
    runtime = RuntimeConfig()
    activities = load_activities(DATA_DIR / "activities.csv", runtime)
    activities = initialize_status(activities, statuses, "GLOBAL_MIN")
    pairs = load_pairs(DATA_DIR / "pairs.csv", runtime)
    pairs = initialize_pairs(pairs, activities, statuses)
    assert pairs.loc[0, "Filtered"] == "WARNING"

    activity_tbl = aggregate_activity(pairs, statuses)
    assert (
        activity_tbl.loc[
            activity_tbl["activity_chembl_id"] == "act1", "Filtered.new"
        ].item()
        == "WARNING"
    )

    assay_tbl = aggregate_assay(activities, statuses)
    assert assay_tbl.loc[0, "Filtered.new"] == "ERROR"
    assert assay_tbl.loc[0, "independent_IC50"] == 5

    document_tbl = aggregate_document(activities, statuses)
    assert document_tbl.loc[0, "Filtered.new"] == "ERROR"

    system_tbl = aggregate_system(activities, statuses)
    assert system_tbl.loc[0, "Filtered.new"] == "ERROR"

    testitem_tbl = aggregate_testitem(system_tbl, statuses)
    assert (
        testitem_tbl.loc[
            testitem_tbl["molecule_chembl_id"] == "test1", "Filtered.new"
        ].item()
        == "ERROR"
    )

    target_tbl = aggregate_target(system_tbl, statuses)
    assert (
        target_tbl.loc[
            target_tbl["target_chembl_id"] == "targ1", "Filtered.new"
        ].item()
        == "ERROR"
    )
