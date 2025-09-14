"""Constants for column names used in the preprocessing pipeline."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Cols:
    """String constants for dataframe column names."""

    ACTIVITY_ID: str = "activity_chembl_id"
    ACTIVITY_ID1: str = "activity_chembl_id1"
    ACTIVITY_ID2: str = "activity_chembl_id2"
    ASSAY_ID: str = "assay_chembl_id"
    DOCUMENT_ID: str = "document_chembl_id"
    # ``testitem_chembl_id`` and ``mesurement_type`` are the canonical column
    # names in the CSV files shipped with this project.  Historically some
    # datasets used ``molecule_chembl_id`` and ``standard_type`` instead.  The
    # pipeline handles these legacy names but exposes the modern identifiers as
    # constants here.
    TESTITEM_ID: str = "testitem_chembl_id"
    TARGET_ID: str = "target_chembl_id"
    MEASUREMENT_TYPE: str = "mesurement_type"
    INDEPENDENT_IC50: str = "independent_IC50"
    NON_INDEPENDENT_IC50: str = "non_independent_IC50"
    INDEPENDENT_KI: str = "independent_Ki"
    NON_INDEPENDENT_KI: str = "non_independent_Ki"
    FILTERED: str = "Filtered"
    FILTERED_INIT: str = "Filtered.init"
    FILTERED_NEW: str = "Filtered.new"
    NO_ISSUE: str = "no_issue"
    SYSTEM_ID: str = "system_id"
    TYPE: str = "type"
