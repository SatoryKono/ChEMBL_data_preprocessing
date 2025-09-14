"""Utilities for working with activity statuses.

This module implements helper functions mirroring the behaviour of the
Power Query (M) reference implementation.  The :class:`StatusAPI` class
wraps a status reference table and exposes convenience methods used
throughout the preprocessing pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd


@dataclass
class StatusAPI:
    """Helper object providing status related utilities.

    Parameters
    ----------
    table:
        Reference table containing at least the columns ``status``,
        ``condition_field``, ``condition_value``, ``order`` and ``score``.
    """

    table: pd.DataFrame

    def __post_init__(self) -> None:  # pragma: no cover - simple assignments
        # sort by order so that lookups based on the global ordering are
        # deterministic and mirror the original M implementation
        self.table = self.table.sort_values("order").reset_index(drop=True)
        self.status_list: List[str] = self.table["status"].tolist()
        self.condition_fields: List[str] = self.table[
            self.table["condition_value"] != "null"
        ]["condition_field"].tolist()
        self.order_map: Dict[str, int] = dict(
            zip(self.table["status"], self.table["order"])
        )
        self.score_map: Dict[str, int] = dict(
            zip(self.table["status"], self.table["score"])
        )

    # ------------------------------------------------------------------
    def get_min(self, condition_fields: List[str]) -> str:
        """Return the minimal status matching ``condition_fields``.

        Parameters
        ----------
        condition_fields:
            List of field names that evaluate to ``True`` for the activity.
        """

        subset = self.table[self.table["condition_field"].isin(condition_fields)]
        if subset.empty:
            raise ValueError("no statuses for given condition fields")
        return subset.iloc[0]["status"]

    def get_max(self, statuses: List[str]) -> str:
        """Return the maximal status from ``statuses`` based on ``order``."""

        subset = self.table[self.table["status"].isin(statuses)]
        if subset.empty:
            raise ValueError("no matching statuses")
        return subset.sort_values("order").iloc[-1]["status"]

    def pair(self, status1: Optional[str], status2: Optional[str]) -> Optional[str]:
        """Return the higher priority status between ``status1`` and ``status2``.

        Unknown statuses fall back to the other value.  If neither status is
        known, the function returns the original value when both match and raises
        :class:`ValueError` otherwise.

        Parameters
        ----------
        status1, status2:
            Status labels to compare.  Values not present in the reference table
            are treated as unknown.

        Returns
        -------
        Optional[str]
            The status with higher priority based on the global ordering.  ``None``
            is returned only when both inputs are unknown and ``None``.
        """

        # Normalise ``NaN`` values produced by pandas to ``None`` for easier
        # comparisons and lookups in ``status_list``.
        if pd.isna(status1):
            status1 = None
        if pd.isna(status2):
            status2 = None

        in_table1 = status1 in self.status_list if status1 is not None else False
        in_table2 = status2 in self.status_list if status2 is not None else False

        if in_table1 and in_table2 and status1 is not None and status2 is not None:
            # Lower ``order`` value indicates higher priority.
            return (
                status1
                if self.order_map[status1] <= self.order_map[status2]
                else status2
            )
        if in_table1:
            return status1
        if in_table2:
            return status2
        # Both statuses are unknown; return the value when identical to preserve
        # the information, otherwise fail fast to surface unexpected inputs.
        if status1 == status2:
            return status1
        raise ValueError("unknown status in pair")

    def ascending(self, a: str, b: str) -> int:
        """Comparator returning ``1`` if ``a`` > ``b`` in the global order."""

        if a == b:
            return 0
        return 1 if self.order_map.get(a, -1) > self.order_map.get(b, -1) else -1

    def descending(self, a: str, b: str) -> int:
        """Comparator returning ``1`` if ``a`` < ``b`` in the global order."""

        if a == b:
            return 0
        return -1 if self.order_map.get(a, -1) > self.order_map.get(b, -1) else 1

    def next(self, status_name: str) -> str:
        """Return the status following ``status_name`` in the global order."""

        try:
            idx = self.status_list.index(status_name)
        except ValueError:
            return self.status_list[-1]
        if idx >= len(self.status_list) - 1:
            return self.status_list[-1]
        return self.status_list[idx + 1]

    def get_order(self, status_name: str) -> int:
        """Return the order of ``status_name`` or ``-1`` if unknown."""

        return self.order_map.get(status_name, -1)

    def get_score(self, status_name: str) -> int:
        """Return the score of ``status_name`` or ``-1`` if unknown."""

        return self.score_map.get(status_name, -1)

    def active(self, row: pd.Series) -> List[str]:
        """Return names of active boolean status flags in ``row``.

        Parameters
        ----------
        row:
            A pandas Series representing a single activity.

        Returns
        -------
        list of str
            Column names whose value evaluates to ``True``.
        """

        return [str(col) for col, val in row.items() if bool(val)]
