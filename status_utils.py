"""Utilities for status handling.

This module provides helper functions to work with status definitions
specified in ``status.csv``. The functions reproduce the semantics of the
original M-code helpers used in the data classification workflow.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd


@dataclass
class StatusUtils:
    """Helper class for status operations.

    Parameters
    ----------
    status_df:
        DataFrame with columns ``status``, ``condition_field``,
        ``condition_value``, ``order`` and ``score``. It is expected to be
        validated and contain unique ``status`` values.
    """

    status_df: pd.DataFrame

    def __post_init__(self) -> None:
        # Sort by ``order`` once to establish the global order of statuses
        self.status_df = self.status_df.sort_values("order").reset_index(drop=True)
        self.status_order: list[str] = self.status_df["status"].tolist()
        self.status_to_order: dict[str, int] = dict(
            zip(self.status_order, self.status_df["order"].tolist(), strict=True)
        )
        self.order_to_status: dict[int, str] = {
            order: status for status, order in self.status_to_order.items()
        }
        self.status_to_score: dict[str, int] = dict(
            zip(self.status_order, self.status_df["score"].tolist(), strict=True)
        )

    # ------------------------------------------------------------------
    # basic helpers operating on status names
    def _filter_existing(self, statuses: Sequence[str]) -> list[str]:
        return [s for s in statuses if s in self.status_to_order]

    def get_min(self, statuses: Sequence[str]) -> str:
        """Return the earliest status from ``statuses``.

        Parameters
        ----------
        statuses:
            Sequence of status names.

        Returns
        -------
        str
            Status with minimal ``order`` value.
        """

        filtered = self._filter_existing(statuses)
        if not filtered:
            raise ValueError("Empty status list")
        return min(filtered, key=self.status_to_order.__getitem__)

    def get_max(self, statuses: Sequence[str]) -> str:
        """Return the latest status from ``statuses``."""

        filtered = self._filter_existing(statuses)
        if not filtered:
            raise ValueError("Empty status list")
        return max(filtered, key=self.status_to_order.__getitem__)

    def pair(self, status1: str, status2: str) -> str:
        """Return the earlier of two statuses."""

        return self.get_min([status1, status2])

    def ascending(self, a: str, b: str) -> int:
        """Comparator for ascending order based on ``order`` field."""

        oa = self.get_order(a)
        ob = self.get_order(b)
        if oa > ob:
            return 1
        if oa == ob:
            return 0
        return -1

    def descending(self, a: str, b: str) -> int:
        """Comparator for descending order based on ``order`` field."""

        return -self.ascending(a, b)

    def next(self, status_name: str) -> str:
        """Return the next status in global order.

        If ``status_name`` is the last one or not found, the last status
        is returned.
        """

        if status_name in self.status_order:
            idx = self.status_order.index(status_name)
            if idx < len(self.status_order) - 1:
                return self.status_order[idx + 1]
        return self.status_order[-1]

    def get_order(self, status_name: str) -> int:
        """Return ``order`` for ``status_name`` or ``-1`` if missing."""

        return self.status_to_order.get(status_name, -1)

    def get_score(self, status_name: str) -> int:
        """Return ``score`` for ``status_name`` or ``-1`` if missing."""

        return self.status_to_score.get(status_name, -1)

    # Convenience helper for mapping order to status
    def status_by_order(self, order: int) -> str:
        return self.order_to_status[order]
