"""Compatibility wrapper around :mod:`status_api`.

The project historically exposed a :class:`StatusUtils` class.  To avoid
breaking existing imports the class is re-exported from
:class:`status_api.StatusAPI`.
"""

from status_api import StatusAPI as StatusUtils

__all__ = ["StatusUtils"]
