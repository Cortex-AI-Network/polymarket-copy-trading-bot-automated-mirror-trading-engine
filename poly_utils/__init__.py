"""
poly-maker  ·  poly_utils
=========================
Shared utilities: authenticated and read-only Google Sheets access.
"""

from __future__ import annotations

from poly_utils.google_utils import (
    ReadOnlySpreadsheet,
    ReadOnlyWorksheet,
    get_spreadsheet,
)

__all__ = [
    "get_spreadsheet",
    "ReadOnlySpreadsheet",
    "ReadOnlyWorksheet",
]