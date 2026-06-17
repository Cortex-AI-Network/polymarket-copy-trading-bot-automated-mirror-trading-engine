"""
poly-maker  ·  Google Sheets Client
Authenticated read/write access via service account, with a
zero-credential read-only fallback using the public CSV export API.
"""

from __future__ import annotations

import logging
import os
import re
import urllib.parse
from io import StringIO
from pathlib import Path
from typing import Any

import gspread
import pandas as pd
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

log = logging.getLogger("poly_maker.google")

# ── Constants ─────────────────────────────────────────────────────────────────

_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
_CREDS_CANDIDATES = [Path("credentials.json"), Path("../credentials.json")]
_REQUEST_TIMEOUT  = 30  # seconds

# Known sheet name → GID mapping (position in the workbook).
# Extend as new sheets are added.
_SHEET_GID: dict[str, int] = {
    "Full Markets":       0,
    "All Markets":        1,
    "Volatility Markets": 2,
    "Selected Markets":   3,
    "Hyperparameters":    4,
}
_HYPERPARAMS_REQUIRED_COLS = {"type", "param", "value"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_credentials() -> Path | None:
    """Return the first existing credentials file path, or None."""
    return next((p for p in _CREDS_CANDIDATES if p.exists()), None)


def _extract_sheet_id(url: str) -> str:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not match:
        raise ValueError(f"Cannot extract sheet ID from URL: {url!r}")
    return match.group(1)


def _require_spreadsheet_url() -> str:
    url = os.getenv("SPREADSHEET_URL")
    if not url:
        raise EnvironmentError("SPREADSHEET_URL environment variable is not set.")
    return url


# ── Public factory ────────────────────────────────────────────────────────────

def get_spreadsheet(*, read_only: bool = False) -> gspread.Spreadsheet | "ReadOnlySpreadsheet":
    """
    Return a spreadsheet handle.

    - **Authenticated** (default): requires ``credentials.json`` in the project
      root or one directory above.  Supports both read and write.
    - **Read-only fallback**: pass ``read_only=True`` when credentials are
      unavailable.  Data is fetched via the public CSV export endpoint.

    Raises
    ------
    EnvironmentError
        If ``SPREADSHEET_URL`` is not set.
    FileNotFoundError
        If credentials are missing and ``read_only=False``.
    """
    url   = _require_spreadsheet_url()
    creds = _find_credentials()

    if creds is None:
        if read_only:
            log.warning("No credentials found — falling back to read-only CSV access.")
            return ReadOnlySpreadsheet(url)
        raise FileNotFoundError(
            f"Credentials file not found (tried: {[str(p) for p in _CREDS_CANDIDATES]}). "
            "Pass read_only=True for unauthenticated access."
        )

    log.info("Authenticating with Google Sheets using %s", creds)
    credentials  = Credentials.from_service_account_file(str(creds), scopes=_SCOPES)
    gspread_client = gspread.authorize(credentials)
    return gspread_client.open_by_url(url)


# ── Read-only spreadsheet ─────────────────────────────────────────────────────

class ReadOnlySpreadsheet:
    """
    Minimal read-only Google Sheets client backed by the public CSV export API.
    Implements the subset of the ``gspread.Spreadsheet`` interface used by poly-maker.
    """

    def __init__(self, url: str) -> None:
        self._sheet_id = _extract_sheet_id(url)
        log.debug("ReadOnlySpreadsheet initialised (id=%s)", self._sheet_id)

    def worksheet(self, title: str) -> "ReadOnlyWorksheet":
        return ReadOnlyWorksheet(self._sheet_id, title)


class ReadOnlyWorksheet:
    """
    Read-only worksheet that fetches data via the Google Sheets CSV export API.
    Tries multiple URL formats in priority order, stopping at the first success.
    """

    def __init__(self, sheet_id: str, title: str) -> None:
        self._sheet_id = sheet_id
        self.title     = title

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _candidate_urls(self) -> list[str]:
        """Return CSV export URLs to try, in descending priority order."""
        encoded = urllib.parse.quote(self.title)
        urls    = [
            f"https://docs.google.com/spreadsheets/d/{self._sheet_id}/gviz/tq?tqx=out:csv&sheet={encoded}",
            f"https://docs.google.com/spreadsheets/d/{self._sheet_id}/gviz/tq?tqx=out:csv&sheet={self.title}",
        ]
        # Known GID first, then brute-force fallback
        known_gids  = [_SHEET_GID[self.title]] if self.title in _SHEET_GID else []
        fallback_gids = [g for g in range(5) if g not in known_gids]
        for gid in known_gids + fallback_gids:
            urls.append(
                f"https://docs.google.com/spreadsheets/d/{self._sheet_id}"
                f"/export?format=csv&gid={gid}"
            )
        return urls

    def _fetch_csv(self, url: str) -> pd.DataFrame | None:
        """Fetch *url* and parse as CSV. Returns None on any error."""
        try:
            response = requests.get(url, timeout=_REQUEST_TIMEOUT)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text))
            return df if not df.empty and len(df.columns) > 1 else None
        except Exception as exc:
            log.debug("CSV fetch failed for %s: %s", url, exc)
            return None

    def _is_valid_for_sheet(self, df: pd.DataFrame) -> bool:
        """Apply sheet-specific validation heuristics."""
        if self.title == "Hyperparameters":
            if not _HYPERPARAMS_REQUIRED_COLS.issubset(df.columns):
                log.debug("Hyperparameters: unexpected columns %s", list(df.columns))
                return False
        return True

    def _try_fetch(self) -> pd.DataFrame | None:
        """Iterate candidate URLs and return the first valid DataFrame."""
        for url in self._candidate_urls():
            log.debug("Trying %s …", url)
            df = self._fetch_csv(url)
            if df is not None and self._is_valid_for_sheet(df):
                log.info("Fetched %d rows from sheet %r", len(df), self.title)
                return df
        log.warning("All URL attempts failed for sheet %r", self.title)
        return None

    # ── Public API (mirrors gspread.Worksheet) ────────────────────────────────

    def get_all_records(self) -> list[dict[str, Any]]:
        """Return all rows as a list of dicts (header row becomes dict keys)."""
        df = self._try_fetch()
        return df.to_dict("records") if df is not None else []

    def get_all_values(self) -> list[list[Any]]:
        """Return all rows as a list of lists, with the header row first."""
        df = self._try_fetch()
        if df is None:
            return []
        return [df.columns.tolist()] + df.values.tolist()