"""
poly-maker  ·  Market Finder & Sheet Updater
Fetches all Polymarket data, scores markets, and syncs to Google Sheets every hour.
Matches the style and structure of update_stats.py.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd
from gspread_dataframe import set_with_dataframe

from data_updater.find_markets import (
    add_volatility_to_df,
    get_all_markets,
    get_all_results,
    get_markets,
    get_sel_df,
)
from data_updater.google_utils import get_spreadsheet
from data_updater.trading_utils import get_clob_client
from dotenv import load_dotenv

load_dotenv()


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    interval_sec:      int   = int(os.getenv("UPDATE_INTERVAL_SEC",    str(60 * 60)))
    max_retries:       int   = int(os.getenv("MAX_RETRIES",            "3"))
    retry_delay_sec:   float = float(os.getenv("RETRY_BASE_DELAY_SEC", "5.0"))
    maker_reward:      float = float(os.getenv("MAKER_REWARD",         "0.75"))
    min_market_count:  int   = int(os.getenv("MIN_MARKET_COUNT",       "50"))
    max_volatility:    float = float(os.getenv("MAX_VOLATILITY",       "20.0"))
    log_file:          str   = os.getenv("LOG_FILE",  "find_markets.log")
    log_level:         str   = os.getenv("LOG_LEVEL", "INFO").upper()

CFG = Config()

# Columns written to Google Sheets — single source of truth
_SHEET_COLUMNS = [
    "question", "answer1", "answer2", "spread",
    "rewards_daily_rate", "gm_reward_per_100", "sm_reward_per_100",
    "bid_reward_per_100", "ask_reward_per_100",
    "volatility_sum", "volatility/reward", "min_size",
    "1_hour", "3_hour", "6_hour", "12_hour", "24_hour", "7_day", "30_day",
    "best_bid", "best_ask", "volatility_price", "max_spread", "tick_size",
    "neg_risk", "market_slug", "token1", "token2", "condition_id",
]


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger(cfg: Config) -> logging.Logger:
    fmt = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-8s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("find_markets")
    log.setLevel(cfg.log_level)
    for h in [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(cfg.log_file, encoding="utf-8"),
    ]:
        h.setFormatter(fmt)
        log.addHandler(h)
    return log

log = _setup_logger(CFG)


# ── Metrics ───────────────────────────────────────────────────────────────────

@dataclass
class Metrics:
    cycles:     int = 0
    successes:  int = 0
    failures:   int = 0
    retries:    int = 0
    skipped:    int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def uptime(self) -> str:
        s = int((datetime.now(timezone.utc) - self.started_at).total_seconds())
        h, r = divmod(s, 3600); m, s = divmod(r, 60)
        return f"{h:02d}h{m:02d}m{s:02d}s"

    def summary(self) -> str:
        rate = self.successes / self.cycles * 100 if self.cycles else 0
        return (
            f"uptime={self.uptime()} cycles={self.cycles} "
            f"ok={self.successes} fail={self.failures} "
            f"skipped={self.skipped} retries={self.retries} "
            f"success_rate={rate:.1f}%"
        )

METRICS = Metrics()


# ── Graceful shutdown ─────────────────────────────────────────────────────────

_shutdown = False

def _handle_signal(sig, _frame) -> None:
    global _shutdown
    if not _shutdown:
        log.info("Shutdown signal received — stopping after current cycle …")
        _shutdown = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Sheet helpers ─────────────────────────────────────────────────────────────

def _update_sheet(data: pd.DataFrame, worksheet) -> None:
    """
    Write *data* to *worksheet*, padding/clearing any extra rows/columns
    left over from a previous (larger) write.
    """
    all_values        = worksheet.get_all_values()
    existing_rows     = len(all_values)
    existing_cols     = len(all_values[0]) if all_values else 0
    num_rows, num_cols = data.shape

    max_rows = max(num_rows, existing_rows)
    max_cols = max(num_cols, existing_cols)

    padded = pd.DataFrame("", index=range(max_rows), columns=range(max_cols))
    padded.iloc[:num_rows, :num_cols] = data.values
    padded.columns = list(data.columns) + [""] * (max_cols - num_cols)

    set_with_dataframe(
        worksheet, padded,
        include_index=False, include_column_header=True, resize=True,
    )


# ── Scoring ───────────────────────────────────────────────────────────────────

def _proximity_score(value: float) -> float:
    """Score how close a bid/ask is to the 0.10–0.25 or 0.75–0.90 bands."""
    if 0.10 <= value <= 0.25:
        return (0.25 - value) / 0.15
    if 0.75 <= value <= 0.90:
        return (value - 0.75) / 0.15
    return 0.0


def _sort_by_composite(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rank markets by a composite score that rewards high GM rewards,
    low volatility, and bids/asks near favourable price bands.
    """
    std_gm  = df["gm_reward_per_100"].std()
    std_vol = df["volatility_sum"].std()

    df = df.copy()
    df["_s_gm"]  = (df["gm_reward_per_100"] - df["gm_reward_per_100"].mean()) / (std_gm  or 1)
    df["_s_vol"] = (df["volatility_sum"]     - df["volatility_sum"].mean())     / (std_vol or 1)
    df["_score"] = (
        df["_s_gm"] - df["_s_vol"]
        + df["best_bid"].apply(_proximity_score)
        + df["best_ask"].apply(_proximity_score)
    )
    return (
        df.sort_values("_score", ascending=False)
          .drop(columns=["_s_gm", "_s_vol", "_score"])
    )


# ── Core fetch & process ──────────────────────────────────────────────────────

def fetch_and_process_data(cfg: Config = CFG) -> None:
    """
    Full pipeline: fetch → score → filter → write to Sheets.
    Raises on failure so the retry wrapper can handle it.
    """
    # Re-initialise connections each cycle (handles token refresh / timeouts)
    spreadsheet = get_spreadsheet()
    client      = get_clob_client()
    sel_df      = get_sel_df(spreadsheet, "Selected Markets")

    wk_all  = spreadsheet.worksheet("All Markets")
    wk_vol  = spreadsheet.worksheet("Volatility Markets")
    wk_full = spreadsheet.worksheet("Full Markets")

    # ── Fetch ────────────────────────────────────────────────────────────────
    log.info("Fetching all markets …")
    all_df = get_all_markets(client)
    log.info("  markets fetched: %d", len(all_df))

    all_results = get_all_results(all_df, client)
    log.info("  results fetched: %d", len(all_results))

    m_data, all_markets = get_markets(all_results, sel_df, maker_reward=cfg.maker_reward)
    log.info("  orderbook data fetched: %d markets", len(all_markets))

    # ── Enrich ───────────────────────────────────────────────────────────────
    df = add_volatility_to_df(all_markets)
    df["volatility_sum"]     = df["24_hour"] + df["7_day"] + df["14_day"]
    df["volatility/reward"]  = (df["gm_reward_per_100"] / df["volatility_sum"]).round(2).astype(str)
    df = df[_SHEET_COLUMNS]

    # ── Split & sort ─────────────────────────────────────────────────────────
    all_df_sorted = df.sort_values("gm_reward_per_100", ascending=False)
    vol_df        = (
        df[df["volatility_sum"] < cfg.max_volatility]
        .sort_values("gm_reward_per_100", ascending=False)
    )

    log.info("  all=%d  low-volatility=%d", len(all_df_sorted), len(vol_df))

    # ── Guard: skip write if data looks incomplete ────────────────────────────
    if len(all_df_sorted) <= cfg.min_market_count:
        METRICS.skipped += 1
        log.warning(
            "Skipping sheet update — only %d markets returned (threshold=%d).",
            len(all_df_sorted), cfg.min_market_count,
        )
        return

    # ── Write to Sheets ───────────────────────────────────────────────────────
    log.info("Writing to Google Sheets …")
    _update_sheet(all_df_sorted, wk_all)
    _update_sheet(vol_df,        wk_vol)
    _update_sheet(m_data,        wk_full)
    log.info("Sheets updated successfully.")


# ── Retry wrapper ─────────────────────────────────────────────────────────────

def _run_with_retry(cfg: Config) -> bool:
    """Call fetch_and_process_data with exponential back-off. Returns True on success."""
    for attempt in range(1, cfg.max_retries + 2):
        try:
            fetch_and_process_data(cfg)
            return True
        except Exception:
            is_last = attempt == cfg.max_retries + 1
            if is_last:
                log.error("All %d attempt(s) exhausted:\n%s", attempt, traceback.format_exc())
                return False
            delay = cfg.retry_delay_sec * (2 ** (attempt - 1))
            METRICS.retries += 1
            log.warning("Attempt %d/%d failed — retrying in %.0fs …",
                        attempt, cfg.max_retries + 1, delay)
            _interruptible_sleep(delay)
    return False


def _interruptible_sleep(seconds: float) -> None:
    """Sleep in 1-second ticks so SIGINT/SIGTERM is handled promptly."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and not _shutdown:
        time.sleep(1)


# ── Daemon loop ───────────────────────────────────────────────────────────────

def run(cfg: Config = CFG) -> None:
    log.info("━" * 56)
    log.info("  find_markets daemon  ·  starting up")
    log.info("  interval=%dh  retries=%d  min_markets=%d  log=%s",
             cfg.interval_sec // 3600, cfg.max_retries, cfg.min_market_count, cfg.log_file)
    log.info("━" * 56)

    while not _shutdown:
        METRICS.cycles += 1
        t0  = time.monotonic()
        now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

        log.info("┌─ Cycle #%d  [%s]", METRICS.cycles, now)
        success = _run_with_retry(cfg)
        elapsed = time.monotonic() - t0

        if success:
            METRICS.successes += 1
            log.info("└─ ✓  done in %.1fs", elapsed)
        else:
            METRICS.failures += 1
            log.error("└─ ✗  failed after %.1fs", elapsed)

        log.info("   %s", METRICS.summary())

        if _shutdown:
            break

        next_run = datetime.fromtimestamp(
            time.time() + cfg.interval_sec, tz=timezone.utc
        ).strftime("%H:%M:%S UTC")
        log.info("   Next run → %s\n", next_run)
        _interruptible_sleep(cfg.interval_sec)

    log.info("━" * 56)
    log.info("  find_markets daemon stopped.  %s", METRICS.summary())
    log.info("━" * 56)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run()