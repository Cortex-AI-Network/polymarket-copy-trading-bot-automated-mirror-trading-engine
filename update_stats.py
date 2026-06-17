"""
poly-maker  ·  Account Stats Updater
Runs update_stats_once() every 3 hours with retry, backoff, and structured logging.
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

from dotenv import load_dotenv

from poly_data.polymarket_client import PolymarketClient
from poly_stats.account_stats import update_stats_once

load_dotenv()


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    interval_sec:    int   = int(os.getenv("UPDATE_INTERVAL_SEC",   str(60 * 60 * 3)))
    max_retries:     int   = int(os.getenv("MAX_RETRIES",           "3"))
    retry_delay_sec: float = float(os.getenv("RETRY_BASE_DELAY_SEC","5.0"))
    log_file:        str   = os.getenv("LOG_FILE", "update_stats.log")
    log_level:       str   = os.getenv("LOG_LEVEL", "INFO").upper()

CFG = Config()


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger(cfg: Config) -> logging.Logger:
    fmt = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-8s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("update_stats")
    log.setLevel(cfg.log_level)
    for h in [logging.StreamHandler(sys.stdout), logging.FileHandler(cfg.log_file, encoding="utf-8")]:
        h.setFormatter(fmt)
        log.addHandler(h)
    return log

log = _setup_logger(CFG)


# ── Metrics ───────────────────────────────────────────────────────────────────

@dataclass
class Metrics:
    cycles:       int = 0
    successes:    int = 0
    failures:     int = 0
    retries:      int = 0
    started_at:   datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def uptime(self) -> str:
        s = int((datetime.now(timezone.utc) - self.started_at).total_seconds())
        h, r = divmod(s, 3600); m, s = divmod(r, 60)
        return f"{h:02d}h{m:02d}m{s:02d}s"

    def summary(self) -> str:
        rate = self.successes / self.cycles * 100 if self.cycles else 0
        return (f"uptime={self.uptime()} cycles={self.cycles} "
                f"ok={self.successes} fail={self.failures} "
                f"retries={self.retries} success_rate={rate:.1f}%")

METRICS = Metrics()


# ── Graceful shutdown ─────────────────────────────────────────────────────────

_shutdown = False

def _handle_signal(sig, _frame):
    global _shutdown
    if not _shutdown:
        log.info("Shutdown signal received — stopping after current cycle …")
        _shutdown = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Core logic ────────────────────────────────────────────────────────────────

def _run_with_retry(client: PolymarketClient, cfg: Config) -> bool:
    """Call update_stats_once with exponential back-off. Returns True on success."""
    for attempt in range(1, cfg.max_retries + 2):
        try:
            update_stats_once(client)
            return True
        except Exception:
            is_last = attempt == cfg.max_retries + 1
            if is_last:
                log.error("All %d attempt(s) exhausted:\n%s", attempt, traceback.format_exc())
                return False
            delay = cfg.retry_delay_sec * (2 ** (attempt - 1))
            METRICS.retries += 1
            log.warning("Attempt %d/%d failed — retrying in %.0fs …", attempt, cfg.max_retries + 1, delay)
            _interruptible_sleep(delay)
    return False


def _interruptible_sleep(seconds: float) -> None:
    """Sleep in 1-second ticks so SIGINT is handled promptly."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and not _shutdown:
        time.sleep(1)


def run(client: PolymarketClient, cfg: Config = CFG) -> None:
    log.info("━" * 56)
    log.info("  update_stats daemon  ·  starting up")
    log.info("  interval=%dh  retries=%d  log=%s",
             cfg.interval_sec // 3600, cfg.max_retries, cfg.log_file)
    log.info("━" * 56)

    while not _shutdown:
        METRICS.cycles += 1
        t0  = time.monotonic()
        now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

        log.info("┌─ Cycle #%d  [%s]", METRICS.cycles, now)
        success = _run_with_retry(client, cfg)
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
    log.info("  update_stats daemon stopped.  %s", METRICS.summary())
    log.info("━" * 56)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run(PolymarketClient())