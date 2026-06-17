"""
╔══════════════════════════════════════════════════════════════════╗
║     Cortex AI       poly-maker  ·  Market-Making Daemon                   ║
║  Async WebSocket engine + background state sync + clean shutdown ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
import signal
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone

from dotenv import load_dotenv

import poly_data.global_state as global_state
from poly_data.data_processing import remove_from_performing
from poly_data.data_utils import update_markets, update_orders, update_positions
from poly_data.polymarket_client import PolymarketClient
from poly_data.websocket_handlers import connect_market_websocket, connect_user_websocket

load_dotenv()


# ── Configuration ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    """All tunables in one place — override via environment variables."""

    position_refresh_sec:  int   = int(os.getenv("POSITION_REFRESH_SEC",  "5"))
    market_refresh_cycles: int   = int(os.getenv("MARKET_REFRESH_CYCLES", "6"))   # × position_refresh_sec
    stale_trade_ttl_sec:   float = float(os.getenv("STALE_TRADE_TTL_SEC", "15.0"))
    ws_reconnect_delay:    float = float(os.getenv("WS_RECONNECT_DELAY",  "1.0"))
    log_level:             str   = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file:              str   = os.getenv("LOG_FILE",  "poly_maker.log")


CFG = Config()


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logging(cfg: Config) -> logging.Logger:
    fmt = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("poly_maker")
    logger.setLevel(cfg.log_level)
    for handler in [logging.StreamHandler(sys.stdout), logging.FileHandler(cfg.log_file, encoding="utf-8")]:
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


log = _setup_logging(CFG)


# ── Runtime metrics ───────────────────────────────────────────────────────────

@dataclass
class Metrics:
    sync_cycles:        int = 0
    sync_errors:        int = 0
    stale_removals:     int = 0
    ws_reconnects:      int = 0
    started_at:         datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def uptime(self) -> str:
        delta = datetime.now(timezone.utc) - self.started_at
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m, s   = divmod(rem, 60)
        return f"{h:02d}h{m:02d}m{s:02d}s"

    def summary(self) -> str:
        return (
            f"uptime={self.uptime()} "
            f"sync_cycles={self.sync_cycles} "
            f"sync_errors={self.sync_errors} "
            f"stale_removals={self.stale_removals} "
            f"ws_reconnects={self.ws_reconnects}"
        )


METRICS = Metrics()


# ── Shutdown coordination ─────────────────────────────────────────────────────

_shutdown_event = asyncio.Event()


def _register_signals() -> None:
    """Register SIGINT / SIGTERM handlers that set the shutdown event."""
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig: signal.Signals) -> None:
        if not _shutdown_event.is_set():
            log.info("Signal %s received — initiating graceful shutdown …", sig.name)
            _shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with_suppress = lambda s=sig: _request_shutdown(signal.Signals(s))
        loop.add_signal_handler(sig, lambda s=sig: _request_shutdown(signal.Signals(s)))


# ── State initialisation ──────────────────────────────────────────────────────

def _bootstrap() -> None:
    """Fetch all required state before starting the event loop."""
    log.info("Bootstrapping — fetching markets, positions, and orders …")
    update_markets()
    update_positions()
    update_orders()
    log.info(
        "Bootstrap complete: %d markets │ %d positions │ %d orders",
        len(global_state.df),
        len(global_state.positions),
        len(global_state.orders),
    )
    log.debug("Initial positions: %s", global_state.positions)
    log.debug("Initial orders:    %s", global_state.orders)


# ── Stale-trade cleanup ───────────────────────────────────────────────────────

def _purge_stale_trades(ttl: float) -> None:
    """
    Remove entries from *performing* that have been pending longer than *ttl* seconds.
    Safe: iterates over a snapshot of keys to avoid mutation-during-iteration errors.
    """
    now = time.time()
    for col in list(global_state.performing):
        for trade_id in list(global_state.performing[col]):
            try:
                age = now - global_state.performing_timestamps[col].get(trade_id, now)
                if age > ttl:
                    log.warning(
                        "Purging stale trade │ col=%s │ id=%s │ age=%.1fs",
                        col, trade_id, age,
                    )
                    remove_from_performing(col, trade_id)
                    METRICS.stale_removals += 1
                    log.debug("performing=%s timestamps=%s", global_state.performing, global_state.performing_timestamps)
            except Exception:
                log.error("Failed to purge trade %s/%s:\n%s", col, trade_id, traceback.format_exc())


# ── Background sync task ──────────────────────────────────────────────────────

async def _sync_loop(cfg: Config) -> None:
    """
    Async replacement for the old daemon thread.

    Cycle cadence (configurable via env):
    • Every cycle  → purge stale trades, refresh positions & orders
    • Every N cycles → also refresh market metadata
    """
    cycle = 0
    log.info("Sync loop started (interval=%ds, market every %d cycles).",
             cfg.position_refresh_sec, cfg.market_refresh_cycles)

    while not _shutdown_event.is_set():
        await asyncio.sleep(cfg.position_refresh_sec)
        if _shutdown_event.is_set():
            break

        cycle += 1
        METRICS.sync_cycles += 1

        try:
            _purge_stale_trades(cfg.stale_trade_ttl_sec)
            update_positions(avgOnly=True)
            update_orders()

            if cycle % cfg.market_refresh_cycles == 0:
                update_markets()
                log.debug("Market metadata refreshed (cycle %d).", cycle)

            gc.collect()

        except Exception:
            METRICS.sync_errors += 1
            log.error("Sync cycle %d failed:\n%s", cycle, traceback.format_exc())

    log.info("Sync loop stopped.")


# ── WebSocket manager ─────────────────────────────────────────────────────────

async def _websocket_loop(cfg: Config) -> None:
    """
    Keeps market + user WebSocket connections alive.
    Re-connects automatically after any failure.
    """
    log.info("WebSocket manager started.")

    while not _shutdown_event.is_set():
        try:
            await asyncio.gather(
                connect_market_websocket(global_state.all_tokens),
                connect_user_websocket(),
            )
            # gather() returned — connections closed cleanly
            if not _shutdown_event.is_set():
                METRICS.ws_reconnects += 1
                log.info("WebSocket connections closed — reconnecting … (total reconnects: %d)", METRICS.ws_reconnects)

        except asyncio.CancelledError:
            break
        except Exception:
            METRICS.ws_reconnects += 1
            log.error("WebSocket error (reconnect #%d):\n%s", METRICS.ws_reconnects, traceback.format_exc())

        if not _shutdown_event.is_set():
            await asyncio.sleep(cfg.ws_reconnect_delay)
            gc.collect()

    log.info("WebSocket manager stopped.")


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    _register_signals()

    log.info("━" * 64)
    log.info("  poly-maker market-making daemon  ·  starting up")
    log.info("  pid=%d │ python=%s", os.getpid(), sys.version.split()[0])
    log.info("━" * 64)

    # Initialise client & global state
    global_state.client    = PolymarketClient()
    global_state.all_tokens = []
    _bootstrap()

    # Launch concurrent tasks
    sync_task = asyncio.create_task(_sync_loop(CFG),      name="sync-loop")
    ws_task   = asyncio.create_task(_websocket_loop(CFG), name="websocket-loop")

    # Block until shutdown is requested
    await _shutdown_event.wait()

    log.info("Shutdown requested — cancelling tasks …")
    for task in (sync_task, ws_task):
        task.cancel()

    await asyncio.gather(sync_task, ws_task, return_exceptions=True)

    log.info("━" * 64)
    log.info("  poly-maker stopped cleanly.")
    log.info("  %s", METRICS.summary())
    log.info("━" * 64)


if __name__ == "__main__":
    asyncio.run(main())
"""
╔══════════════════════════════════════════════════════════════════╗
║            poly-maker  ·  Market-Making Daemon                   ║
║  Async WebSocket engine + background state sync + clean shutdown ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
import signal
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import NoReturn

from dotenv import load_dotenv

import poly_data.global_state as global_state
from poly_data.data_processing import remove_from_performing
from poly_data.data_utils import update_markets, update_orders, update_positions
from poly_data.polymarket_client import PolymarketClient
from poly_data.websocket_handlers import connect_market_websocket, connect_user_websocket

load_dotenv()


# ── Configuration ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    """
    All runtime tunables in one place.
    Every field can be overridden via the corresponding environment variable.
    """

    position_refresh_sec:  int   = int(os.getenv("POSITION_REFRESH_SEC",  "5"))
    market_refresh_cycles: int   = int(os.getenv("MARKET_REFRESH_CYCLES", "6"))    # × position_refresh_sec
    stale_trade_ttl_sec:   float = float(os.getenv("STALE_TRADE_TTL_SEC", "15.0"))
    ws_reconnect_delay:    float = float(os.getenv("WS_RECONNECT_DELAY",  "1.0"))
    shutdown_timeout_sec:  float = float(os.getenv("SHUTDOWN_TIMEOUT_SEC","10.0"))
    log_level:             str   = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file:              str   = os.getenv("LOG_FILE",  "poly_maker.log")

    def __post_init__(self) -> None:
        if self.position_refresh_sec <= 0:
            raise ValueError("POSITION_REFRESH_SEC must be > 0")
        if self.market_refresh_cycles <= 0:
            raise ValueError("MARKET_REFRESH_CYCLES must be > 0")
        if self.stale_trade_ttl_sec <= 0:
            raise ValueError("STALE_TRADE_TTL_SEC must be > 0")
        if self.log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"Invalid LOG_LEVEL: {self.log_level!r}")


CFG = Config()


# ── Logging ────────────────────────────────────────────────────────────────────

def _setup_logging(cfg: Config) -> logging.Logger:
    """
    Configure the root 'poly_maker' logger with a console handler and a
    rotating-safe file handler.  Duplicate handlers are avoided on reload.
    """
    fmt = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-8s │ %(name)-30s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("poly_maker")
    logger.setLevel(cfg.log_level)

    # Guard against duplicate handlers when the module is reloaded (e.g. tests)
    if logger.handlers:
        return logger

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(cfg.log_file, encoding="utf-8", errors="replace"),
    ]
    for handler in handlers:
        handler.setFormatter(fmt)
        handler.setLevel(cfg.log_level)
        logger.addHandler(handler)

    return logger


log = _setup_logging(CFG)


# ── Runtime metrics ────────────────────────────────────────────────────────────

@dataclass
class Metrics:
    """
    Lightweight counters collected during the daemon lifetime.
    Thread-safe reads are acceptable since writes happen only from the
    async event loop (single-threaded).
    """

    sync_cycles:    int      = 0
    sync_errors:    int      = 0
    stale_removals: int      = 0
    ws_reconnects:  int      = 0
    started_at:     datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # ── Helpers ────────────────────────────────────────────────────────────────

    def uptime(self) -> str:
        """Return a human-readable uptime string, e.g. ``02h15m30s``."""
        delta      = datetime.now(timezone.utc) - self.started_at
        total_secs = int(delta.total_seconds())
        h, rem     = divmod(total_secs, 3600)
        m, s       = divmod(rem, 60)
        return f"{h:02d}h{m:02d}m{s:02d}s"

    def summary(self) -> str:
        """Return a single-line metrics summary suitable for log output."""
        return (
            f"uptime={self.uptime()} "
            f"sync_cycles={self.sync_cycles} "
            f"sync_errors={self.sync_errors} "
            f"stale_removals={self.stale_removals} "
            f"ws_reconnects={self.ws_reconnects}"
        )

    def error_rate(self) -> float:
        """Return sync error rate as a fraction (0.0 – 1.0)."""
        return self.sync_errors / self.sync_cycles if self.sync_cycles else 0.0


METRICS = Metrics()


# ── Shutdown coordination ──────────────────────────────────────────────────────

_shutdown_event: asyncio.Event = asyncio.Event()


def _register_signals() -> None:
    """
    Register SIGINT / SIGTERM handlers on the running event loop.
    Both signals trigger a single graceful-shutdown sequence.
    Must be called from within a running async context.
    """
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig: signal.Signals) -> None:
        if not _shutdown_event.is_set():
            log.info("Signal %s received — initiating graceful shutdown …", sig.name)
            _shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: _request_shutdown(signal.Signals(s)))

    log.debug("Signal handlers registered for SIGINT and SIGTERM.")


# ── State initialisation ───────────────────────────────────────────────────────

def _bootstrap() -> None:
    """
    Fetch all required state synchronously before the async event loop
    takes over.  Raises on failure so the process exits with a clear error
    rather than running with stale / empty state.
    """
    log.info("Bootstrapping — fetching markets, positions, and orders …")

    try:
        update_markets()
        update_positions()
        update_orders()
    except Exception as exc:
        log.critical("Bootstrap failed — cannot start daemon: %s", exc, exc_info=True)
        raise SystemExit(1) from exc

    log.info(
        "Bootstrap complete: %d markets │ %d positions │ %d orders",
        len(global_state.df),
        len(global_state.positions),
        len(global_state.orders),
    )
    log.debug("Initial positions : %s", global_state.positions)
    log.debug("Initial orders    : %s", global_state.orders)


# ── Stale-trade cleanup ────────────────────────────────────────────────────────

def _purge_stale_trades(ttl: float) -> None:
    """
    Remove entries from *performing* whose timestamp is older than *ttl* seconds.

    Iterates over a snapshot of keys so that ``remove_from_performing`` can
    mutate the underlying dict without triggering a ``RuntimeError``.
    """
    now        = time.monotonic()
    purge_count = 0

    for col in list(global_state.performing):
        for trade_id in list(global_state.performing.get(col, {})):
            try:
                ts  = global_state.performing_timestamps.get(col, {}).get(trade_id)
                age = now - ts if ts is not None else 0.0

                if age > ttl:
                    log.warning(
                        "Purging stale trade │ col=%s │ id=%s │ age=%.1fs (ttl=%.1fs)",
                        col, trade_id, age, ttl,
                    )
                    remove_from_performing(col, trade_id)
                    METRICS.stale_removals += 1
                    purge_count += 1

            except Exception:
                log.error(
                    "Failed to purge trade %s/%s:\n%s",
                    col, trade_id, traceback.format_exc(),
                )

    if purge_count:
        log.debug(
            "Stale purge complete: %d removed │ performing=%s",
            purge_count, global_state.performing,
        )


# ── Background sync task ───────────────────────────────────────────────────────

async def _sync_loop(cfg: Config) -> None:
    """
    Periodic state-sync coroutine.

    Cadence (all intervals configurable via env vars):

    ┌────────────────────────────────────────────────────────┐
    │  Every cycle         → purge stale trades              │
    │                      → refresh positions & orders      │
    │  Every N cycles      → also refresh market metadata    │
    └────────────────────────────────────────────────────────┘

    Errors inside a cycle are caught and counted; the loop continues
    running unless the shutdown event is set.
    """
    cycle = 0
    log.info(
        "Sync loop started  (interval=%ds │ market-refresh every %d cycles).",
        cfg.position_refresh_sec,
        cfg.market_refresh_cycles,
    )

    while not _shutdown_event.is_set():
        try:
            await asyncio.wait_for(
                asyncio.shield(asyncio.sleep(cfg.position_refresh_sec)),
                timeout=cfg.position_refresh_sec + 1.0,
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            break

        if _shutdown_event.is_set():
            break

        cycle += 1
        METRICS.sync_cycles += 1
        cycle_start = time.monotonic()

        try:
            _purge_stale_trades(cfg.stale_trade_ttl_sec)
            update_positions(avgOnly=True)
            update_orders()

            if cycle % cfg.market_refresh_cycles == 0:
                update_markets()
                log.debug("Market metadata refreshed (cycle=%d).", cycle)

            elapsed = time.monotonic() - cycle_start
            log.debug("Sync cycle %d complete in %.3fs.", cycle, elapsed)

            gc.collect()

        except Exception:
            METRICS.sync_errors += 1
            log.error(
                "Sync cycle %d failed (error_rate=%.1f%%):\n%s",
                cycle,
                METRICS.error_rate() * 100,
                traceback.format_exc(),
            )

    log.info("Sync loop stopped (total cycles=%d │ errors=%d).", METRICS.sync_cycles, METRICS.sync_errors)


# ── WebSocket manager ──────────────────────────────────────────────────────────

async def _websocket_loop(cfg: Config) -> None:
    """
    Keeps the market-data and user WebSocket connections alive.

    Both connections are managed together via ``asyncio.gather``.
    On any failure (or clean closure), the loop waits *ws_reconnect_delay*
    seconds before attempting to reconnect, honouring the shutdown event.
    """
    log.info("WebSocket manager started.")

    while not _shutdown_event.is_set():
        try:
            await asyncio.gather(
                connect_market_websocket(global_state.all_tokens),
                connect_user_websocket(),
            )
            # gather() returned without exception — connections closed cleanly
            if not _shutdown_event.is_set():
                METRICS.ws_reconnects += 1
                log.info(
                    "WebSocket connections closed cleanly — reconnecting … "
                    "(reconnect #%d)",
                    METRICS.ws_reconnects,
                )

        except asyncio.CancelledError:
            log.debug("WebSocket manager received CancelledError — stopping.")
            break

        except Exception:
            METRICS.ws_reconnects += 1
            log.error(
                "WebSocket error (reconnect #%d):\n%s",
                METRICS.ws_reconnects,
                traceback.format_exc(),
            )

        if not _shutdown_event.is_set():
            log.debug(
                "Waiting %.1fs before next WebSocket reconnect …",
                cfg.ws_reconnect_delay,
            )
            await asyncio.sleep(cfg.ws_reconnect_delay)
            gc.collect()

    log.info("WebSocket manager stopped (total reconnects=%d).", METRICS.ws_reconnects)


# ── Graceful shutdown ──────────────────────────────────────────────────────────

async def _shutdown(
    tasks: list[asyncio.Task],  # type: ignore[type-arg]
    timeout: float,
) -> None:
    """
    Cancel *tasks* and wait for them to finish, capped at *timeout* seconds.
    Logs any tasks that do not honour the timeout.
    """
    log.info("Cancelling %d background task(s) …", len(tasks))
    for task in tasks:
        task.cancel()

    try:
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=timeout,
        )
        log.debug("All tasks finished cleanly within %.1fs.", timeout)
    except asyncio.TimeoutError:
        still_running = [t.get_name() for t in tasks if not t.done()]
        log.warning(
            "Shutdown timed out after %.1fs — tasks still running: %s",
            timeout, still_running,
        )


# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    """
    Async entry point for the poly-maker daemon.

    Lifecycle:
        1. Register OS signal handlers.
        2. Initialise the Polymarket client and bootstrap global state.
        3. Launch background tasks (sync loop + WebSocket manager).
        4. Block until a shutdown signal is received.
        5. Cancel tasks and wait for clean exit.
    """
    _register_signals()

    _banner = "━" * 64
    log.info(_banner)
    log.info("  poly-maker market-making daemon  ·  starting up")
    log.info("  pid=%-6d │ python=%s", os.getpid(), sys.version.split()[0])
    log.info("  log_level=%-8s │ log_file=%s", CFG.log_level, CFG.log_file)
    log.info(_banner)

    # ── Initialise client & global state ──────────────────────────────────────
    global_state.client     = PolymarketClient()
    global_state.all_tokens = []
    _bootstrap()

    # ── Launch background tasks ────────────────────────────────────────────────
    tasks: list[asyncio.Task] = [  # type: ignore[type-arg]
        asyncio.create_task(_sync_loop(CFG),      name="sync-loop"),
        asyncio.create_task(_websocket_loop(CFG), name="websocket-loop"),
    ]
    log.info("Background tasks started: %s", [t.get_name() for t in tasks])

    # ── Wait for shutdown signal ───────────────────────────────────────────────
    await _shutdown_event.wait()
    log.info("Shutdown signal received — beginning graceful teardown …")

    await _shutdown(tasks, timeout=CFG.shutdown_timeout_sec)

    # ── Final summary ──────────────────────────────────────────────────────────
    log.info(_banner)
    log.info("  poly-maker stopped cleanly.")
    log.info("  %s", METRICS.summary())
    log.info(_banner)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass  # Already handled via signal handlers / _bootstrap SystemExit
    except Exception:
        logging.getLogger("poly_maker").critical(
            "Unhandled exception in main:\n%s", traceback.format_exc()
        )
        sys.exit(1)