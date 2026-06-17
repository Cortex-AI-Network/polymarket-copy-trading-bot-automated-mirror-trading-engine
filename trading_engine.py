"""
poly-maker  ·  Trading Execution Engine
Handles market-making logic: order placement, position merging, stop-loss, and take-profit.
"""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import math
import os
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

import poly_data.CONSTANTS as CONSTANTS
import poly_data.global_state as global_state
from poly_data.data_utils import get_order, get_position, set_position
from poly_data.trading_utils import (
    get_best_bid_ask_deets,
    get_buy_sell_amount,
    get_order_prices,
    round_down,
    round_up,
)

# ── Constants ─────────────────────────────────────────────────────────────────

_POSITIONS_DIR  = Path("positions")
_PRICE_BAND_LO  = 0.1
_PRICE_BAND_HI  = 0.9
_CANCEL_PRICE_DIFF  = 0.005   # 0.5 cents
_CANCEL_SIZE_RATIO  = 0.10    # 10 %
_PRICE_DEVIATION_MAX = 0.05   # max delta from sheet reference price
_SELL_TP_DIFF_PCT    = 2.0    # % diff before updating a sell order
_SELL_SIZE_FLOOR     = 0.97   # re-send sell if size < 97 % of position
_BUY_POS_HEADROOM    = 0.95   # re-send buy if position+orders < 95 % of max_size
_ABS_POSITION_CAP    = 250    # hard cap on any single token position
_ORDERBOOK_DEPTH     = 100
_ORDERBOOK_DEPTH_MIN = 20
_ORDERBOOK_THRESHOLD = 0.1

_POSITIONS_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("poly_maker.trading")


# ── Data helpers ──────────────────────────────────────────────────────────────

@dataclass
class OrderBook:
    best_bid:              float
    best_bid_size:         float
    second_best_bid:       float | None
    second_best_bid_size:  float | None
    top_bid:               float
    best_ask:              float
    best_ask_size:         float
    second_best_ask:       float | None
    second_best_ask_size:  float | None
    top_ask:               float
    bid_sum_within_n_pct:  float
    ask_sum_within_n_pct:  float

    @property
    def mid(self) -> float:
        return (self.top_bid + self.top_ask) / 2

    @property
    def spread(self) -> float:
        return round(self.best_ask - self.best_bid, 2)

    @property
    def liquidity_ratio(self) -> float:
        try:
            return self.bid_sum_within_n_pct / self.ask_sum_within_n_pct
        except ZeroDivisionError:
            return 0.0

    @classmethod
    def from_raw(cls, d: dict[str, Any], round_length: int) -> "OrderBook":
        def _r(v):
            return round(v, round_length) if v is not None else None

        return cls(
            best_bid             = _r(d["best_bid"]),
            best_bid_size        = d["best_bid_size"],
            second_best_bid      = _r(d["second_best_bid"]),
            second_best_bid_size = d["second_best_bid_size"],
            top_bid              = _r(d["top_bid"]),
            best_ask             = _r(d["best_ask"]),
            best_ask_size        = d["best_ask_size"],
            second_best_ask      = _r(d["second_best_ask"]),
            second_best_ask_size = d["second_best_ask_size"],
            top_ask              = _r(d["top_ask"]),
            bid_sum_within_n_pct = d.get("bid_sum_within_n_percent", 0),
            ask_sum_within_n_pct = d.get("ask_sum_within_n_percent", 0),
        )


def _fetch_orderbook(market: str, token_name: str, round_length: int) -> OrderBook:
    """Fetch order book, falling back to a smaller depth if top-of-book is missing."""
    raw = get_best_bid_ask_deets(market, token_name, _ORDERBOOK_DEPTH, _ORDERBOOK_THRESHOLD)
    if any(raw.get(k) is None for k in ("best_bid", "best_ask", "best_bid_size", "best_ask_size")):
        log.debug("Top-of-book incomplete — retrying with depth=%d", _ORDERBOOK_DEPTH_MIN)
        raw = get_best_bid_ask_deets(market, token_name, _ORDERBOOK_DEPTH_MIN, _ORDERBOOK_THRESHOLD)
    return OrderBook.from_raw(raw, round_length)


# ── Risk-off file helpers ─────────────────────────────────────────────────────

def _risk_file(market: str) -> Path:
    return _POSITIONS_DIR / f"{market}.json"


def _write_risk_file(market: str, details: dict) -> None:
    _risk_file(market).write_text(json.dumps(details))


def _in_risk_off_period(market: str) -> bool:
    """Return True if a risk-off cooldown is still active for *market*."""
    fp = _risk_file(market)
    if not fp.exists():
        return False
    try:
        details        = json.loads(fp.read_text())
        resume_at      = pd.to_datetime(details["sleep_till"])
        current_time   = pd.Timestamp.utcnow().tz_localize(None)
        if current_time < resume_at:
            log.info("Risk-off active — buy suppressed. (risked off at %s)", details["time"])
            return True
    except Exception:
        log.warning("Could not parse risk file for %s:\n%s", market, traceback.format_exc())
    return False


# ── Order cancellation helpers ────────────────────────────────────────────────

def _should_cancel(existing_price: float, existing_size: float,
                   target_price: float, target_size: float) -> bool:
    price_diff = abs(existing_price - target_price) if existing_price > 0 else math.inf
    size_diff  = abs(existing_size  - target_size)  if existing_size  > 0 else math.inf
    return (
        price_diff > _CANCEL_PRICE_DIFF
        or size_diff > target_size * _CANCEL_SIZE_RATIO
        or existing_size == 0
    )


# ── Order senders ─────────────────────────────────────────────────────────────

def send_buy_order(order: dict) -> None:
    """
    Place a BUY order, cancelling the existing one only when necessary.

    Skips placement if:
    - The existing order is still valid (minor drift).
    - The target price is below the incentive threshold.
    - The price is outside the acceptable band [0.1, 0.9).
    """
    client = global_state.client
    ex_buy  = order["orders"]["buy"]
    ex_sell = order["orders"]["sell"]

    cancel = _should_cancel(ex_buy["price"], ex_buy["size"], order["price"], order["size"])

    if not cancel:
        log.debug(
            "Keeping existing BUY — price_diff=%.4f  size_diff=%.1f",
            abs(ex_buy["price"] - order["price"]),
            abs(ex_buy["size"]  - order["size"]),
        )
        return

    if ex_buy["size"] > 0 or ex_sell["size"] > 0:
        log.info("Cancelling BUY orders for token %s", order["token"])
        client.cancel_all_asset(order["token"])

    incentive_floor = order["mid_price"] - order["max_spread"] / 100
    if order["price"] < incentive_floor:
        log.info(
            "BUY suppressed — price %.4f < incentive floor %.4f (mid=%.4f)",
            order["price"], incentive_floor, order["mid_price"],
        )
        return

    if not (_PRICE_BAND_LO <= order["price"] < _PRICE_BAND_HI):
        log.info("BUY suppressed — price %.4f outside band [%.1f, %.1f)", order["price"], _PRICE_BAND_LO, _PRICE_BAND_HI)
        return

    log.info("Placing BUY  token=%s  price=%.4f  size=%.2f", order["token"], order["price"], order["size"])
    client.create_order(
        order["token"], "BUY", order["price"], order["size"],
        order["neg_risk"] == "TRUE",
    )


def send_sell_order(order: dict) -> None:
    """
    Place a SELL order, cancelling the existing one only when necessary.
    """
    client  = global_state.client
    ex_sell = order["orders"]["sell"]
    ex_buy  = order["orders"]["buy"]

    cancel = _should_cancel(ex_sell["price"], ex_sell["size"], order["price"], order["size"])

    if not cancel:
        log.debug(
            "Keeping existing SELL — price_diff=%.4f  size_diff=%.1f",
            abs(ex_sell["price"] - order["price"]),
            abs(ex_sell["size"]  - order["size"]),
        )
        return

    if ex_sell["size"] > 0 or ex_buy["size"] > 0:
        log.info("Cancelling SELL orders for token %s", order["token"])
        client.cancel_all_asset(order["token"])

    log.info("Placing SELL  token=%s  price=%.4f  size=%.2f", order["token"], order["price"], order["size"])
    client.create_order(
        order["token"], "SELL", order["price"], order["size"],
        order["neg_risk"] == "TRUE",
    )


# ── Per-market lock registry ──────────────────────────────────────────────────

_market_locks: dict[str, asyncio.Lock] = {}


def _get_lock(market: str) -> asyncio.Lock:
    if market not in _market_locks:
        _market_locks[market] = asyncio.Lock()
    return _market_locks[market]


# ── Core trading logic ────────────────────────────────────────────────────────

async def perform_trade(market: str) -> None:
    """
    Market-making loop for a single *market* (condition_id).

    Flow per cycle
    ──────────────
    1. Merge opposing positions if above threshold (frees capital).
    2. For each outcome (YES / NO):
       a. Fetch fresh order book.
       b. Compute bid / ask prices.
       c. Execute stop-loss if triggered.
       d. Place / refresh BUY order when position < max_size.
       e. Place / refresh SELL (take-profit) order when position ≥ max_size.
    """
    async with _get_lock(market):
        try:
            await _trade_market(market)
        except Exception:
            log.error("Unhandled error in perform_trade(%s):\n%s", market, traceback.format_exc())
        finally:
            gc.collect()
            await asyncio.sleep(2)


async def _trade_market(market: str) -> None:  # noqa: C901  (complexity justified by domain logic)
    client = global_state.client
    row    = global_state.df[global_state.df["condition_id"] == market].iloc[0]
    params = global_state.params[row["param_type"]]

    round_length = len(str(row["tick_size"]).split(".")[1])

    outcomes = [
        {"name": "token1", "token": row["token1"], "answer": row["answer1"]},
        {"name": "token2", "token": row["token2"], "answer": row["answer2"]},
    ]

    log.info("\n%s | %s", pd.Timestamp.utcnow().tz_localize(None), row["question"])

    # ── 1. Position merging ───────────────────────────────────────────────────
    pos_1 = get_position(row["token1"])["size"]
    pos_2 = get_position(row["token2"])["size"]

    if min(pos_1, pos_2) > CONSTANTS.MIN_MERGE_SIZE:
        raw_1, raw_2 = client.get_position(row["token1"])[0], client.get_position(row["token2"])[0]
        merge_raw    = min(raw_1, raw_2)
        merge_scaled = merge_raw / 10**6

        if merge_scaled > CONSTANTS.MIN_MERGE_SIZE:
            log.info("Merging positions: token1=%.2f  token2=%.2f  amount=%.2f", raw_1, raw_2, merge_scaled)
            client.merge_positions(merge_raw, market, row["neg_risk"] == "TRUE")
            set_position(row["token1"], "SELL", merge_scaled, 0, "merge")
            set_position(row["token2"], "SELL", merge_scaled, 0, "merge")

    # ── 2. Per-outcome trading ────────────────────────────────────────────────
    for outcome in outcomes:
        token      = int(outcome["token"])
        orders     = get_order(token)
        ob         = _fetch_orderbook(market, outcome["name"], round_length)

        bid_price, ask_price = get_order_prices(
            ob.best_bid, ob.best_bid_size, ob.top_bid,
            ob.best_ask, ob.best_ask_size, ob.top_ask,
            get_position(token)["avgPrice"], row,
        )
        bid_price = round(bid_price, round_length)
        ask_price = round(ask_price, round_length)

        pos       = get_position(token)
        position  = round_down(pos["size"], 2)
        avg_price = pos["avgPrice"]
        max_size  = row.get("max_size", row["trade_size"])

        log.info(
            "[%s] pos=%.2f avg=%.4f | book: bid=%.4f ask=%.4f | target: bid=%.4f ask=%.4f | mid=%.4f",
            outcome["answer"], position, avg_price,
            ob.best_bid, ob.best_ask, bid_price, ask_price, ob.mid,
        )

        buy_amount, sell_amount = get_buy_sell_amount(
            position, bid_price, row, get_position(global_state.REVERSE_TOKENS[str(token)])["size"]
        )

        order_base = {
            "token":      token,
            "mid_price":  ob.mid,
            "neg_risk":   row["neg_risk"],
            "max_spread": row["max_spread"],
            "orders":     orders,
            "token_name": outcome["name"],
            "row":        row,
        }

        # ── 2a. Stop-loss ─────────────────────────────────────────────────────
        if sell_amount > 0:
            if avg_price == 0:
                log.debug("[%s] avgPrice=0 — skipping sell evaluation", outcome["answer"])
                continue

            fresh_ob = _fetch_orderbook(market, outcome["name"], round_length)
            mid_now  = round_up((fresh_ob.best_bid + fresh_ob.best_ask) / 2, round_length)
            pnl      = (mid_now - avg_price) / avg_price * 100

            log.info("[%s] mid=%.4f spread=%.4f pnl=%.2f%%", outcome["answer"], mid_now, fresh_ob.spread, pnl)

            stop_triggered = (
                pnl < params["stop_loss_threshold"]
                and fresh_ob.spread <= params["spread_threshold"]
            ) or row["3_hour"] > params["volatility_threshold"]

            if stop_triggered:
                risk_details = {
                    "time":       str(pd.Timestamp.utcnow().tz_localize(None)),
                    "question":   row["question"],
                    "msg":        (
                        f"Stop-loss: sell {sell_amount} | spread={fresh_ob.spread:.4f} "
                        f"pnl={pnl:.2f}% vol3h={row['3_hour']}"
                    ),
                    "sleep_till": str(
                        pd.Timestamp.utcnow().tz_localize(None)
                        + pd.Timedelta(hours=params["sleep_period"])
                    ),
                }
                log.warning("STOP-LOSS triggered | %s", risk_details["msg"])

                send_sell_order({**order_base, "size": sell_amount, "price": fresh_ob.best_bid})
                client.cancel_all_market(market)
                _write_risk_file(market, risk_details)
                continue

        # ── 2b. Buy order management ──────────────────────────────────────────
        if position < max_size and position < _ABS_POSITION_CAP and buy_amount >= row["min_size"]:
            sheet_ref = row["best_bid"] if outcome["name"] == "token1" else 1 - row["best_ask"]
            sheet_ref = round(sheet_ref, round_length)

            if _in_risk_off_period(market):
                pass  # suppressed by risk-off cooldown

            elif row["3_hour"] > params["volatility_threshold"]:
                log.info("[%s] Cancelling — 3h vol %.4f > threshold %.4f", outcome["answer"], row["3_hour"], params["volatility_threshold"])
                client.cancel_all_asset(token)

            elif abs(bid_price - sheet_ref) >= _PRICE_DEVIATION_MAX:
                log.info("[%s] Cancelling — price deviation %.4f >= %.2f", outcome["answer"], abs(bid_price - sheet_ref), _PRICE_DEVIATION_MAX)
                client.cancel_all_asset(token)

            else:
                rev_token = global_state.REVERSE_TOKENS[str(token)]
                rev_pos   = get_position(rev_token)

                if rev_pos["size"] > row["min_size"]:
                    log.info("[%s] BUY suppressed — reverse position exists (%.2f)", outcome["answer"], rev_pos["size"])
                    if orders["buy"]["size"] > CONSTANTS.MIN_MERGE_SIZE:
                        client.cancel_all_asset(token)
                    continue

                if ob.liquidity_ratio <= 0:
                    log.info("[%s] BUY suppressed — liquidity_ratio=%.4f", outcome["answer"], ob.liquidity_ratio)
                    client.cancel_all_asset(token)

                else:
                    order = {**order_base, "size": buy_amount, "price": bid_price}
                    existing_buy = orders["buy"]

                    if ob.best_bid > existing_buy["price"]:
                        log.info("[%s] BUY refresh — better price available", outcome["answer"])
                        send_buy_order(order)
                    elif position + existing_buy["size"] < _BUY_POS_HEADROOM * max_size:
                        log.info("[%s] BUY refresh — insufficient position+orders vs max_size", outcome["answer"])
                        send_buy_order(order)
                    elif existing_buy["size"] > order["size"] * 1.01:
                        log.info("[%s] BUY refresh — existing order oversized", outcome["answer"])
                        send_buy_order(order)

        # ── 2c. Take-profit / sell order management ───────────────────────────
        elif sell_amount > 0:
            tp_price   = round_up(avg_price * (1 + params["take_profit_threshold"] / 100), round_length)
            final_price = round_up(max(tp_price, ask_price), round_length)
            order       = {**order_base, "size": sell_amount, "price": final_price}

            ex_sell_price = float(orders["sell"]["price"])
            diff_pct      = abs(ex_sell_price - float(tp_price)) / float(tp_price) * 100

            if diff_pct > _SELL_TP_DIFF_PCT:
                log.info("[%s] SELL refresh — tp_price=%.4f  current=%.4f  diff=%.2f%%",
                         outcome["answer"], tp_price, ex_sell_price, diff_pct)
                send_sell_order(order)
            elif orders["sell"]["size"] < position * _SELL_SIZE_FLOOR:
                log.info("[%s] SELL refresh — sell_size %.2f < %.0f%% of position %.2f",
                         outcome["answer"], orders["sell"]["size"], _SELL_SIZE_FLOOR * 100, position)
                send_sell_order(order)