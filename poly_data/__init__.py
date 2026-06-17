"""
poly-maker  ·  poly_data
========================
Core data layer: Polymarket API client, global state, WebSocket handlers,
order / position management, and market data processing.
"""

from __future__ import annotations

# ── Client ─────────────────────────────────────────────────────────────────────
from poly_data.polymarket_client import PolymarketClient

# ── Global state (re-exported for convenience) ─────────────────────────────────
import poly_data.global_state as global_state

# ── Market & order data helpers ────────────────────────────────────────────────
from poly_data.data_utils import (
    get_order,
    get_position,
    set_order,
    set_position,
    update_markets,
    update_orders,
    update_positions,
)

# ── Order-book / trade processing ──────────────────────────────────────────────
from poly_data.data_processing import (
    add_to_performing,
    process_book_data,
    process_data,
    process_price_change,
    process_user_data,
    remove_from_performing,
)

# ── WebSocket connections ──────────────────────────────────────────────────────
from poly_data.websocket_handlers import (
    connect_market_websocket,
    connect_user_websocket,
)

# ── Utilities ──────────────────────────────────────────────────────────────────
from poly_data.utils import get_sheet_df, pretty_print

# ── Constants ──────────────────────────────────────────────────────────────────
from poly_data.CONSTANTS import MIN_MERGE_SIZE

# ── Smart-contract ABIs ────────────────────────────────────────────────────────
from poly_data.abis import ConditionalTokenABI, NegRiskAdapterABI, erc20_abi

__all__ = [
    # Client
    "PolymarketClient",
    # Global state module
    "global_state",
    # Data helpers
    "get_order",
    "get_position",
    "set_order",
    "set_position",
    "update_markets",
    "update_orders",
    "update_positions",
    # Processing
    "add_to_performing",
    "process_book_data",
    "process_data",
    "process_price_change",
    "process_user_data",
    "remove_from_performing",
    # WebSockets
    "connect_market_websocket",
    "connect_user_websocket",
    # Utilities
    "get_sheet_df",
    "pretty_print",
    # Constants
    "MIN_MERGE_SIZE",
    # ABIs
    "ConditionalTokenABI",
    "NegRiskAdapterABI",
    "erc20_abi",
]