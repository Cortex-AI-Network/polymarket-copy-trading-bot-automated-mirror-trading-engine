# poly-merger

Position-merging utility for [Polymarket](https://polymarket.com).  
When you hold both YES and NO shares in the same market, this tool merges them on-chain to recover your USDC collateral.

---

## What it does

| Benefit | Detail |
|---|---|
| **Recover collateral** | Converts paired YES + NO positions back to USDC |
| **Reduce gas costs** | Batches merge operations into a single transaction |
| **Free up capital** | Releases locked collateral for redeployment |
| **Simplify positions** | Keeps your portfolio clean during automated market-making |

---

## How it works

The merger calls Polymarket's on-chain contracts directly:

- **Standard markets** → `ConditionalTokens.mergePositions`
- **Negative-risk markets** → `NegRiskAdapter.mergePositions`

Both paths are handled automatically based on the `is_neg_risk` flag.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Node.js | ≥ 16.x |
| ethers.js | 5.x |
| `.env` file | See below |

### `.env` setup

```env
PK=your_polygon_private_key_here
```

> ⚠️ Never commit your `.env` file. It is already listed in `.gitignore`.

---

## Installation

```bash
cd poly_merger
npm install
```

---

## Usage

### Standalone

```bash
node merge.js <amount> <condition_id> <is_neg_risk>
```

| Argument | Type | Description |
|---|---|---|
| `amount` | `int` | Raw token amount to merge (USDC × 10⁶, e.g. `1000000` = 1 USDC) |
| `condition_id` | `string` | Market condition ID (hex string) |
| `is_neg_risk` | `bool` | `true` for negative-risk markets, `false` otherwise |

**Example — merge 1 USDC in a negative-risk market:**

```bash
node merge.js 1000000 0x1234abcd...ef true
```

### Via poly-maker (automatic)

The merger is invoked automatically by `PolymarketClient.merge_positions()` when the bot detects mergeable opposing positions. No manual intervention needed.

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Merge succeeded — tx hash printed to stdout |
| `1` | Merge failed — error details printed to stderr |

---

## Notes

- `amount` is in raw units (multiply USDC by `1e6`).  
- Based on open-source Polymarket contracts, optimised for automated market-making.
- Requires an active Polygon RPC connection (`https://polygon-rpc.com`).