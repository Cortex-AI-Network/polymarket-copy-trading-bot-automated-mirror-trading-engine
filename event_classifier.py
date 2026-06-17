"""
poly-maker  ·  Event Classifier
Fuses FinBERT sentiment scores with Llama structural classifications
into a single, typed StructuredEvent used by downstream trading logic.
"""

from __future__ import annotations

import logging
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import asyncio

from src.processing_layer.finbert_inference import SentimentResult, finbert
from src.processing_layer.llama_client import LlamaClassification, llama_client

log = logging.getLogger("poly_maker.classifier")


# ── Constants ─────────────────────────────────────────────────────────────────

_MIN_TEXT_LEN       = 10
_TEXT_PREVIEW_LEN   = 200
_MAX_POSITION_PCT   = 0.08   # hard cap on suggested position size

# Weight of each model in the blended score
_FINBERT_WEIGHT     = 0.6
_LLAMA_WEIGHT       = 0.4

# Amplification + clamp applied to raw blended score
_SCORE_AMPLIFIER    = 1.5
_SCORE_CLAMP        = 2.0

# Multiplier applied per event type before amplification
_EVENT_MULTIPLIERS: dict[str, float] = {
    "election":    1.5,
    "regulation":  1.4,
    "cyber_attack":1.3,
    "economic":    1.2,
    "tweet":       0.8,
    "other":       1.0,
}

_SENTIMENT_SCORE: dict[str, float] = {
    "positive": 1.0,
    "neutral":  0.0,
    "negative": -1.0,
}

_URGENCY_MAP: dict[str, str] = {
    "immediate": "immediate",
    "short":     "immediate",
    "medium":    "wait",
    "long":      "ignore",
}


# ── Impact enum ───────────────────────────────────────────────────────────────

class EventImpact(float, Enum):
    """
    Float-valued enum so impact can be compared directly with thresholds.
    Inheriting from float means ``EventImpact.BULLISH > 0`` is True.
    """
    VERY_BULLISH =  2.0
    BULLISH      =  1.0
    NEUTRAL      =  0.0
    BEARISH      = -1.0
    VERY_BEARISH = -2.0


def _score_to_impact(score: float) -> EventImpact:
    if score >=  1.5: return EventImpact.VERY_BULLISH
    if score >=  0.5: return EventImpact.BULLISH
    if score <= -1.5: return EventImpact.VERY_BEARISH
    if score <= -0.5: return EventImpact.BEARISH
    return EventImpact.NEUTRAL


_IMPACT_POSITION_WEIGHT: dict[EventImpact, float] = {
    EventImpact.VERY_BULLISH:  1.0,
    EventImpact.BULLISH:       0.6,
    EventImpact.NEUTRAL:       0.0,
    EventImpact.BEARISH:      -0.6,
    EventImpact.VERY_BEARISH: -1.0,
}


# ── Output type ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class StructuredEvent:
    """
    Immutable, fully-typed event signal ready for consumption by the
    trading engine. All fields are set once at construction time.
    """

    # ── Source ────────────────────────────────────────────────────────────────
    original_text:         str
    timestamp:             datetime

    # ── Model outputs ─────────────────────────────────────────────────────────
    sentiment_scores:      SentimentResult
    finbert_score:         float
    llama_classification:  Optional[LlamaClassification]
    llama_confidence:      float

    # ── Derived signals ───────────────────────────────────────────────────────
    event_type:                   str
    impact_horizon:               str
    combined_score:               float          # clamped to [-2.0, +2.0]
    confidence:                   float          # average model confidence [0, 1]
    impact_trading:               EventImpact
    suggested_position_size_pct:  float          # [0, _MAX_POSITION_PCT]
    urgency:                      str            # "immediate" | "wait" | "ignore"
    explanation:                  str

    # ── Optional context ──────────────────────────────────────────────────────
    affected_markets: tuple[str, ...] = field(default_factory=tuple)

    def __str__(self) -> str:
        return (
            f"[{self.impact_trading.name:<12s}  score={self.combined_score:+.3f}  "
            f"conf={self.confidence:.2f}  urgency={self.urgency}]  "
            f"{self.original_text[:60]!r}"
        )


# ── Classifier ────────────────────────────────────────────────────────────────

class EventClassifier:
    """
    Async event classifier that runs FinBERT and Llama in parallel,
    then fuses their outputs into a :class:`StructuredEvent`.

    Both model clients are injected for testability; production code
    uses the module-level singletons by default.
    """

    def __init__(self) -> None:
        self._finbert = finbert
        self._llama   = llama_client

    # ── Public API ────────────────────────────────────────────────────────────

    async def classify_event(
        self,
        text: str,
        market_context: dict | None = None,
    ) -> StructuredEvent | None:
        """
        Classify a single *text* event.

        Returns ``None`` if the text is too short, FinBERT fails, or
        an unexpected exception occurs.
        """
        if not text or len(text.strip()) < _MIN_TEXT_LEN:
            return None

        try:
            sentiment_result, llama_result = await asyncio.gather(
                self._finbert.predict(text),
                self._llama.classify_event(text, market_context),
            )
        except Exception:
            log.error("Model inference failed for %r:\n%s", text[:60], traceback.format_exc())
            return None

        if sentiment_result is None:
            log.warning("FinBERT returned None for text %r — skipping", text[:60])
            return None

        return self._fuse(text, sentiment_result, llama_result)

    async def classify_batch(
        self,
        texts: list[str],
        market_context: dict | None = None,
    ) -> list[StructuredEvent | None]:
        """Classify multiple texts concurrently."""
        return list(
            await asyncio.gather(*(self.classify_event(t, market_context) for t in texts))
        )

    # ── Fusion logic ──────────────────────────────────────────────────────────

    def _fuse(
        self,
        text: str,
        sentiment: SentimentResult,
        llama: LlamaClassification | None,
    ) -> StructuredEvent:
        """Combine FinBERT and Llama signals into a single StructuredEvent."""

        # ── Extract Llama fields (with safe defaults) ─────────────────────────
        event_type        = llama.event_type       if llama else "other"
        impact_horizon    = llama.impact_horizon   if llama else "medium"
        llama_confidence  = llama.confidence       if llama else 0.5
        affected_markets  = tuple(llama.affected_markets) if llama else ()
        llama_score       = _SENTIMENT_SCORE.get(llama.sentiment_implied or "", 0.0) if llama else 0.0

        # ── Blend scores ──────────────────────────────────────────────────────
        finbert_score = sentiment.trading_score                                   # [-1, +1]
        blended       = finbert_score * _FINBERT_WEIGHT + llama_score * _LLAMA_WEIGHT
        multiplier    = _EVENT_MULTIPLIERS.get(event_type, 1.0)
        combined      = max(-_SCORE_CLAMP, min(_SCORE_CLAMP, blended * multiplier * _SCORE_AMPLIFIER))

        # ── Derived trading signals ───────────────────────────────────────────
        impact     = _score_to_impact(combined)
        confidence = (sentiment.confidence_score + llama_confidence) / 2.0
        pos_size   = max(
            0.0,
            min(_MAX_POSITION_PCT, (abs(combined) / _SCORE_CLAMP) * _MAX_POSITION_PCT * confidence),
        )
        urgency     = _URGENCY_MAP.get(impact_horizon, "wait")
        explanation = (
            f"finbert={finbert_score:+.3f} × {_FINBERT_WEIGHT} | "
            f"llama={llama_score:+.3f} × {_LLAMA_WEIGHT} | "
            f"event_mult={multiplier} → combined={combined:+.3f} ({impact.name})"
        )

        log.info("classify → %s", explanation)

        return StructuredEvent(
            original_text                = text[:_TEXT_PREVIEW_LEN],
            timestamp                    = datetime.now(timezone.utc),
            sentiment_scores             = sentiment,
            finbert_score                = finbert_score,
            llama_classification         = llama,
            llama_confidence             = llama_confidence,
            event_type                   = event_type,
            impact_horizon               = impact_horizon,
            combined_score               = combined,
            confidence                   = confidence,
            impact_trading               = impact,
            suggested_position_size_pct  = pos_size,
            urgency                      = urgency,
            explanation                  = explanation,
            affected_markets             = affected_markets,
        )


# ── Module-level singleton ────────────────────────────────────────────────────

event_classifier = EventClassifier()