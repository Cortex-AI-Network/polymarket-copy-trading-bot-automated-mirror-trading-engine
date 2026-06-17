"""
poly-maker  ·  FinBERT Sentiment Engine
Fine-tuned FinBERT inference for financial text (tweets, news headlines).
Singleton pattern with automatic device detection and async-safe execution.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import ClassVar, Optional

import numpy as np
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

log = logging.getLogger("poly_maker.sentiment")

# ── Constants ─────────────────────────────────────────────────────────────────

_MODEL_NAME      = "ProsusAI/finbert"   # swap for a local fine-tuned path via env if needed
_MAX_TOKEN_LEN   = 512
_MIN_TEXT_LEN    = 5
_TEXT_PREVIEW_LEN = 200

# FinBERT label order: 0 = neutral, 1 = positive, 2 = negative
_LABEL_MAP: dict[int, str] = {0: "neutral", 1: "positive", 2: "negative"}
_SCORE_MAP: dict[str, float] = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class SentimentResult:
    """Immutable, structured output of a single FinBERT inference pass."""

    text:             str
    sentiment:        str    # "positive" | "negative" | "neutral"
    confidence_score: float  # probability of the winning class  [0, 1]
    positive_prob:    float
    negative_prob:    float
    neutral_prob:     float

    @property
    def trading_score(self) -> float:
        """
        Scalar signal suitable for downstream trading logic.
        Maps sentiment × confidence → [-1.0, +1.0].
        """
        return _SCORE_MAP[self.sentiment] * self.confidence_score

    def __str__(self) -> str:
        return (
            f"[{self.sentiment.upper():8s}  conf={self.confidence_score:.3f}  "
            f"score={self.trading_score:+.3f}]  {self.text[:80]!r}"
        )


# ── Inference engine ──────────────────────────────────────────────────────────

class FinBERTInference:
    """
    Thread-safe, async-compatible FinBERT singleton.

    Usage
    ─────
        engine = FinBERTInference()           # returns the shared instance
        result = await engine.predict(text)
        score  = result.trading_score         # -1 … +1
    """

    _instance:   ClassVar[Optional["FinBERTInference"]] = None
    _model:      AutoModelForSequenceClassification | None = None
    _tokenizer:  AutoTokenizer | None = None
    _device:     torch.device | None = None
    _lock:       ClassVar[asyncio.Lock] = asyncio.Lock()

    # ── Singleton construction ────────────────────────────────────────────────

    def __new__(cls) -> "FinBERTInference":
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._load_model()
            cls._instance = instance
        return cls._instance

    def _load_model(self) -> None:
        """Load tokenizer + model onto the best available device."""
        import os
        model_path = os.getenv("FINBERT_MODEL_PATH", _MODEL_NAME)
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        log.info("Loading FinBERT from %r on %s …", model_path, self._device)
        try:
            self._tokenizer = AutoTokenizer.from_pretrained(model_path)
            self._model = (
                AutoModelForSequenceClassification
                .from_pretrained(model_path)
                .to(self._device)
            )
            self._model.eval()
            log.info("FinBERT ready  (device=%s  labels=%s)", self._device, list(_LABEL_MAP.values()))
        except Exception:
            log.exception("Failed to load FinBERT from %r", model_path)
            raise

    # ── Public API ────────────────────────────────────────────────────────────

    async def predict(self, text: str) -> SentimentResult | None:
        """
        Async sentiment inference for a single *text*.
        Returns ``None`` for empty / too-short inputs or on inference error.
        """
        if not text or len(text.strip()) < _MIN_TEXT_LEN:
            return None
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self._infer, text)
        except Exception:
            log.error("Inference error for text %r", text[:80], exc_info=True)
            return None

    async def predict_batch(self, texts: list[str]) -> list[SentimentResult | None]:
        """
        Async inference over a list of texts.
        Runs all predictions concurrently in the thread-pool executor.
        """
        return list(await asyncio.gather(*(self.predict(t) for t in texts)))

    # ── Internal sync inference ───────────────────────────────────────────────

    def _infer(self, text: str) -> SentimentResult:
        """Blocking inference — called from an executor thread."""
        inputs = self._tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=_MAX_TOKEN_LEN,
            padding=True,
        )
        inputs = {k: v.to(self._device) for k, v in inputs.items()}

        with torch.no_grad():
            logits = self._model(**inputs).logits
            probs  = torch.nn.functional.softmax(logits, dim=-1).cpu().numpy()[0]

        idx       = int(np.argmax(probs))
        sentiment = _LABEL_MAP[idx]

        result = SentimentResult(
            text             = text[:_TEXT_PREVIEW_LEN],
            sentiment        = sentiment,
            confidence_score = float(probs[idx]),
            positive_prob    = float(probs[1]),
            negative_prob    = float(probs[2]),
            neutral_prob     = float(probs[0]),
        )
        log.debug("predict → %s", result)
        return result


# ── Module-level singleton ────────────────────────────────────────────────────

finbert = FinBERTInference()