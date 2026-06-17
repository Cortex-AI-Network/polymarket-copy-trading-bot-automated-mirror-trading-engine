"""
Async HTTP client for fine-tuned Llama 3 8B model (GPU server).
All API keys and endpoints are placeholders – replace XXXXXX with actual values.
"""

import aiohttp
import asyncio
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import json
import os
from src.monitoring.logger import setup_logger

logger = setup_logger(__name__)

@dataclass
class LlamaClassification:
    """Classification result from Llama model."""
    event_type: str      # election, regulation, tweet, cyber_attack, economic, other
    impact_horizon: str  # immediate, short, medium, long
    confidence: float    # 0-1
    raw_response: str
    sentiment_implied: Optional[str] = None
    affected_markets: Optional[List[str]] = None

class LlamaClient:
    """Async client for Llama 3 8B GPU server."""
    
    def __init__(self, endpoint: Optional[str] = None, timeout: int = 10):
        self.endpoint = endpoint or os.getenv("LLAMA_GPU_ENDPOINT", "XXXXXX")  # Replace with actual
        self.timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def classify_event(self, text: str, context: Optional[Dict[str, Any]] = None) -> Optional[LlamaClassification]:
        """Send text to Llama and get structured classification."""
        prompt = self._build_prompt(text, context)
        payload = {
            "prompt": prompt,
            "max_tokens": 150,
            "temperature": 0.1,
            "top_p": 0.9,
            "stop": ["\n\n"]
        }
        try:
            session = await self._get_session()
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with session.post(self.endpoint, json=payload, timeout=timeout) as response:
                if response.status == 200:
                    result = await response.json()
                    raw_text = result.get("text", "")
                    return self._parse_response(raw_text)
                else:
                    logger.error(f"Llama API error: {response.status}")
                    return None
        except asyncio.TimeoutError:
            logger.error(f"Llama request timeout after {self.timeout}s")
            return None
        except Exception as e:
            logger.error(f"Llama client error: {e}")
            return None
    
    def _build_prompt(self, text: str, context: Optional[Dict[str, Any]] = None) -> str:
        context_str = ""
        if context:
            context_str = f"\nContext: Market: {context.get('market_id', 'unknown')}, Price: {context.get('price', 'N/A')}"
        return f"""Classify the following news/tweet for prediction market trading. Return JSON only.

Text: {text}{context_str}

Return JSON with exactly these fields:
- event_type: one of [election, regulation, tweet, cyber_attack, economic, other]
- impact_horizon: one of [immediate, short, medium, long] (immediate=minutes, short=hours, medium=days, long=weeks+)
- confidence: float 0-1
- sentiment_implied: one of [positive, negative, neutral]
- affected_markets: list of likely affected markets (max 3)

JSON:"""
    
    def _parse_response(self, raw_text: str) -> Optional[LlamaClassification]:
        try:
            import re
            json_match = re.search(r'\{.*\}', raw_text, re.DOTALL)
            if not json_match:
                logger.warning(f"No JSON found in Llama response: {raw_text[:100]}")
                return None
            data = json.loads(json_match.group())
            return LlamaClassification(
                event_type=data.get("event_type", "other"),
                impact_horizon=data.get("impact_horizon", "medium"),
                confidence=float(data.get("confidence", 0.5)),
                raw_response=raw_text,
                sentiment_implied=data.get("sentiment_implied", "neutral"),
                affected_markets=data.get("affected_markets", [])
            )
        except Exception as e:
            logger.error(f"Failed to parse Llama response: {e}")
            return None
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def classify_batch(self, texts: List[str]) -> List[Optional[LlamaClassification]]:
        tasks = [self.classify_event(text) for text in texts]
        return await asyncio.gather(*tasks)

llama_client = LlamaClient()