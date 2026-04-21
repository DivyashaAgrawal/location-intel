"""
Sentiment analysis module.

Provides rating-based sentiment classification and optional LLM-powered
review text analysis via local Ollama.
"""
from __future__ import annotations

import logging

import json
import pandas as pd
import requests as http_requests
from src.core.config import OLLAMA_BASE_URL, OLLAMA_MODEL

logger = logging.getLogger(__name__)



def sentiment_from_rating(rating: float | None) -> str:
    """Simple rating-based sentiment classification."""
    if rating is None:
        return "neutral"
    # pandas converts None -> NaN inside a Series; guard against that too.
    try:
        if pd.isna(rating):
            return "neutral"
    except (TypeError, ValueError):
        pass
    if rating >= 4.0:
        return "positive"
    if rating >= 3.0:
        return "neutral"
    return "negative"


def compute_sentiment_distribution(ratings: list[float]) -> dict:
    """
    Compute sentiment distribution from a list of ratings.
    Returns: {"positive_pct": float, "neutral_pct": float, "negative_pct": float}
    """
    if not ratings:
        return {"positive_pct": 0, "neutral_pct": 0, "negative_pct": 0}

    total = len(ratings)
    positive = sum(1 for r in ratings if r >= 4.0)
    neutral = sum(1 for r in ratings if 3.0 <= r < 4.0)
    negative = sum(1 for r in ratings if r < 3.0)

    return {
        "positive_pct": round(positive / total * 100, 1),
        "neutral_pct": round(neutral / total * 100, 1),
        "negative_pct": round(negative / total * 100, 1),
    }


def analyze_reviews_with_llm(reviews: list[str]) -> dict:
    """
    Use local Ollama to analyze a batch of review texts.
    Returns sentiment breakdown and key themes.

    Only call this if you have actual review text (from Outscraper or similar).
    Serper.dev Maps API doesn't return review text, only ratings.
    """
    if not reviews:
        return {"summary": "No review text available", "themes": []}

    # Cap at 30 to fit the 3B model context window.
    reviews_text = "\n---\n".join(reviews[:30])

    prompt = f"""Analyze these store reviews. Return ONLY valid JSON:
{{"positive_pct": <number>, "neutral_pct": <number>, "negative_pct": <number>, "top_positives": ["theme1","theme2","theme3"], "top_negatives": ["theme1","theme2","theme3"], "one_line_summary": "brief summary"}}

Reviews:
{reviews_text}"""

    try:
        response = http_requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2, "num_predict": 200},
            },
            timeout=30,
        )
        response.raise_for_status()

        raw = response.json().get("response", "").strip()
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(raw[start:end])
    except Exception as e:
        logger.warning(f"  Ollama sentiment analysis failed: {e}")

    return {"summary": "Analysis unavailable", "themes": []}


def enrich_sentiment_from_ratings(stores_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add sentiment columns to the stores DataFrame based on ratings.
    Works without any API key - pure heuristic.
    """
    df = stores_df.copy()

    if "rating" in df.columns:
        df["sentiment"] = df["rating"].apply(sentiment_from_rating)

        if "positive_pct" not in df.columns:
            def estimate_positive(r):
                if r is None or pd.isna(r):
                    return 50.0
                return round(min(95, max(10, (r - 1) * 22)), 1)

            def estimate_negative(r):
                if r is None or pd.isna(r):
                    return 20.0
                return round(min(50, max(2, (5 - r) * 12)), 1)

            df["positive_pct"] = df["rating"].apply(estimate_positive)
            df["negative_pct"] = df["rating"].apply(estimate_negative)
            df["neutral_pct"] = (100 - df["positive_pct"] - df["negative_pct"]).clip(lower=0)

    return df
