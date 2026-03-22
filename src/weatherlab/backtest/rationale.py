from __future__ import annotations


def build_rationale(
    *,
    fair_prob: float,
    market_mid: float,
    tradable_yes_ask: float | None,
    edge_vs_ask: float | None,
    minutes_to_close: int | None,
    city_id: str | None = None,
    forecast_source: str | None = None,
    candidate_rank: int | None = None,
    candidate_bucket: str | None = None,
    strategy_context: dict | None = None,
) -> dict:
    return {
        'summary': 'Weather contract decision snapshot',
        'fair_probability': fair_prob,
        'market_mid': market_mid,
        'tradable_yes_ask': tradable_yes_ask,
        'edge_vs_ask': edge_vs_ask,
        'minutes_to_close': minutes_to_close,
        'city_id': city_id,
        'forecast_source': forecast_source,
        'candidate_rank': candidate_rank,
        'candidate_bucket': candidate_bucket,
        'strategy_context': strategy_context or {},
        'principle': 'compare the full board before isolating a single bet',
        'questions': [
            'What did we think would happen?',
            'What was the market offering?',
            'How did this compare to the other available bets today?',
            'Did the edge survive execution?',
        ],
    }
