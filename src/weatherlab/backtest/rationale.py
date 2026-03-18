from __future__ import annotations


def build_rationale(*, fair_prob: float, market_mid: float, tradable_yes_ask: float | None, edge_vs_ask: float | None, minutes_to_close: int | None) -> dict:
    return {
        'summary': 'Weather contract decision snapshot',
        'fair_probability': fair_prob,
        'market_mid': market_mid,
        'tradable_yes_ask': tradable_yes_ask,
        'edge_vs_ask': edge_vs_ask,
        'minutes_to_close': minutes_to_close,
        'principle': 'forecast first, bid second',
        'questions': [
            'What did we think would happen?',
            'What was the market offering?',
            'Did the edge survive execution?',
        ],
    }
