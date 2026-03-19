"""Editable signal logic for weather market experiments."""

from __future__ import annotations


def compute_edge(fair_prob: float, tradable_yes_ask: float) -> float:
    return fair_prob - tradable_yes_ask


def choose_action(*, fair_prob: float, tradable_yes_ask: float, min_edge: float = 0.05) -> str:
    edge = compute_edge(fair_prob, tradable_yes_ask)
    if edge >= min_edge:
        return "BUY_YES"
    return "NO_TRADE"


if __name__ == "__main__":
    examples = [
        {"fair_prob": 0.62, "tradable_yes_ask": 0.50},
        {"fair_prob": 0.53, "tradable_yes_ask": 0.50},
    ]
    for example in examples:
        action = choose_action(**example)
        print(example, "->", action)
