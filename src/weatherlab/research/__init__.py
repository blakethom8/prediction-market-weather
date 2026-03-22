"""Research and historical-analysis namespace.

This package makes the research side of the repo explicit alongside
`weatherlab.live` while keeping the existing package-root imports available.
"""

from .archive_plan import ARCHIVE_SOURCE_CANDIDATES, get_focus_city_archive_plan, get_focus_city_ids
from .evaluation import EvalRow, score_row
from .replay import replay_decision_for_market

__all__ = [
    'ARCHIVE_SOURCE_CANDIDATES',
    'EvalRow',
    'get_focus_city_archive_plan',
    'get_focus_city_ids',
    'replay_decision_for_market',
    'score_row',
]
