from __future__ import annotations

from datetime import UTC, datetime
import subprocess
import tempfile
import unittest
from pathlib import Path

from weatherlab.pipeline.calibration_reviews import (
    crossed_calibration_thresholds,
    maybe_write_calibration_reviews,
)


class CalibrationReviewHookTests(unittest.TestCase):
    def test_crossed_calibration_thresholds_only_returns_new_multiples_of_10(self):
        self.assertEqual(crossed_calibration_thresholds(8, 9), [])
        self.assertEqual(crossed_calibration_thresholds(9, 10), [10])
        self.assertEqual(crossed_calibration_thresholds(10, 11), [])
        self.assertEqual(crossed_calibration_thresholds(19, 31), [20, 30])

    def test_maybe_write_calibration_reviews_runs_command_and_writes_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            calls = []

            def fake_runner(command, **kwargs):
                calls.append((command, kwargs))
                return subprocess.CompletedProcess(
                    args=command,
                    returncode=0,
                    stdout='=== CALIBRATION REPORT ===\nSettled bets with outcomes: 10\n',
                    stderr='',
                )

            paths = maybe_write_calibration_reviews(
                before_count=9,
                after_count=10,
                reviews_dir=Path(tmp),
                repo_root=Path(tmp),
                runner=fake_runner,
                now=datetime(2026, 4, 26, 12, 0, tzinfo=UTC),
            )

            self.assertEqual(len(paths), 1)
            self.assertEqual(paths[0].name, '2026-04-26-calibration-010-settled-bets.md')
            self.assertEqual(calls[0][0], ['make', 'chief', '--', 'calibration'])
            self.assertEqual(calls[0][1]['cwd'], tmp)

            report = paths[0].read_text()
            self.assertIn('Calibration Review - 10 Settled Bets', report)
            self.assertIn('9 -> 10', report)
            self.assertIn('=== CALIBRATION REPORT ===', report)

    def test_maybe_write_calibration_reviews_does_nothing_without_threshold_crossing(self):
        with tempfile.TemporaryDirectory() as tmp:
            def fake_runner(command, **kwargs):
                raise AssertionError('runner should not be called')

            paths = maybe_write_calibration_reviews(
                before_count=10,
                after_count=11,
                reviews_dir=Path(tmp),
                runner=fake_runner,
            )

            self.assertEqual(paths, [])
            self.assertEqual(list(Path(tmp).iterdir()), [])


if __name__ == '__main__':
    unittest.main()
